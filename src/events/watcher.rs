//! Kubernetes list/watch pipeline and demo seeder.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Event;
use kube::api::{Api, ListParams, WatchEvent, WatchParams};
use kube::{Client, Error as KubeError, ResourceExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{Config, EventsMode};
use crate::events::model::{ClusterEvent, InvolvedObject};
use crate::events::registry::{EventRegistry, UpsertOutcome};
use crate::metrics::Metrics;

/// Coarse watch lifecycle for health/MCP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WatchState {
    Starting = 0,
    Listing = 1,
    Watching = 2,
    BackingOff = 3,
    Stopped = 4,
}

impl WatchState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Listing => "listing",
            Self::Watching => "watching",
            Self::BackingOff => "backing_off",
            Self::Stopped => "stopped",
        }
    }

    /// Coarse health label for HTTP/MCP (`healthy` / `degraded` / …).
    #[must_use]
    pub const fn health_status(self) -> &'static str {
        match self {
            Self::Starting | Self::Listing => "starting",
            Self::Watching => "healthy",
            Self::BackingOff => "degraded",
            Self::Stopped => "unhealthy",
        }
    }
}

/// Snapshot of watch/runtime diagnostics for health/MCP.
#[derive(Debug, Clone)]
pub struct WatchRuntimeStatus {
    pub state: WatchState,
    pub started_at: DateTime<Utc>,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_watch_success_at: Option<DateTime<Utc>>,
    pub last_watch_error_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
}

/// Shared watch state handle with diagnostic counters/timestamps.
#[derive(Clone, Debug)]
pub struct WatchStateHandle {
    inner: Arc<AtomicU8>,
    started_at: DateTime<Utc>,
    last_event_at: Arc<std::sync::Mutex<Option<DateTime<Utc>>>>,
    last_watch_success_at: Arc<std::sync::Mutex<Option<DateTime<Utc>>>>,
    last_watch_error_at: Arc<std::sync::Mutex<Option<DateTime<Utc>>>>,
    last_error: Arc<std::sync::Mutex<Option<String>>>,
    consecutive_failures: Arc<std::sync::atomic::AtomicU32>,
}

impl WatchStateHandle {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AtomicU8::new(WatchState::Starting as u8)),
            started_at: Utc::now(),
            last_event_at: Arc::new(std::sync::Mutex::new(None)),
            last_watch_success_at: Arc::new(std::sync::Mutex::new(None)),
            last_watch_error_at: Arc::new(std::sync::Mutex::new(None)),
            last_error: Arc::new(std::sync::Mutex::new(None)),
            consecutive_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }

    pub fn set(&self, state: WatchState) {
        self.inner.store(state as u8, Ordering::Relaxed);
    }

    #[must_use]
    pub fn get(&self) -> WatchState {
        match self.inner.load(Ordering::Relaxed) {
            1 => WatchState::Listing,
            2 => WatchState::Watching,
            3 => WatchState::BackingOff,
            4 => WatchState::Stopped,
            _ => WatchState::Starting,
        }
    }

    pub fn note_event(&self) {
        if let Ok(mut guard) = self.last_event_at.lock() {
            *guard = Some(Utc::now());
        }
    }

    pub fn note_watch_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_watch_success_at.lock() {
            *guard = Some(Utc::now());
        }
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = None;
        }
    }

    pub fn note_watch_error(&self, error: &str) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_watch_error_at.lock() {
            *guard = Some(Utc::now());
        }
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = Some(sanitize_error(error));
        }
    }

    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    #[must_use]
    pub fn snapshot(&self) -> WatchRuntimeStatus {
        WatchRuntimeStatus {
            state: self.get(),
            started_at: self.started_at,
            last_event_at: self.last_event_at.lock().ok().and_then(|g| *g),
            last_watch_success_at: self.last_watch_success_at.lock().ok().and_then(|g| *g),
            last_watch_error_at: self.last_watch_error_at.lock().ok().and_then(|g| *g),
            last_error: self.last_error.lock().ok().and_then(|g| g.clone()),
            consecutive_failures: self.consecutive_failures(),
        }
    }
}

impl Default for WatchStateHandle {
    fn default() -> Self {
        Self::new()
    }
}

fn sanitize_error(error: &str) -> String {
    let trimmed = error.trim();
    let mut out: String = trimmed.chars().take(256).collect();
    if trimmed.chars().count() > 256 {
        out.push('…');
    }
    out
}

/// How the pipeline should recover after a watch stream ends or errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchResumeAction {
    /// resourceVersion is gone/expired — clear RV and re-list.
    Relist,
    /// Resume watch from the last known resourceVersion (no list).
    Rewatch,
}

/// Classify a Kubernetes API error for resume correctness.
///
/// HTTP 410 Gone means the resourceVersion is too old; any other error keeps the
/// last RV and rewatches after backoff (avoids DDoS via repeated full lists).
#[must_use]
pub fn classify_watch_error(err: &KubeError) -> WatchResumeAction {
    match err {
        KubeError::Api(status) if status.code == 410 => WatchResumeAction::Relist,
        _ => WatchResumeAction::Rewatch,
    }
}

/// Compute exponential backoff duration: `base * 2^attempt`, capped at `max`.
#[must_use]
pub fn next_backoff(base: Duration, max: Duration, attempt: u32) -> Duration {
    let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
    base.checked_mul(factor).unwrap_or(max).min(max).max(base)
}

/// Run event ingestion until cancelled.
pub async fn run_event_pipeline(
    config: Config,
    registry: EventRegistry,
    metrics: Metrics,
    watch_state: WatchStateHandle,
    cancel: CancellationToken,
) {
    match config.events_mode {
        EventsMode::Demo => {
            seed_demo_events(&registry, &metrics).await;
            watch_state.set(WatchState::Watching);
            cancel.cancelled().await;
            watch_state.set(WatchState::Stopped);
        }
        EventsMode::Kubernetes => {
            if let Err(err) = run_kubernetes_loop(
                config,
                registry,
                metrics,
                watch_state.clone(),
                cancel.clone(),
            )
            .await
            {
                warn!(error = %err, "kubernetes event pipeline exited with error");
            }
            watch_state.set(WatchState::Stopped);
        }
    }
}

async fn seed_demo_events(registry: &EventRegistry, metrics: &Metrics) {
    let now = Utc::now();
    let demos = [
        ("demo-uid-1", "Warning", "BackOff", "demo pod crashloop"),
        ("demo-uid-2", "Normal", "Scheduled", "demo pod scheduled"),
        (
            "demo-uid-3",
            "Warning",
            "FailedMount",
            "demo volume mount failure",
        ),
    ];

    for (uid, event_type, reason, message) in demos {
        let event = ClusterEvent {
            uid: uid.to_owned(),
            namespace: "demo".to_owned(),
            name: format!("demo.{uid}"),
            resource_version: "1".to_owned(),
            event_type: event_type.to_owned(),
            reason: reason.to_owned(),
            message: message.to_owned(),
            count: 1,
            involved_object: InvolvedObject {
                kind: "Pod".to_owned(),
                namespace: "demo".to_owned(),
                name: "demo-pod".to_owned(),
                uid: Some(format!("pod-{uid}")),
                api_version: Some("v1".to_owned()),
            },
            source_component: Some("clustersentinel-demo".to_owned()),
            first_timestamp: Some(now),
            last_timestamp: Some(now),
            event_time: Some(now),
            registered_at: now,
        };
        apply_upsert(registry, metrics, None, event).await;
    }
    info!(count = 3, "seeded demo events");
}

async fn run_kubernetes_loop(
    config: Config,
    registry: EventRegistry,
    metrics: Metrics,
    watch_state: WatchStateHandle,
    cancel: CancellationToken,
) -> Result<(), KubeError> {
    let client = Client::try_default().await?;
    let mut resource_version = String::new();
    let mut consecutive_failures: u32 = 0;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        // Bootstrap list only when we have no usable resourceVersion.
        if resource_version.is_empty() {
            watch_state.set(WatchState::Listing);
            match bootstrap_list(
                &client,
                &config,
                &registry,
                &metrics,
                &watch_state,
                &mut resource_version,
            )
            .await
            {
                Ok(()) => {
                    consecutive_failures = 0;
                    watch_state.note_watch_success();
                }
                Err(err) => {
                    metrics.watch_errors.inc();
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    watch_state.note_watch_error(&err.to_string());
                    warn!(error = %err, attempt = consecutive_failures, "event list failed");
                    watch_state.set(WatchState::BackingOff);
                    let delay = next_backoff(
                        config.watch_backoff,
                        config.watch_backoff_max,
                        consecutive_failures.saturating_sub(1),
                    );
                    if sleep_or_cancel(delay, &cancel).await {
                        break;
                    }
                    continue;
                }
            }
        }

        if resource_version.is_empty() {
            // Empty cluster / empty list with no RV — back off before retrying list.
            warn!("list returned empty resourceVersion; backing off before retry");
            metrics.watch_errors.inc();
            consecutive_failures = consecutive_failures.saturating_add(1);
            watch_state.set(WatchState::BackingOff);
            let delay = next_backoff(
                config.watch_backoff,
                config.watch_backoff_max,
                consecutive_failures.saturating_sub(1),
            );
            if sleep_or_cancel(delay, &cancel).await {
                break;
            }
            continue;
        }

        watch_state.set(WatchState::Watching);
        match watch_once(
            &client,
            &config,
            &registry,
            &metrics,
            &watch_state,
            &mut resource_version,
            &cancel,
        )
        .await
        {
            Ok(WatchExit::Cancelled) => break,
            Ok(WatchExit::Completed) => {
                // Timeout / clean EOF — resume from last RV without re-listing.
                consecutive_failures = 0;
                watch_state.note_watch_success();
                metrics.watch_restarts.inc();
                debug!(%resource_version, "watch completed; resuming from resourceVersion");
            }
            Err(err) => {
                metrics.watch_errors.inc();
                metrics.watch_restarts.inc();
                consecutive_failures = consecutive_failures.saturating_add(1);
                watch_state.note_watch_error(&err.to_string());
                match classify_watch_error(&err) {
                    WatchResumeAction::Relist => {
                        warn!(
                            error = %err,
                            attempt = consecutive_failures,
                            "watch resourceVersion expired; clearing for re-list"
                        );
                        resource_version.clear();
                    }
                    WatchResumeAction::Rewatch => {
                        warn!(
                            error = %err,
                            %resource_version,
                            attempt = consecutive_failures,
                            "event watch failed; will resume from last resourceVersion"
                        );
                    }
                }
                watch_state.set(WatchState::BackingOff);
                let delay = next_backoff(
                    config.watch_backoff,
                    config.watch_backoff_max,
                    consecutive_failures.saturating_sub(1),
                );
                if sleep_or_cancel(delay, &cancel).await {
                    break;
                }
            }
        }
    }

    Ok(())
}

enum WatchExit {
    Completed,
    Cancelled,
}

async fn bootstrap_list(
    client: &Client,
    config: &Config,
    registry: &EventRegistry,
    metrics: &Metrics,
    watch_state: &WatchStateHandle,
    resource_version: &mut String,
) -> Result<(), KubeError> {
    if config.namespaces.is_empty() {
        let api: Api<Event> = Api::all(client.clone());
        list_all_pages(
            &api,
            config.list_limit,
            registry,
            metrics,
            watch_state,
            resource_version,
        )
        .await?;
    } else {
        for namespace in &config.namespaces {
            let api: Api<Event> = Api::namespaced(client.clone(), namespace);
            list_all_pages(
                &api,
                config.list_limit,
                registry,
                metrics,
                watch_state,
                resource_version,
            )
            .await?;
        }
    }
    Ok(())
}

async fn list_all_pages(
    api: &Api<Event>,
    page_limit: u32,
    registry: &EventRegistry,
    metrics: &Metrics,
    watch_state: &WatchStateHandle,
    resource_version: &mut String,
) -> Result<(), KubeError> {
    let mut continue_token: Option<String> = None;
    loop {
        let mut lp = ListParams::default().limit(page_limit);
        if let Some(token) = continue_token.as_deref() {
            lp = lp.continue_token(token);
        }
        let list = api.list(&lp).await?;
        let next = list
            .metadata
            .continue_
            .clone()
            .filter(|token| !token.is_empty());
        ingest_list(list, registry, metrics, watch_state, resource_version).await;
        match next {
            Some(token) => continue_token = Some(token),
            None => break,
        }
    }
    Ok(())
}

async fn ingest_list(
    list: kube::api::ObjectList<Event>,
    registry: &EventRegistry,
    metrics: &Metrics,
    watch_state: &WatchStateHandle,
    resource_version: &mut String,
) {
    if let Some(rv) = list.metadata.resource_version.clone() {
        *resource_version = rv;
    }
    let now = Utc::now();
    for event in list.items {
        match ClusterEvent::try_from_kube(&event, now) {
            Ok(mapped) => apply_upsert(registry, metrics, Some(watch_state), mapped).await,
            Err(err) => {
                debug!(error = %err, "skipping malformed event");
            }
        }
    }
}

async fn watch_once(
    client: &Client,
    config: &Config,
    registry: &EventRegistry,
    metrics: &Metrics,
    watch_state: &WatchStateHandle,
    resource_version: &mut String,
    cancel: &CancellationToken,
) -> Result<WatchExit, KubeError> {
    let timeout_secs = u32::try_from(config.watch_timeout.as_secs()).unwrap_or(290);
    let wp = WatchParams::default().timeout(timeout_secs);

    if config.namespaces.is_empty() {
        let api: Api<Event> = Api::all(client.clone());
        return watch_api(
            api,
            wp,
            registry,
            metrics,
            watch_state,
            resource_version,
            cancel,
        )
        .await;
    }

    // Namespaced watches run sequentially per namespace for simplicity.
    for namespace in &config.namespaces {
        if cancel.is_cancelled() {
            return Ok(WatchExit::Cancelled);
        }
        let api: Api<Event> = Api::namespaced(client.clone(), namespace);
        match watch_api(
            api,
            wp.clone(),
            registry,
            metrics,
            watch_state,
            resource_version,
            cancel,
        )
        .await?
        {
            WatchExit::Cancelled => return Ok(WatchExit::Cancelled),
            WatchExit::Completed => {}
        }
    }
    Ok(WatchExit::Completed)
}

async fn watch_api(
    api: Api<Event>,
    wp: WatchParams,
    registry: &EventRegistry,
    metrics: &Metrics,
    watch_state: &WatchStateHandle,
    resource_version: &mut String,
    cancel: &CancellationToken,
) -> Result<WatchExit, KubeError> {
    let stream = api.watch(&wp, resource_version).await?;
    let mut stream = stream.boxed();

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(WatchExit::Cancelled),
            next = stream.try_next() => {
                match next? {
                    None => return Ok(WatchExit::Completed),
                    Some(WatchEvent::Bookmark(bookmark)) => {
                        if !bookmark.metadata.resource_version.is_empty() {
                            *resource_version = bookmark.metadata.resource_version;
                        }
                    }
                    Some(WatchEvent::Added(event) | WatchEvent::Modified(event)) => {
                        if let Some(rv) = event.resource_version() {
                            *resource_version = rv;
                        }
                        let now = Utc::now();
                        match ClusterEvent::try_from_kube(&event, now) {
                            Ok(mapped) => {
                                apply_upsert(registry, metrics, Some(watch_state), mapped).await;
                            }
                            Err(err) => debug!(error = %err, "skipping malformed watch event"),
                        }
                    }
                    Some(WatchEvent::Deleted(event)) => {
                        if let Some(rv) = event.resource_version() {
                            *resource_version = rv;
                        }
                        // Keep deleted events in registry for agent triage until TTL eviction.
                    }
                    Some(WatchEvent::Error(status)) => {
                        warn!(?status, "watch error event from apiserver");
                        return Err(KubeError::Api(status));
                    }
                }
            }
        }
    }
}

async fn apply_upsert(
    registry: &EventRegistry,
    metrics: &Metrics,
    watch_state: Option<&WatchStateHandle>,
    event: ClusterEvent,
) {
    let event_type = event.event_type.clone();
    let reason = event.reason.clone();
    let namespace = event.namespace.clone();
    let involved_kind = event.involved_object.kind.clone();
    let source = event.source_component.clone();

    match registry.upsert(event).await {
        UpsertOutcome::Inserted | UpsertOutcome::Updated => {
            if let Some(state) = watch_state {
                state.note_event();
            }
            metrics.observe_registered_labels(
                &event_type,
                &reason,
                &namespace,
                &involved_kind,
                source.as_deref(),
            );
        }
        UpsertOutcome::Deduped => {
            metrics.events_deduped.inc();
        }
    }
    metrics
        .registry_size
        .set(i64::try_from(registry.len().await).unwrap_or(i64::MAX));
}

/// Returns true when cancelled during sleep.
async fn sleep_or_cancel(duration: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(duration) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use super::*;
    use axum::http::{Request, Response};
    use kube::client::Body;
    use kube::core::Status;
    use tower_test::mock;

    #[test]
    fn backoff_grows_then_caps() {
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(60);
        assert_eq!(next_backoff(base, max, 0), Duration::from_secs(5));
        assert_eq!(next_backoff(base, max, 1), Duration::from_secs(10));
        assert_eq!(next_backoff(base, max, 2), Duration::from_secs(20));
        assert_eq!(next_backoff(base, max, 3), Duration::from_secs(40));
        assert_eq!(next_backoff(base, max, 4), Duration::from_secs(60));
        assert_eq!(next_backoff(base, max, 10), Duration::from_secs(60));
    }

    #[test]
    fn expired_resource_version_triggers_relist() {
        let gone = KubeError::Api(
            Status::failure("too old resource version", "Gone")
                .with_code(410)
                .boxed(),
        );
        assert_eq!(classify_watch_error(&gone), WatchResumeAction::Relist);

        let other = KubeError::Api(Status::failure("timeout", "Timeout").with_code(504).boxed());
        assert_eq!(classify_watch_error(&other), WatchResumeAction::Rewatch);
    }

    #[test]
    fn watch_states_map_to_health_statuses() {
        assert_eq!(WatchState::Starting.health_status(), "starting");
        assert_eq!(WatchState::Listing.health_status(), "starting");
        assert_eq!(WatchState::Watching.health_status(), "healthy");
        assert_eq!(WatchState::BackingOff.health_status(), "degraded");
        assert_eq!(WatchState::Stopped.health_status(), "unhealthy");
    }

    #[tokio::test]
    async fn bootstrap_list_reads_all_pages() {
        let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let server = tokio::spawn(async move {
            let mut handle = pin!(handle);
            let (first_request, first_send) =
                handle.next_request().await.expect("first list request");
            assert!(
                first_request
                    .uri()
                    .query()
                    .is_some_and(|query| query.contains("limit=1"))
            );
            assert!(!first_request.uri().to_string().contains("continue="));
            first_send.send_response(Response::new(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "EventList",
                    "metadata": {"continue": "next-page", "resourceVersion": "10"},
                    "items": [kube_event_json("event-1", "1")]
                }))
                .expect("first page json"),
            )));

            let (second_request, second_send) =
                handle.next_request().await.expect("second list request");
            assert!(
                second_request
                    .uri()
                    .query()
                    .is_some_and(|query| query.contains("continue=next-page"))
            );
            second_send.send_response(Response::new(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "EventList",
                    "metadata": {"resourceVersion": "11"},
                    "items": [kube_event_json("event-2", "2")]
                }))
                .expect("second page json"),
            )));
        });

        let api: Api<Event> = Api::all(Client::new(mock_service, "default"));
        let registry = EventRegistry::new(10, Duration::from_secs(3600));
        let metrics = Metrics::try_new().expect("metrics");
        let watch_state = WatchStateHandle::new();
        let mut resource_version = String::new();

        list_all_pages(
            &api,
            1,
            &registry,
            &metrics,
            &watch_state,
            &mut resource_version,
        )
        .await
        .expect("paginated list");
        server.await.expect("mock server");

        assert_eq!(registry.len().await, 2);
        assert_eq!(resource_version, "11");
    }

    fn kube_event_json(uid: &str, resource_version: &str) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {
                "name": format!("event-{uid}"),
                "namespace": "default",
                "resourceVersion": resource_version,
                "uid": uid
            },
            "involvedObject": {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": "web",
                "namespace": "default",
                "uid": "pod-1"
            },
            "message": "test event",
            "reason": "BackOff",
            "type": "Warning"
        })
    }
}

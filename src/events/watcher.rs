//! Kubernetes list/watch pipeline with durable write-before-ack.

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
use crate::events::WatchCheckpoint;
use crate::events::model::{ClusterEvent, InvolvedObject};
use crate::events::registry::{EventRegistry, UpsertOutcome};
use crate::events::writer::{DurableWriterHandle, WriterError};
use crate::metrics::Metrics;

/// Cluster-wide watch scope key stored in `watch_checkpoints.scope`.
pub const SCOPE_ALL: &str = "all";
const STORAGE_PRUNE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAX_PRUNE_BATCHES_PER_CYCLE: usize = 256;

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

/// Watch scope identity for per-namespace checkpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchScope {
    All,
    Namespace(String),
}

impl WatchScope {
    #[must_use]
    pub fn as_key(&self) -> &str {
        match self {
            Self::All => SCOPE_ALL,
            Self::Namespace(ns) => ns.as_str(),
        }
    }
}

/// Run event ingestion until cancelled.
pub async fn run_event_pipeline(
    config: Config,
    registry: EventRegistry,
    writer: DurableWriterHandle,
    metrics: Metrics,
    watch_state: WatchStateHandle,
    cancel: CancellationToken,
) {
    // Startup: prune + warm read-cache from durable store.
    match prune_until_current(&writer, &config).await {
        Ok(true) => {}
        Ok(false) => {
            warn!("startup prune reached its batch budget; cleanup will continue periodically")
        }
        Err(err) => warn!(error = %err, "startup prune failed"),
    }
    match writer
        .load_recent(&config.cluster_id, config.registry_capacity)
        .await
    {
        Ok(recent) => {
            for event in recent {
                let _ = registry.upsert(event).await;
            }
        }
        Err(err) => warn!(error = %err, "failed to warm registry from durable store"),
    }

    let prune_task = tokio::spawn(run_periodic_prune(
        writer.clone(),
        config.clone(),
        cancel.child_token(),
    ));

    match config.events_mode {
        EventsMode::Demo => {
            seed_demo_events(&registry, &writer, &config.cluster_id, &metrics).await;
            watch_state.set(WatchState::Watching);
            cancel.cancelled().await;
            watch_state.set(WatchState::Stopped);
        }
        EventsMode::Kubernetes => {
            if let Err(err) = run_kubernetes_scopes(
                config,
                registry,
                writer,
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

    if let Err(err) = prune_task.await {
        warn!(error = %err, "storage prune task join failed");
    }
}

async fn prune_until_current(
    writer: &DurableWriterHandle,
    config: &Config,
) -> Result<bool, WriterError> {
    let batch_size = config.storage_prune_batch.max(1);
    for _ in 0..MAX_PRUNE_BATCHES_PER_CYCLE {
        let result = writer
            .prune(
                Utc::now(),
                config.storage_retention,
                config.storage_max_events,
                batch_size,
            )
            .await?;
        if result.total() < batch_size {
            return Ok(true);
        }
        tokio::task::yield_now().await;
    }
    Ok(false)
}

async fn run_periodic_prune(
    writer: DurableWriterHandle,
    config: Config,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(STORAGE_PRUNE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = interval.tick() => {
                match prune_until_current(&writer, &config).await {
                    Ok(true) => {}
                    Ok(false) => warn!("periodic prune reached its batch budget; cleanup will continue next cycle"),
                    Err(err) => warn!(error = %err, "periodic storage prune failed"),
                }
            }
        }
    }
}

async fn seed_demo_events(
    registry: &EventRegistry,
    writer: &DurableWriterHandle,
    cluster_id: &str,
    metrics: &Metrics,
) {
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
        let checkpoint = WatchCheckpoint {
            cluster_id: cluster_id.to_owned(),
            scope: "demo".to_owned(),
            resource_version: "1".to_owned(),
            updated_at: now,
        };
        let _ = persist_then_cache(
            writer,
            registry,
            metrics,
            None,
            cluster_id,
            event,
            Some(checkpoint),
        )
        .await;
    }
    info!(count = 3, "seeded demo events");
}

async fn run_kubernetes_scopes(
    config: Config,
    registry: EventRegistry,
    writer: DurableWriterHandle,
    metrics: Metrics,
    watch_state: WatchStateHandle,
    cancel: CancellationToken,
) -> Result<(), KubeError> {
    let client = Client::try_default().await?;
    let scopes: Vec<WatchScope> = if config.namespaces.is_empty() {
        vec![WatchScope::All]
    } else {
        config
            .namespaces
            .iter()
            .cloned()
            .map(WatchScope::Namespace)
            .collect()
    };

    let mut joins = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let child = cancel.child_token();
        let cfg = config.clone();
        let reg = registry.clone();
        let wr = writer.clone();
        let met = metrics.clone();
        let ws = watch_state.clone();
        let cli = client.clone();
        joins.push(tokio::spawn(async move {
            run_scope_loop(cli, cfg, scope, reg, wr, met, ws, child).await
        }));
    }

    cancel.cancelled().await;
    for join in joins {
        match join.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = %err, "scope watch loop exited with kube error"),
            Err(err) => warn!(error = %err, "scope watch task join failed"),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_scope_loop(
    client: Client,
    config: Config,
    scope: WatchScope,
    registry: EventRegistry,
    writer: DurableWriterHandle,
    metrics: Metrics,
    watch_state: WatchStateHandle,
    cancel: CancellationToken,
) -> Result<(), KubeError> {
    let scope_key = scope.as_key().to_owned();
    let mut resource_version = match writer.load_checkpoint(&config.cluster_id, &scope_key).await {
        Ok(Some(cp)) => cp.resource_version,
        Ok(None) => String::new(),
        Err(err) => {
            warn!(error = %err, scope = %scope_key, "failed to load checkpoint; cold start");
            String::new()
        }
    };
    let mut consecutive_failures: u32 = 0;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        if resource_version.is_empty() {
            watch_state.set(WatchState::Listing);
            match bootstrap_list(
                &client,
                &config,
                &scope,
                &registry,
                &writer,
                &metrics,
                &watch_state,
                &mut resource_version,
                &cancel,
            )
            .await
            {
                Ok(ListOutcome::Cancelled) => break,
                Ok(ListOutcome::Completed) => {
                    consecutive_failures = 0;
                    watch_state.note_watch_success();
                }
                Err(ListError::Kube(err)) => {
                    metrics.watch_errors.inc();
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    watch_state.note_watch_error(&err.to_string());
                    warn!(error = %err, scope = %scope_key, attempt = consecutive_failures, "event list failed");
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
                Err(ListError::Storage(err)) => {
                    metrics.watch_errors.inc();
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    watch_state.note_watch_error(&err.to_string());
                    warn!(error = %err, scope = %scope_key, "storage failure during list; backing off");
                    watch_state.set(WatchState::BackingOff);
                    let delay = next_backoff(
                        config.watch_backoff,
                        config.watch_backoff_max,
                        consecutive_failures.saturating_sub(1),
                    );
                    if sleep_or_cancel(delay, &cancel).await {
                        break;
                    }
                    // Do not advance RV on storage failure.
                    resource_version.clear();
                    continue;
                }
            }
        }

        if resource_version.is_empty() {
            warn!(scope = %scope_key, "list returned empty resourceVersion; backing off before retry");
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
            &scope,
            &registry,
            &writer,
            &metrics,
            &watch_state,
            &mut resource_version,
            &cancel,
        )
        .await
        {
            Ok(WatchExit::Cancelled) => break,
            Ok(WatchExit::Completed) => {
                consecutive_failures = 0;
                watch_state.note_watch_success();
                metrics.watch_restarts.inc();
                debug!(%resource_version, scope = %scope_key, "watch completed; resuming from resourceVersion");
            }
            Ok(WatchExit::StorageBackoff) => {
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
                            scope = %scope_key,
                            attempt = consecutive_failures,
                            "watch resourceVersion expired; clearing checkpoint for re-list"
                        );
                        if let Err(clear_err) = writer
                            .clear_checkpoint(&config.cluster_id, &scope_key)
                            .await
                        {
                            warn!(error = %clear_err, scope = %scope_key, "failed to clear checkpoint");
                        }
                        resource_version.clear();
                    }
                    WatchResumeAction::Rewatch => {
                        warn!(
                            error = %err,
                            %resource_version,
                            scope = %scope_key,
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
    StorageBackoff,
}

enum ListOutcome {
    Completed,
    Cancelled,
}

enum ListError {
    Kube(KubeError),
    Storage(WriterError),
}

#[allow(clippy::too_many_arguments)]
async fn bootstrap_list(
    client: &Client,
    config: &Config,
    scope: &WatchScope,
    registry: &EventRegistry,
    writer: &DurableWriterHandle,
    metrics: &Metrics,
    watch_state: &WatchStateHandle,
    resource_version: &mut String,
    cancel: &CancellationToken,
) -> Result<ListOutcome, ListError> {
    let api: Api<Event> = match scope {
        WatchScope::All => Api::all(client.clone()),
        WatchScope::Namespace(ns) => Api::namespaced(client.clone(), ns),
    };
    list_all_pages(
        &api,
        config,
        scope.as_key(),
        registry,
        writer,
        metrics,
        watch_state,
        resource_version,
        cancel,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn list_all_pages(
    api: &Api<Event>,
    config: &Config,
    scope_key: &str,
    registry: &EventRegistry,
    writer: &DurableWriterHandle,
    metrics: &Metrics,
    watch_state: &WatchStateHandle,
    resource_version: &mut String,
    cancel: &CancellationToken,
) -> Result<ListOutcome, ListError> {
    let mut continue_token: Option<String> = None;
    let mut snapshot_resource_version: Option<String> = None;
    loop {
        if cancel.is_cancelled() {
            return Ok(ListOutcome::Cancelled);
        }
        let mut lp = ListParams::default().limit(config.list_limit);
        if let Some(token) = continue_token.as_deref() {
            lp = lp.continue_token(token);
        }
        let list = api.list(&lp).await.map_err(ListError::Kube)?;
        let next = list
            .metadata
            .continue_
            .clone()
            .filter(|token| !token.is_empty());
        let page_rv = list.metadata.resource_version.clone();
        if page_rv.as_deref().is_some_and(|value| !value.is_empty()) {
            snapshot_resource_version = page_rv;
        }
        let now = Utc::now();
        for event in list.items {
            if cancel.is_cancelled() {
                return Ok(ListOutcome::Cancelled);
            }
            match ClusterEvent::try_from_kube(&event, now) {
                Ok(mapped) => {
                    match persist_then_cache(
                        writer,
                        registry,
                        metrics,
                        Some(watch_state),
                        &config.cluster_id,
                        mapped,
                        None,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(err) => return Err(ListError::Storage(err)),
                    }
                }
                Err(err) => debug!(error = %err, "skipping malformed event"),
            }
        }
        match next {
            Some(token) => continue_token = Some(token),
            None => {
                if let Some(rv) = snapshot_resource_version {
                    let checkpoint = WatchCheckpoint {
                        cluster_id: config.cluster_id.clone(),
                        scope: scope_key.to_owned(),
                        resource_version: rv.clone(),
                        updated_at: Utc::now(),
                    };
                    writer
                        .save_checkpoint(checkpoint)
                        .await
                        .map_err(ListError::Storage)?;
                    *resource_version = rv;
                }
                break;
            }
        }
    }
    Ok(ListOutcome::Completed)
}

#[allow(clippy::too_many_arguments)]
async fn watch_once(
    client: &Client,
    config: &Config,
    scope: &WatchScope,
    registry: &EventRegistry,
    writer: &DurableWriterHandle,
    metrics: &Metrics,
    watch_state: &WatchStateHandle,
    resource_version: &mut String,
    cancel: &CancellationToken,
) -> Result<WatchExit, KubeError> {
    let timeout_secs = u32::try_from(config.watch_timeout.as_secs()).unwrap_or(290);
    let wp = WatchParams::default().timeout(timeout_secs);
    let api: Api<Event> = match scope {
        WatchScope::All => Api::all(client.clone()),
        WatchScope::Namespace(ns) => Api::namespaced(client.clone(), ns),
    };
    watch_api(
        api,
        wp,
        config,
        scope.as_key(),
        registry,
        writer,
        metrics,
        watch_state,
        resource_version,
        cancel,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn watch_api(
    api: Api<Event>,
    wp: WatchParams,
    config: &Config,
    scope_key: &str,
    registry: &EventRegistry,
    writer: &DurableWriterHandle,
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
                        let rv = bookmark.metadata.resource_version;
                        if rv.is_empty() {
                            continue;
                        }
                        let checkpoint = WatchCheckpoint {
                            cluster_id: config.cluster_id.clone(),
                            scope: scope_key.to_owned(),
                            resource_version: rv.clone(),
                            updated_at: Utc::now(),
                        };
                        match writer.save_checkpoint(checkpoint).await {
                            Ok(()) => *resource_version = rv,
                            Err(err) => {
                                warn!(error = %err, scope = %scope_key, "checkpoint write failed");
                                watch_state.note_watch_error(&err.to_string());
                                return Ok(WatchExit::StorageBackoff);
                            }
                        }
                    }
                    Some(WatchEvent::Added(event) | WatchEvent::Modified(event)) => {
                        let now = Utc::now();
                        let Some(rv) = event.resource_version() else {
                            continue;
                        };
                        match ClusterEvent::try_from_kube(&event, now) {
                            Ok(mapped) => {
                                let checkpoint = WatchCheckpoint {
                                    cluster_id: config.cluster_id.clone(),
                                    scope: scope_key.to_owned(),
                                    resource_version: rv.clone(),
                                    updated_at: Utc::now(),
                                };
                                match persist_then_cache(
                                    writer,
                                    registry,
                                    metrics,
                                    Some(watch_state),
                                    &config.cluster_id,
                                    mapped,
                                    Some(checkpoint),
                                ).await {
                                    Ok(_) => *resource_version = rv,
                                    Err(err) => {
                                        warn!(error = %err, scope = %scope_key, "durable write failed");
                                        watch_state.note_watch_error(&err.to_string());
                                        return Ok(WatchExit::StorageBackoff);
                                    }
                                }
                            }
                            Err(err) => debug!(error = %err, "skipping malformed watch event"),
                        }
                    }
                    Some(WatchEvent::Deleted(event)) => {
                        if let Some(rv) = event.resource_version() {
                            let checkpoint = WatchCheckpoint {
                                cluster_id: config.cluster_id.clone(),
                                scope: scope_key.to_owned(),
                                resource_version: rv.clone(),
                                updated_at: Utc::now(),
                            };
                            match writer.save_checkpoint(checkpoint).await {
                                Ok(()) => *resource_version = rv,
                                Err(err) => {
                                    warn!(error = %err, scope = %scope_key, "checkpoint write failed on delete");
                                    watch_state.note_watch_error(&err.to_string());
                                    return Ok(WatchExit::StorageBackoff);
                                }
                            }
                        }
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

/// Persist first; update read-cache/metrics only after successful commit.
pub async fn persist_then_cache(
    writer: &DurableWriterHandle,
    registry: &EventRegistry,
    metrics: &Metrics,
    watch_state: Option<&WatchStateHandle>,
    cluster_id: &str,
    event: ClusterEvent,
    checkpoint: Option<WatchCheckpoint>,
) -> Result<UpsertOutcome, WriterError> {
    let event_type = event.event_type.clone();
    let reason = event.reason.clone();
    let namespace = event.namespace.clone();
    let involved_kind = event.involved_object.kind.clone();
    let source = event.source_component.clone();
    let cached = event.clone();

    let outcome = writer
        .upsert_with_checkpoint(cluster_id, event, checkpoint)
        .await?;

    let _ = registry.upsert(cached).await;
    match outcome {
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
    Ok(outcome)
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
    use super::*;
    use crate::events::SqliteEventStore;
    use crate::events::storage_health::{StorageBackend, StorageHealthHandle};
    use crate::events::writer::spawn_durable_writer;
    use kube::core::Status;
    use tokio::sync::Mutex;

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

    fn sample(uid: &str, rv: &str) -> ClusterEvent {
        let now = Utc::now();
        ClusterEvent {
            uid: uid.to_owned(),
            namespace: "dev".to_owned(),
            name: format!("evt-{uid}"),
            resource_version: rv.to_owned(),
            event_type: "Warning".to_owned(),
            reason: "BackOff".to_owned(),
            message: "msg".to_owned(),
            count: 1,
            involved_object: InvolvedObject {
                kind: "Pod".to_owned(),
                namespace: "dev".to_owned(),
                name: "web".to_owned(),
                uid: Some("pod".to_owned()),
                api_version: Some("v1".to_owned()),
            },
            source_component: Some("kubelet".to_owned()),
            first_timestamp: None,
            last_timestamp: Some(now),
            event_time: None,
            registered_at: now,
        }
    }

    #[tokio::test]
    async fn persist_then_cache_skips_registry_on_storage_failure() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let metrics = Metrics::try_new().expect("metrics");
        let health = StorageHealthHandle::new(StorageBackend::Memory);
        health.mark_ready();
        let (writer, join) =
            spawn_durable_writer(store, 4, cancel.child_token(), metrics.clone(), health);
        let registry = EventRegistry::new(10, Duration::from_secs(3600));

        let bad = WatchCheckpoint {
            cluster_id: "default".to_owned(),
            scope: String::new(),
            resource_version: "1".to_owned(),
            updated_at: Utc::now(),
        };
        let err = persist_then_cache(
            &writer,
            &registry,
            &metrics,
            None,
            "default",
            sample("fail", "1"),
            Some(bad),
        )
        .await
        .expect_err("storage must fail");
        assert!(matches!(err, WriterError::Store(_)));
        assert!(registry.get("fail").await.is_none());

        cancel.cancel();
        let _ = join.await;
    }

    #[tokio::test]
    async fn persist_then_cache_updates_registry_after_commit() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let metrics = Metrics::try_new().expect("metrics");
        let health = StorageHealthHandle::new(StorageBackend::Memory);
        health.mark_ready();
        let (writer, join) =
            spawn_durable_writer(store, 4, cancel.child_token(), metrics.clone(), health);
        let registry = EventRegistry::new(10, Duration::from_secs(3600));

        let cp = WatchCheckpoint {
            cluster_id: "default".to_owned(),
            scope: "dev".to_owned(),
            resource_version: "9".to_owned(),
            updated_at: Utc::now(),
        };
        let outcome = persist_then_cache(
            &writer,
            &registry,
            &metrics,
            None,
            "default",
            sample("ok", "9"),
            Some(cp),
        )
        .await
        .expect("persist");
        assert_eq!(outcome, UpsertOutcome::Inserted);
        assert!(registry.get("ok").await.is_some());

        cancel.cancel();
        let _ = join.await;
    }

    #[tokio::test]
    async fn pipeline_cancel_stops_cleanly() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let metrics = Metrics::try_new().expect("metrics");
        let health = StorageHealthHandle::new(StorageBackend::Memory);
        health.mark_ready();
        let (writer, writer_join) =
            spawn_durable_writer(store, 4, cancel.child_token(), metrics.clone(), health);
        let registry = EventRegistry::new(10, Duration::from_secs(3600));
        let watch_state = WatchStateHandle::new();

        let mut cfg = Config::from_env().expect("config");
        cfg.events_mode = EventsMode::Demo;
        cfg.storage_path = None;
        cfg.cluster_id = "default".to_owned();

        let pipeline_cancel = cancel.child_token();
        let handle = tokio::spawn({
            let pipeline_cancel = pipeline_cancel.clone();
            async move {
                run_event_pipeline(cfg, registry, writer, metrics, watch_state, pipeline_cancel)
                    .await;
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        let _ = handle.await;
        let _ = writer_join.await;
    }
}

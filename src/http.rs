//! HTTP surface: health, readiness, metrics, events API, and streamable MCP.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::auth::{BearerAuthState, require_mcp_bearer};
use crate::config::EventsMode;
use crate::events::{
    EventQuery, EventRegistry, EventStoreHandle, StorageHealthHandle, WatchStateHandle,
};
use crate::mcp::{AppState, SentinelMcp, mcp_factory};
use crate::metrics::{Metrics, format_event_inventory_metrics};

/// Shared HTTP state.
#[derive(Clone)]
pub struct HttpState {
    pub registry: EventRegistry,
    pub store: EventStoreHandle,
    pub metrics: Metrics,
    pub watch_state: WatchStateHandle,
    pub storage_health: StorageHealthHandle,
    pub events_mode: EventsMode,
    pub mcp_allowed_hosts: Vec<String>,
    /// Mandatory Bearer token for `/mcp` and `/api/v1/events`.
    pub mcp_auth_token: Arc<str>,
    pub metrics_event_limit: usize,
    pub configured_namespaces: Vec<String>,
    pub build_version: String,
    pub git_sha: String,
}

/// Query params for `GET /api/v1/events`.
#[derive(Debug, Deserialize)]
pub struct ListEventsParams {
    #[serde(default = "default_events_limit")]
    pub limit: u32,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default, rename = "type")]
    pub type_filter: Option<String>,
}

fn default_events_limit() -> u32 {
    100
}

/// Build the Axum router including authenticated `/mcp` and `/api/v1/events`.
///
/// Public: `/health` (liveness) and `/ready` (readiness). `/metrics` stays on the
/// Service for Prometheus scrape and must not be exposed via external HTTPRoute.
pub fn router(state: HttpState, cancel: CancellationToken) -> Router {
    let mcp_state = AppState {
        registry: state.registry.clone(),
        store: state.store.clone(),
        metrics: state.metrics.clone(),
        watch_state: state.watch_state.clone(),
        storage_health: state.storage_health.clone(),
        events_mode: state.events_mode,
        configured_namespaces: state.configured_namespaces.clone(),
        build_version: state.build_version.clone(),
        git_sha: state.git_sha.clone(),
    };

    let mcp_config = StreamableHttpServerConfig::default()
        .with_cancellation_token(cancel)
        .with_allowed_hosts(state.mcp_allowed_hosts.clone());

    let mcp_service = StreamableHttpService::new(
        mcp_factory(mcp_state),
        LocalSessionManager::default().into(),
        mcp_config,
    );

    let auth_state = BearerAuthState {
        token: state.mcp_auth_token.clone(),
        metrics: state.metrics.clone(),
    };

    let mcp_routes =
        Router::new()
            .fallback_service(mcp_service)
            .layer(middleware::from_fn_with_state(
                auth_state.clone(),
                require_mcp_bearer,
            ));

    let protected_api = Router::new()
        .route("/api/v1/events", get(list_events))
        .layer(middleware::from_fn_with_state(
            auth_state,
            require_mcp_bearer,
        ));

    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_handler))
        .merge(protected_api)
        .nest("/mcp", mcp_routes)
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state))
}

async fn health(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    // Liveness: always 200 so transient storage failures do not restart the pod.
    state
        .metrics
        .http_requests
        .with_label_values(&["/health", "200"])
        .inc();
    let storage = state.storage_health.snapshot();
    let status = if storage.status.is_ready() {
        state.watch_state.get().health_status()
    } else {
        "unhealthy"
    };
    Json(json!({
        "status": status,
        "ready": storage.status.is_ready(),
        "events_mode": state.events_mode.as_str(),
        "registry_size": state.registry.len().await,
        "watch_state": state.watch_state.get().as_str(),
        "storage_status": storage.status.as_str(),
        "storage_backend": storage.backend.as_str(),
        "last_storage_error_at": storage.last_storage_error_at,
        "last_storage_success_at": storage.last_storage_success_at,
        "last_storage_error": storage.last_error,
    }))
}

async fn ready(State(state): State<Arc<HttpState>>) -> Response {
    let storage = state.storage_health.snapshot();
    let body = json!({
        "ready": storage.status.is_ready(),
        "storage_status": storage.status.as_str(),
        "storage_backend": storage.backend.as_str(),
        "last_storage_error_at": storage.last_storage_error_at,
        "last_storage_error": storage.last_error,
        "watch_state": state.watch_state.get().as_str(),
    });
    if storage.status.is_ready() {
        state
            .metrics
            .http_requests
            .with_label_values(&["/ready", "200"])
            .inc();
        (StatusCode::OK, Json(body)).into_response()
    } else {
        state
            .metrics
            .http_requests
            .with_label_values(&["/ready", "503"])
            .inc();
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    }
}

async fn metrics_handler(State(state): State<Arc<HttpState>>) -> Response {
    let now = Utc::now();
    let snap = state.storage_health.snapshot();
    state
        .metrics
        .set_checkpoint_ages(&snap.checkpoint_updated_at, now);
    if let Ok(stats) = state.store.stats().await {
        state.metrics.set_storage_stats(stats.rows, stats.bytes);
    }

    match state.metrics.gather_text() {
        Ok(mut body) => {
            let events = state
                .registry
                .list(EventQuery {
                    limit: state.metrics_event_limit,
                    ..EventQuery::default()
                })
                .await;
            body.push('\n');
            body.push_str(&format_event_inventory_metrics(
                &events,
                state.metrics_event_limit,
            ));
            state
                .metrics
                .http_requests
                .with_label_values(&["/metrics", "200"])
                .inc();
            (
                StatusCode::OK,
                [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                body,
            )
                .into_response()
        }
        Err(err) => {
            state
                .metrics
                .http_requests
                .with_label_values(&["/metrics", "500"])
                .inc();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": err.to_string() })),
            )
                .into_response()
        }
    }
}

async fn list_events(
    State(state): State<Arc<HttpState>>,
    Query(params): Query<ListEventsParams>,
) -> impl IntoResponse {
    let limit = usize::try_from(params.limit.clamp(1, 500)).unwrap_or(100);
    match state
        .store
        .list(&EventQuery {
            limit,
            namespace: params.namespace,
            reason: params.reason,
            type_filter: params.type_filter,
        })
        .await
    {
        Ok(events) => {
            state
                .metrics
                .http_requests
                .with_label_values(&["/api/v1/events", "200"])
                .inc();
            (
                StatusCode::OK,
                Json(json!({
                    "count": events.len(),
                    "events": events,
                })),
            )
                .into_response()
        }
        Err(err) => {
            state
                .metrics
                .http_requests
                .with_label_values(&["/api/v1/events", "500"])
                .inc();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": err.to_string() })),
            )
                .into_response()
        }
    }
}

#[must_use]
pub fn health_value(status: &str) -> Value {
    json!({ "status": status })
}

#[allow(dead_code)]
fn _type_check(_: SentinelMcp) {}

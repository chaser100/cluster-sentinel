//! Embedded MCP server (tools + resources) for agent callers.

use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler, handler::server::wrapper::Parameters,
    model::*, schemars, service::RequestContext, tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::EventsMode;
use crate::events::{
    EventQuery, EventRegistry, EventSearchQuery, EventStoreHandle, ParsedCursor,
    StorageHealthHandle, SummaryGroupBy, WatchStateHandle, encode_keyset_cursor, parse_cursor,
};
use crate::metrics::Metrics;

/// Shared application state visible to MCP tools.
#[derive(Clone)]
pub struct AppState {
    pub registry: EventRegistry,
    pub store: EventStoreHandle,
    pub metrics: Metrics,
    pub watch_state: WatchStateHandle,
    pub storage_health: StorageHealthHandle,
    pub events_mode: EventsMode,
    pub configured_namespaces: Vec<String>,
    pub build_version: String,
    pub git_sha: String,
}

/// MCP service implementing ClusterSentinel tools/resources.
#[derive(Clone)]
pub struct SentinelMcp {
    state: AppState,
    tool_router: rmcp::handler::server::router::tool::ToolRouter<Self>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListRecentEventsArgs {
    /// Max events to return (1..=500).
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// Optional namespace filter.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Optional exact reason filter.
    #[serde(default)]
    pub reason: Option<String>,
    /// Optional type filter (`Normal` / `Warning`).
    #[serde(default)]
    pub type_filter: Option<String>,
}

fn default_limit() -> u32 {
    50
}

fn default_search_limit() -> u32 {
    50
}

fn default_summary_limit() -> u32 {
    20
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetEventArgs {
    /// Kubernetes event UID.
    pub uid: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchEventsArgs {
    #[serde(default = "default_search_limit")]
    pub limit: u32,
    /// Opaque cursor from a previous `next_cursor`.
    ///
    /// Preferred: versioned keyset `v1:<rfc3339>|<event_uid>`.
    /// Deprecated compatibility: decimal offset string.
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub types: Vec<String>,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub involved_kind: Option<String>,
    #[serde(default)]
    pub involved_name: Option<String>,
    #[serde(default)]
    pub involved_uid: Option<String>,
    #[serde(default)]
    pub source_component: Option<String>,
    #[serde(default)]
    pub message_contains: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SummarizeEventsArgs {
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub until: Option<DateTime<Utc>>,
    /// Optional exact event type (`Normal` / `Warning`).
    #[serde(default, rename = "type")]
    pub type_filter: Option<String>,
    /// Grouping dimensions: `namespace`, `reason`, `involved_kind`, `type`.
    #[serde(default = "default_group_by")]
    pub group_by: Vec<String>,
    #[serde(default = "default_summary_limit")]
    pub limit: u32,
}

fn default_group_by() -> Vec<String> {
    vec![
        "namespace".to_owned(),
        "reason".to_owned(),
        "involved_kind".to_owned(),
    ]
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct HealthOutput {
    status: String,
    ready: bool,
    events_mode: String,
    registry_size: usize,
    store_rows: usize,
    watch_state: String,
    started_at: DateTime<Utc>,
    uptime_seconds: i64,
    last_event_at: Option<DateTime<Utc>>,
    last_watch_success_at: Option<DateTime<Utc>>,
    last_watch_error_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    consecutive_failures: u32,
    configured_namespaces: Vec<String>,
    registry_capacity: usize,
    retention_seconds: u64,
    oldest_event_at: Option<DateTime<Utc>>,
    newest_event_at: Option<DateTime<Utc>>,
    build_version: String,
    git_sha: String,
    storage_status: String,
    storage_backend: String,
    last_storage_error_at: Option<DateTime<Utc>>,
    last_storage_success_at: Option<DateTime<Utc>>,
    last_storage_error: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct ListRecentEventsOutput {
    events: Vec<crate::events::ClusterEvent>,
    returned: usize,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct SearchEventsOutput {
    events: Vec<crate::events::ClusterEvent>,
    matched: usize,
    returned: usize,
    truncated: bool,
    next_cursor: Option<String>,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct SummaryGroupOutput {
    namespace: Option<String>,
    reason: Option<String>,
    involved_kind: Option<String>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    event_objects: usize,
    occurrences: i64,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    affected_objects: usize,
    sample_message: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct SummarizeEventsOutput {
    groups: Vec<SummaryGroupOutput>,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct MetricsSummaryOutput {
    registry_size: usize,
    store_rows: usize,
    events_deduped_total: u64,
    watch_restarts_total: u64,
    watch_errors_total: u64,
    watch_state: String,
    events_mode: String,
    storage_status: String,
    storage_backend: String,
    storage_rows: i64,
    storage_bytes: i64,
    storage_last_success_timestamp: i64,
}

fn parse_group_by(raw: &[String]) -> Result<Vec<SummaryGroupBy>, McpError> {
    let mut out = Vec::with_capacity(raw.len());
    for item in raw {
        let parsed = match item.trim().to_ascii_lowercase().as_str() {
            "namespace" => SummaryGroupBy::Namespace,
            "reason" => SummaryGroupBy::Reason,
            "involved_kind" => SummaryGroupBy::InvolvedKind,
            "type" => SummaryGroupBy::Type,
            other => {
                return Err(McpError::invalid_params(
                    format!(
                        "unsupported group_by '{other}' (expected namespace|reason|involved_kind|type)"
                    ),
                    None,
                ));
            }
        };
        if !out.contains(&parsed) {
            out.push(parsed);
        }
    }
    if out.is_empty() {
        out = vec![
            SummaryGroupBy::Namespace,
            SummaryGroupBy::Reason,
            SummaryGroupBy::InvolvedKind,
        ];
    }
    Ok(out)
}

fn validate_event_type(event_type: &str) -> Result<(), McpError> {
    if matches!(event_type, "Normal" | "Warning") {
        return Ok(());
    }
    Err(McpError::invalid_params(
        format!("unsupported event type '{event_type}' (expected Normal|Warning)"),
        None,
    ))
}

fn validate_event_types(event_types: &[String]) -> Result<(), McpError> {
    for event_type in event_types {
        validate_event_type(event_type)?;
    }
    Ok(())
}

struct CursorQueryParts {
    offset: usize,
    after: Option<(DateTime<Utc>, String)>,
}

fn cursor_to_query_parts(cursor: Option<&str>) -> Result<CursorQueryParts, McpError> {
    match parse_cursor(cursor).map_err(|err| McpError::invalid_params(err, None))? {
        ParsedCursor::Start => Ok(CursorQueryParts {
            offset: 0,
            after: None,
        }),
        ParsedCursor::Offset(offset) => Ok(CursorQueryParts {
            offset,
            after: None,
        }),
        ParsedCursor::Keyset {
            observed_at,
            event_uid,
        } => Ok(CursorQueryParts {
            offset: 0,
            after: Some((observed_at, event_uid)),
        }),
    }
}

fn store_err(err: impl std::fmt::Display) -> McpError {
    McpError::internal_error(format!("event store error: {err}"), None)
}

#[tool_router]
impl SentinelMcp {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Return ClusterSentinel health and watch state",
        output_schema = rmcp::handler::server::tool::schema_for_type::<HealthOutput>(),
        annotations(
            title = "Get health",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_health(&self) -> Result<CallToolResult, McpError> {
        self.with_tool_metrics("get_health", 0, || async {
            let payload = self.health_json().await?;
            Ok(CallToolResult::structured(payload))
        })
        .await
    }

    #[tool(
        description = "List recent registered Kubernetes cluster events (newest observed_at first)",
        output_schema = rmcp::handler::server::tool::schema_for_type::<ListRecentEventsOutput>(),
        annotations(
            title = "List recent events",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_recent_events(
        &self,
        Parameters(args): Parameters<ListRecentEventsArgs>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(event_type) = args.type_filter.as_deref() {
            validate_event_type(event_type)?;
        }
        let limit = usize::try_from(args.limit.clamp(1, 500)).unwrap_or(50);
        self.with_tool_metrics("list_recent_events", 0, || async {
            let events = self
                .state
                .store
                .list(&EventQuery {
                    limit,
                    namespace: args.namespace,
                    reason: args.reason,
                    type_filter: args.type_filter,
                })
                .await
                .map_err(store_err)?;
            let returned = events.len();
            let payload = serde_json::to_value(ListRecentEventsOutput {
                events,
                returned,
                generated_at: Utc::now(),
            })
            .map_err(|err| {
                McpError::internal_error(format!("failed to serialize events: {err}"), None)
            })?;
            Ok((CallToolResult::structured(payload), returned as u64))
        })
        .await
    }

    #[tool(
        description = "Fetch one registered cluster event by Kubernetes UID",
        output_schema = rmcp::handler::server::tool::schema_for_type::<crate::events::ClusterEvent>(),
        annotations(
            title = "Get event",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_event(
        &self,
        Parameters(args): Parameters<GetEventArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.with_tool_metrics("get_event", 0, || async {
            match self.state.store.get(&args.uid).await.map_err(store_err)? {
                Some(event) => {
                    let payload = serde_json::to_value(event).map_err(|err| {
                        McpError::internal_error(format!("failed to serialize event: {err}"), None)
                    })?;
                    Ok((CallToolResult::structured(payload), 1_u64))
                }
                None => Err(McpError::resource_not_found(
                    format!("event uid '{}' not found", args.uid),
                    None,
                )),
            }
        })
        .await
    }

    #[tool(
        description = "Search retained cluster events with rich filters and keyset cursor pagination",
        output_schema = rmcp::handler::server::tool::schema_for_type::<SearchEventsOutput>(),
        annotations(
            title = "Search events",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn search_events(
        &self,
        Parameters(args): Parameters<SearchEventsArgs>,
    ) -> Result<CallToolResult, McpError> {
        validate_event_types(&args.types)?;
        let limit = usize::try_from(args.limit.clamp(1, 500)).unwrap_or(50);
        let cursor = cursor_to_query_parts(args.cursor.as_deref())?;
        self.with_tool_metrics("search_events", 0, || async {
            let result = self
                .state
                .store
                .search(&EventSearchQuery {
                    limit,
                    offset: cursor.offset,
                    after: cursor.after,
                    since: args.since,
                    until: args.until,
                    namespaces: args.namespaces,
                    types: args.types,
                    reasons: args.reasons,
                    involved_kind: args.involved_kind,
                    involved_name: args.involved_name,
                    involved_uid: args.involved_uid,
                    source_component: args.source_component,
                    message_contains: args.message_contains,
                })
                .await
                .map_err(store_err)?;
            let returned = result.returned as u64;
            let next_cursor = result
                .next_after
                .as_ref()
                .map(|(observed_at, uid)| encode_keyset_cursor(*observed_at, uid));
            let payload = json!({
                "events": result.events,
                "matched": result.matched,
                "returned": result.returned,
                "truncated": result.truncated,
                "next_cursor": next_cursor,
                "generated_at": Utc::now(),
            });
            Ok((CallToolResult::structured(payload), returned))
        })
        .await
    }

    #[tool(
        description = "Aggregate retained cluster events by namespace/reason/involved_kind/type",
        output_schema = rmcp::handler::server::tool::schema_for_type::<SummarizeEventsOutput>(),
        annotations(
            title = "Summarize events",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn summarize_events(
        &self,
        Parameters(args): Parameters<SummarizeEventsArgs>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(event_type) = args.type_filter.as_deref() {
            validate_event_type(event_type)?;
        }
        let group_by = parse_group_by(&args.group_by)?;
        let limit = usize::try_from(args.limit.clamp(1, 200)).unwrap_or(20);
        self.with_tool_metrics("summarize_events", 0, || async {
            let groups = self
                .state
                .store
                .summarize(
                    args.since,
                    args.until,
                    args.type_filter.as_deref(),
                    &group_by,
                    limit,
                )
                .await
                .map_err(store_err)?;
            let returned = groups.len() as u64;
            let payload = json!({
                "groups": groups.iter().map(|group| json!({
                    "namespace": group.key.namespace,
                    "reason": group.key.reason,
                    "involved_kind": group.key.involved_kind,
                    "type": group.key.event_type,
                    "event_objects": group.event_objects,
                    "occurrences": group.occurrences,
                    "first_seen": group.first_seen,
                    "last_seen": group.last_seen,
                    "affected_objects": group.affected_objects,
                    "sample_message": group.sample_message,
                })).collect::<Vec<_>>(),
                "generated_at": Utc::now(),
            });
            Ok((CallToolResult::structured(payload), returned))
        })
        .await
    }

    #[tool(
        description = "Return a compact metrics summary for agent triage",
        output_schema = rmcp::handler::server::tool::schema_for_type::<MetricsSummaryOutput>(),
        annotations(
            title = "Get metrics summary",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_metrics_summary(&self) -> Result<CallToolResult, McpError> {
        self.with_tool_metrics("get_metrics_summary", 0, || async {
            let store_rows = self.state.store.count().await.map_err(store_err)?;
            let payload = json!({
                "registry_size": self.state.registry.len().await,
                "store_rows": store_rows,
                "events_deduped_total": self.state.metrics.events_deduped.get(),
                "watch_restarts_total": self.state.metrics.watch_restarts.get(),
                "watch_errors_total": self.state.metrics.watch_errors.get(),
                "watch_state": self.state.watch_state.get().as_str(),
                "events_mode": self.state.events_mode.as_str(),
                "storage_status": self.state.storage_health.status().as_str(),
                "storage_backend": self.state.storage_health.backend().as_str(),
                "storage_rows": self.state.metrics.storage_rows.get(),
                "storage_bytes": self.state.metrics.storage_bytes.get(),
                "storage_last_success_timestamp": self.state.metrics.storage_last_success_timestamp.get(),
            });
            Ok(CallToolResult::structured(payload))
        })
        .await
    }

    async fn health_json(&self) -> Result<serde_json::Value, McpError> {
        let snap = self.state.watch_state.snapshot();
        let (oldest, newest) = self
            .state
            .store
            .observed_bounds()
            .await
            .map_err(store_err)?;
        let store_rows = self.state.store.count().await.map_err(store_err)?;
        let uptime_seconds = (Utc::now() - snap.started_at).num_seconds().max(0);
        let storage = self.state.storage_health.snapshot();
        let status = if storage.status.is_ready() {
            snap.state.health_status()
        } else {
            "unhealthy"
        };
        Ok(json!({
            "status": status,
            "ready": storage.status.is_ready(),
            "events_mode": self.state.events_mode.as_str(),
            "registry_size": self.state.registry.len().await,
            "store_rows": store_rows,
            "watch_state": snap.state.as_str(),
            "started_at": snap.started_at,
            "uptime_seconds": uptime_seconds,
            "last_event_at": snap.last_event_at,
            "last_watch_success_at": snap.last_watch_success_at,
            "last_watch_error_at": snap.last_watch_error_at,
            "last_error": snap.last_error,
            "consecutive_failures": snap.consecutive_failures,
            "configured_namespaces": self.state.configured_namespaces,
            "registry_capacity": self.state.registry.capacity(),
            "retention_seconds": self.state.registry.retention().as_secs(),
            "oldest_event_at": oldest,
            "newest_event_at": newest,
            "build_version": self.state.build_version,
            "git_sha": self.state.git_sha,
            "storage_status": storage.status.as_str(),
            "storage_backend": storage.backend.as_str(),
            "last_storage_error_at": storage.last_storage_error_at,
            "last_storage_success_at": storage.last_storage_success_at,
            "last_storage_error": storage.last_error,
        }))
    }

    async fn with_tool_metrics<F, Fut, T>(
        &self,
        tool: &str,
        _unused: u64,
        f: F,
    ) -> Result<CallToolResult, McpError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, McpError>>,
        T: IntoToolOutcome,
    {
        let started = Instant::now();
        match f().await {
            Ok(outcome) => {
                let (result, events) = outcome.into_tool_outcome();
                self.state.metrics.observe_mcp_tool(
                    tool,
                    "ok",
                    started.elapsed().as_secs_f64(),
                    events,
                );
                Ok(result)
            }
            Err(err) => {
                self.state.metrics.observe_mcp_tool(
                    tool,
                    "error",
                    started.elapsed().as_secs_f64(),
                    0,
                );
                Err(err)
            }
        }
    }
}

trait IntoToolOutcome {
    fn into_tool_outcome(self) -> (CallToolResult, u64);
}

impl IntoToolOutcome for CallToolResult {
    fn into_tool_outcome(self) -> (CallToolResult, u64) {
        (self, 0)
    }
}

impl IntoToolOutcome for (CallToolResult, u64) {
    fn into_tool_outcome(self) -> (CallToolResult, u64) {
        self
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SentinelMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
        )
        .with_server_info(Implementation::new(
            "clustersentinel",
            self.state.build_version.clone(),
        ))
        .with_instructions(
            "ClusterSentinel MCP: read-only Kubernetes Event index for agents. Prefer search_events/summarize_events for triage; no cluster mutation tools.",
        )
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult::with_all_items(vec![
            Resource::new("clustersentinel://status", "ClusterSentinel status"),
            Resource::new("clustersentinel://events/recent", "Recent cluster events"),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        match request.uri.as_str() {
            "clustersentinel://status" => {
                let body = self.health_json().await?.to_string();
                Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    body,
                    "clustersentinel://status",
                )])
                .into())
            }
            "clustersentinel://events/recent" => {
                let events = self
                    .state
                    .store
                    .list(&EventQuery {
                        limit: 50,
                        ..EventQuery::default()
                    })
                    .await
                    .map_err(store_err)?;
                let body = serde_json::to_string(&events).map_err(|err| {
                    McpError::internal_error(format!("failed to serialize events: {err}"), None)
                })?;
                Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    body,
                    "clustersentinel://events/recent",
                )])
                .into())
            }
            other => Err(McpError::resource_not_found(
                format!("unknown resource '{other}'"),
                Some(json!({ "uri": other })),
            )),
        }
    }
}

/// Factory for streamable HTTP MCP sessions.
pub fn mcp_factory(state: AppState) -> impl Fn() -> Result<SentinelMcp, std::io::Error> + Clone {
    let state = Arc::new(state);
    move || Ok(SentinelMcp::new((*state).clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_validation_is_strict() {
        assert!(validate_event_type("Normal").is_ok());
        assert!(validate_event_type("Warning").is_ok());
        assert!(validate_event_type("Critical").is_err());
        assert!(validate_event_type("warning").is_err());
    }
}

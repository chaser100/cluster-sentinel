//! Prometheus metrics registry for ClusterSentinel.

use std::sync::Arc;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder, opts,
};

use crate::error::{AppError, AppResult};
use crate::events::ClusterEvent;

/// Label names for `clustersentinel_events_registered_total`.
///
/// Kept low-cardinality on purpose (no object name/uid/message) so dashboards
/// can filter/sort without exploding Prometheus series.
pub const EVENTS_REGISTERED_LABELS: [&str; 5] = [
    "type",
    "reason",
    "event_namespace",
    "involved_kind",
    "source",
];

/// Default max events exported as inventory gauges on `/metrics`.
pub const DEFAULT_METRICS_EVENT_LIMIT: usize = 500;
/// Max characters kept in the Prometheus `message` label.
pub const EVENT_MESSAGE_LABEL_MAX: usize = 256;

/// Shared metrics handles.
#[derive(Clone, Debug)]
pub struct Metrics {
    pub registry: Registry,
    pub events_registered: IntCounterVec,
    pub events_deduped: IntCounter,
    pub registry_size: IntGauge,
    pub watch_restarts: IntCounter,
    pub watch_errors: IntCounter,
    pub http_requests: IntCounterVec,
    pub mcp_tool_calls: IntCounterVec,
    pub mcp_tool_duration: HistogramVec,
    pub mcp_auth_failures: IntCounter,
    pub mcp_active_sessions: IntGauge,
    pub mcp_events_returned: IntCounterVec,
}

impl Metrics {
    /// Build and register all metrics.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Other`] when Prometheus registration fails.
    pub fn try_new() -> AppResult<Self> {
        let registry = Registry::new();

        let events_registered = IntCounterVec::new(
            Opts::new(
                "clustersentinel_events_registered_total",
                "Cluster events inserted or updated in the registry",
            ),
            &EVENTS_REGISTERED_LABELS,
        )
        .map_err(metric_err)?;
        let events_deduped = IntCounter::with_opts(Opts::new(
            "clustersentinel_events_deduped_total",
            "Cluster events skipped as duplicates",
        ))
        .map_err(metric_err)?;

        let registry_size = IntGauge::with_opts(Opts::new(
            "clustersentinel_events_registry_size",
            "Current number of events retained in the registry",
        ))
        .map_err(metric_err)?;

        let watch_restarts = IntCounter::with_opts(Opts::new(
            "clustersentinel_watch_restarts_total",
            "Kubernetes watch restart count",
        ))
        .map_err(metric_err)?;

        let watch_errors = IntCounter::with_opts(Opts::new(
            "clustersentinel_watch_errors_total",
            "Kubernetes list/watch error count",
        ))
        .map_err(metric_err)?;

        let http_requests = IntCounterVec::new(
            opts!(
                "clustersentinel_http_requests_total",
                "HTTP requests handled by ClusterSentinel"
            ),
            &["path", "status"],
        )
        .map_err(metric_err)?;

        let mcp_tool_calls = IntCounterVec::new(
            Opts::new(
                "clustersentinel_mcp_tool_calls_total",
                "MCP tool invocations by tool and status",
            ),
            &["tool", "status"],
        )
        .map_err(metric_err)?;

        let mcp_tool_duration = HistogramVec::new(
            HistogramOpts::new(
                "clustersentinel_mcp_tool_duration_seconds",
                "MCP tool call latency in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
            ]),
            &["tool"],
        )
        .map_err(metric_err)?;

        let mcp_auth_failures = IntCounter::with_opts(Opts::new(
            "clustersentinel_mcp_auth_failures_total",
            "Bearer auth failures on protected MCP/API routes",
        ))
        .map_err(metric_err)?;

        let mcp_active_sessions = IntGauge::with_opts(Opts::new(
            "clustersentinel_mcp_active_sessions",
            "Approximate active streamable MCP sessions (best-effort)",
        ))
        .map_err(metric_err)?;

        let mcp_events_returned = IntCounterVec::new(
            Opts::new(
                "clustersentinel_mcp_events_returned_total",
                "Events returned by MCP tools",
            ),
            &["tool"],
        )
        .map_err(metric_err)?;

        registry
            .register(Box::new(events_registered.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(events_deduped.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(registry_size.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(watch_restarts.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(watch_errors.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(http_requests.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(mcp_tool_calls.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(mcp_tool_duration.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(mcp_auth_failures.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(mcp_active_sessions.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(mcp_events_returned.clone()))
            .map_err(metric_err)?;

        Ok(Self {
            registry,
            events_registered,
            events_deduped,
            registry_size,
            watch_restarts,
            watch_errors,
            http_requests,
            mcp_tool_calls,
            mcp_tool_duration,
            mcp_auth_failures,
            mcp_active_sessions,
            mcp_events_returned,
        })
    }

    /// Record one MCP tool invocation.
    pub fn observe_mcp_tool(&self, tool: &str, status: &str, duration_secs: f64, events: u64) {
        self.mcp_tool_calls.with_label_values(&[tool, status]).inc();
        self.mcp_tool_duration
            .with_label_values(&[tool])
            .observe(duration_secs);
        if events > 0 {
            self.mcp_events_returned
                .with_label_values(&[tool])
                .inc_by(events);
        }
    }

    /// Encode metrics in Prometheus text exposition format.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Other`] on encode failures.
    pub fn gather_text(&self) -> AppResult<String> {
        let families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&families, &mut buffer).map_err(|err| {
            AppError::Other(anyhow::anyhow!(
                "failed to encode prometheus metrics: {err}"
            ))
        })?;
        String::from_utf8(buffer).map_err(|err| {
            AppError::Other(anyhow::anyhow!("prometheus metrics were not utf-8: {err}"))
        })
    }

    /// Increment registered-events counter using dashboard filter labels.
    pub fn observe_registered(&self, event: &ClusterEvent) {
        self.observe_registered_labels(
            &event.event_type,
            &event.reason,
            &event.namespace,
            &event.involved_object.kind,
            event.source_component.as_deref(),
        );
    }

    /// Increment registered-events counter from explicit label fields.
    pub fn observe_registered_labels(
        &self,
        event_type: &str,
        reason: &str,
        namespace: &str,
        involved_kind: &str,
        source_component: Option<&str>,
    ) {
        let event_type = label_or_none(event_type);
        let reason = label_or_none(reason);
        let namespace = label_or_none(namespace);
        let involved_kind = label_or_none(involved_kind);
        let source = source_component.map(label_or_none).unwrap_or("_none");

        self.events_registered
            .with_label_values(&[event_type, reason, namespace, involved_kind, source])
            .inc();
    }
}

/// Empty / whitespace labels become `_none` for stable PromQL filters.
fn label_or_none(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.is_empty() { "_none" } else { trimmed }
}

fn metric_err(err: prometheus::Error) -> AppError {
    AppError::Other(anyhow::anyhow!("prometheus registry error: {err}"))
}

/// Wrap metrics in Arc for shared ownership across tasks.
#[must_use]
pub fn shared(metrics: Metrics) -> Arc<Metrics> {
    Arc::new(metrics)
}

/// Escape a Prometheus label value (`\`, `\n`, `"`).
#[must_use]
pub fn escape_prom_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '"' => out.push_str("\\\""),
            other => out.push(other),
        }
    }
    out
}

/// Truncate on a UTF-8 char boundary and append `…` when clipped.
#[must_use]
pub fn truncate_prom_message(message: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let trimmed = message.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_owned();
    }
    let keep = max_chars.saturating_sub(1);
    let mut out: String = trimmed.chars().take(keep).collect();
    out.push('…');
    out
}

/// kubectl-style involved object reference (`Kind/name`).
#[must_use]
pub fn involved_object_ref(event: &ClusterEvent) -> String {
    let kind = label_or_none(&event.involved_object.kind);
    let name = label_or_none(&event.involved_object.name);
    format!("{kind}/{name}")
}

/// Best-effort last-seen unix timestamp for inventory gauges.
#[must_use]
pub fn event_last_seen_unix(event: &ClusterEvent) -> i64 {
    event.observed_at().timestamp()
}

/// Render bounded event inventory gauges for Grafana table panels.
///
/// Series cardinality is capped by `limit` (newest-first input expected).
/// Full messages remain available via MCP / `/api/v1/events`.
#[must_use]
pub fn format_event_inventory_metrics(events: &[ClusterEvent], limit: usize) -> String {
    let limit = limit.clamp(1, 2_000);
    let mut out = String::with_capacity(events.len().min(limit).saturating_mul(256));
    out.push_str(
        "# HELP clustersentinel_event_last_seen_timestamp Unix timestamp of last observation for a retained cluster event\n",
    );
    out.push_str("# TYPE clustersentinel_event_last_seen_timestamp gauge\n");

    for event in events.iter().take(limit) {
        let event_namespace = escape_prom_label(label_or_none(&event.namespace));
        let event_type = escape_prom_label(label_or_none(&event.event_type));
        let reason = escape_prom_label(label_or_none(&event.reason));
        let involved_object = escape_prom_label(&involved_object_ref(event));
        let message = escape_prom_label(&truncate_prom_message(
            &event.message,
            EVENT_MESSAGE_LABEL_MAX,
        ));
        let count = event.count.max(0);
        let source = escape_prom_label(
            event
                .source_component
                .as_deref()
                .map(label_or_none)
                .unwrap_or("_none"),
        );
        let ts = event_last_seen_unix(event);
        out.push_str(&format!(
            "clustersentinel_event_last_seen_timestamp{{event_namespace=\"{event_namespace}\",type=\"{event_type}\",reason=\"{reason}\",involved_object=\"{involved_object}\",message=\"{message}\",count=\"{count}\",source=\"{source}\"}} {ts}\n"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::InvolvedObject;
    use chrono::{TimeZone, Utc};

    #[test]
    fn observe_registered_emits_filter_labels() {
        let metrics = Metrics::try_new().expect("metrics");
        let event = ClusterEvent {
            uid: "u1".into(),
            namespace: "demo".into(),
            name: "e1".into(),
            resource_version: "1".into(),
            event_type: "Warning".into(),
            reason: "BackOff".into(),
            message: "crash".into(),
            count: 1,
            involved_object: InvolvedObject {
                kind: "Pod".into(),
                namespace: "demo".into(),
                name: "web".into(),
                uid: None,
                api_version: Some("v1".into()),
            },
            source_component: Some("kubelet".into()),
            first_timestamp: None,
            last_timestamp: None,
            event_time: None,
            registered_at: Utc::now(),
        };
        metrics.observe_registered(&event);
        let text = metrics.gather_text().expect("encode");
        assert!(
            text.contains(
                "clustersentinel_events_registered_total{event_namespace=\"demo\",involved_kind=\"Pod\",reason=\"BackOff\",source=\"kubelet\",type=\"Warning\"}"
            ),
            "unexpected metrics text: {text}"
        );
    }

    #[test]
    fn empty_labels_map_to_none() {
        assert_eq!(label_or_none(""), "_none");
        assert_eq!(label_or_none("  "), "_none");
        assert_eq!(label_or_none("Pod"), "Pod");
    }

    #[test]
    fn inventory_metrics_include_object_and_message() {
        let last_seen = Utc.with_ymd_and_hms(2026, 9, 2, 13, 50, 0).unwrap();
        let event = ClusterEvent {
            uid: "u1".into(),
            namespace: "kube-system".into(),
            name: "e1".into(),
            resource_version: "1".into(),
            event_type: "Warning".into(),
            reason: "VolumeFailedDelete".into(),
            message: "rpc error: code = Internal desc = missing configuration".into(),
            count: 3,
            involved_object: InvolvedObject {
                kind: "PersistentVolume".into(),
                namespace: String::new(),
                name: "pvc-f3308b76".into(),
                uid: None,
                api_version: Some("v1".into()),
            },
            source_component: Some("cephfs.csi.ceph.com".into()),
            first_timestamp: None,
            last_timestamp: Some(last_seen),
            event_time: None,
            registered_at: last_seen,
        };
        let text = format_event_inventory_metrics(std::slice::from_ref(&event), 10);
        assert!(text.contains("clustersentinel_event_last_seen_timestamp{"));
        assert!(text.contains("involved_object=\"PersistentVolume/pvc-f3308b76\""));
        assert!(
            text.contains("message=\"rpc error: code = Internal desc = missing configuration\"")
        );
        assert!(text.contains("reason=\"VolumeFailedDelete\""));
        assert!(text.contains("count=\"3\""));
        assert!(text.contains(&format!("}} {}\n", last_seen.timestamp())));
    }

    #[test]
    fn truncates_and_escapes_message_label() {
        let long = "a".repeat(300);
        let truncated = truncate_prom_message(&long, 16);
        assert_eq!(truncated.chars().count(), 16);
        assert!(truncated.ends_with('…'));
        assert_eq!(escape_prom_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}

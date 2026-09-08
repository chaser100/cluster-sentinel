//! Prometheus metrics registry for ClusterSentinel.

use std::sync::Arc;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder, opts,
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

/// Low-cardinality `result` labels for `clustersentinel_storage_writes_total`.
pub const STORAGE_WRITE_RESULTS: [&str; 3] = ["ok", "error", "deduped"];
/// Low-cardinality `operation` labels for storage error/latency series.
pub const STORAGE_OPERATIONS: [&str; 4] = ["upsert", "checkpoint", "prune", "open"];

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
    pub storage_writes: IntCounterVec,
    pub storage_write_duration: HistogramVec,
    pub storage_errors: IntCounterVec,
    pub storage_rows: IntGauge,
    pub storage_bytes: IntGauge,
    pub storage_pruned: IntCounterVec,
    pub storage_last_success_timestamp: IntGauge,
    pub watch_checkpoint_age_seconds: IntGaugeVec,
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

        let storage_writes = IntCounterVec::new(
            Opts::new(
                "clustersentinel_storage_writes_total",
                "Durable storage write attempts by result",
            ),
            &["result"],
        )
        .map_err(metric_err)?;

        let storage_write_duration = HistogramVec::new(
            HistogramOpts::new(
                "clustersentinel_storage_write_duration_seconds",
                "Durable storage operation latency in seconds",
            )
            .buckets(vec![
                0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
            ]),
            &["operation"],
        )
        .map_err(metric_err)?;

        let storage_errors = IntCounterVec::new(
            Opts::new(
                "clustersentinel_storage_errors_total",
                "Durable storage errors by operation",
            ),
            &["operation"],
        )
        .map_err(metric_err)?;

        let storage_rows = IntGauge::with_opts(Opts::new(
            "clustersentinel_storage_rows",
            "Number of durable event rows in SQLite",
        ))
        .map_err(metric_err)?;

        let storage_bytes = IntGauge::with_opts(Opts::new(
            "clustersentinel_storage_bytes",
            "Approximate SQLite database size in bytes (page_count * page_size)",
        ))
        .map_err(metric_err)?;

        let storage_pruned = IntCounterVec::new(
            Opts::new(
                "clustersentinel_storage_pruned_total",
                "Durable rows pruned by retention policy",
            ),
            &["reason"],
        )
        .map_err(metric_err)?;

        let storage_last_success_timestamp = IntGauge::with_opts(Opts::new(
            "clustersentinel_storage_last_success_timestamp",
            "Unix timestamp of the last successful durable storage write",
        ))
        .map_err(metric_err)?;

        let watch_checkpoint_age_seconds = IntGaugeVec::new(
            Opts::new(
                "clustersentinel_watch_checkpoint_age_seconds",
                "Age in seconds of the last committed watch checkpoint per scope",
            ),
            &["scope"],
        )
        .map_err(metric_err)?;

        // Pre-register low-cardinality label sets so cardinality stays stable.
        for result in STORAGE_WRITE_RESULTS {
            let _ = storage_writes.with_label_values(&[result]);
        }
        for operation in STORAGE_OPERATIONS {
            let _ = storage_write_duration.with_label_values(&[operation]);
            let _ = storage_errors.with_label_values(&[operation]);
        }
        let _ = storage_pruned.with_label_values(&["age"]);
        let _ = storage_pruned.with_label_values(&["overflow"]);

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
        registry
            .register(Box::new(storage_writes.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(storage_write_duration.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(storage_errors.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(storage_rows.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(storage_bytes.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(storage_pruned.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(storage_last_success_timestamp.clone()))
            .map_err(metric_err)?;
        registry
            .register(Box::new(watch_checkpoint_age_seconds.clone()))
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
            storage_writes,
            storage_write_duration,
            storage_errors,
            storage_rows,
            storage_bytes,
            storage_pruned,
            storage_last_success_timestamp,
            watch_checkpoint_age_seconds,
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

    /// Observe one durable storage operation (latency + result/error counters).
    pub fn observe_storage_op(&self, operation: &str, duration_secs: f64, success: bool) {
        let operation = label_or_none(operation);
        self.storage_write_duration
            .with_label_values(&[operation])
            .observe(duration_secs);
        if success {
            self.storage_writes.with_label_values(&["ok"]).inc();
            self.storage_last_success_timestamp
                .set(chrono::Utc::now().timestamp());
        } else {
            self.storage_writes.with_label_values(&["error"]).inc();
            self.storage_errors.with_label_values(&[operation]).inc();
        }
    }

    /// Observe an upsert outcome that did not fail (ok or deduped).
    pub fn observe_storage_upsert_ok(&self, duration_secs: f64, deduped: bool) {
        self.storage_write_duration
            .with_label_values(&["upsert"])
            .observe(duration_secs);
        if deduped {
            self.storage_writes.with_label_values(&["deduped"]).inc();
        } else {
            self.storage_writes.with_label_values(&["ok"]).inc();
        }
        self.storage_last_success_timestamp
            .set(chrono::Utc::now().timestamp());
    }

    /// Refresh size gauges from a store stats snapshot.
    pub fn set_storage_stats(&self, rows: usize, bytes: u64) {
        self.storage_rows
            .set(i64::try_from(rows).unwrap_or(i64::MAX));
        self.storage_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    /// Increment prune counters and refresh size gauges.
    pub fn observe_prune(&self, age: usize, overflow: usize, rows: usize, bytes: u64) {
        if age > 0 {
            self.storage_pruned
                .with_label_values(&["age"])
                .inc_by(u64::try_from(age).unwrap_or(u64::MAX));
        }
        if overflow > 0 {
            self.storage_pruned
                .with_label_values(&["overflow"])
                .inc_by(u64::try_from(overflow).unwrap_or(u64::MAX));
        }
        self.set_storage_stats(rows, bytes);
    }

    /// Set checkpoint age gauges from known scope timestamps.
    pub fn set_checkpoint_ages(
        &self,
        checkpoints: &std::collections::HashMap<String, chrono::DateTime<chrono::Utc>>,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        for (scope, updated_at) in checkpoints {
            let age = (now - *updated_at).num_seconds().max(0);
            self.watch_checkpoint_age_seconds
                .with_label_values(&[label_or_none(scope)])
                .set(age);
        }
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

    #[test]
    fn storage_metrics_keep_stable_low_cardinality_labels() {
        let metrics = Metrics::try_new().expect("metrics");
        metrics.observe_storage_upsert_ok(0.01, false);
        metrics.observe_storage_upsert_ok(0.02, true);
        metrics.observe_storage_op("upsert", 0.03, false);
        metrics.observe_prune(2, 1, 10, 4096);
        metrics.set_checkpoint_ages(
            &std::collections::HashMap::from([(
                "all".to_owned(),
                chrono::Utc::now() - chrono::Duration::seconds(42),
            )]),
            chrono::Utc::now(),
        );
        let text = metrics.gather_text().expect("encode");
        for needle in [
            "clustersentinel_storage_writes_total{result=\"ok\"}",
            "clustersentinel_storage_writes_total{result=\"deduped\"}",
            "clustersentinel_storage_writes_total{result=\"error\"}",
            "clustersentinel_storage_errors_total{operation=\"upsert\"}",
            "clustersentinel_storage_pruned_total{reason=\"age\"}",
            "clustersentinel_storage_pruned_total{reason=\"overflow\"}",
            "clustersentinel_storage_rows",
            "clustersentinel_storage_bytes",
            "clustersentinel_storage_last_success_timestamp",
            "clustersentinel_watch_checkpoint_age_seconds{scope=\"all\"}",
            "clustersentinel_storage_write_duration_seconds_bucket{operation=\"upsert\"",
        ] {
            assert!(text.contains(needle), "missing {needle} in:\n{text}");
        }
        // Cardinality guard: only the three declared result labels appear.
        let result_series = text
            .lines()
            .filter(|l| l.starts_with("clustersentinel_storage_writes_total{"))
            .count();
        assert_eq!(result_series, 3, "unexpected write result cardinality");
    }
}

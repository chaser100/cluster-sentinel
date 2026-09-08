//! Application configuration (env-first for Kubernetes).

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::{AppError, AppResult};

/// Default bind address.
pub const DEFAULT_BIND: &str = "0.0.0.0:8080";
/// Default list page size for event bootstrap.
pub const DEFAULT_LIST_LIMIT: u32 = 500;
/// Default watch timeout (seconds); must stay &lt; kube WatchParams max (295).
pub const DEFAULT_WATCH_TIMEOUT_SECS: u64 = 290;
/// Default initial watch/list backoff (seconds).
pub const DEFAULT_WATCH_BACKOFF_SECS: u64 = 5;
/// Default max exponential backoff (seconds).
pub const DEFAULT_WATCH_BACKOFF_MAX_SECS: u64 = 60;
/// Default in-memory registry capacity.
pub const DEFAULT_REGISTRY_CAPACITY: usize = 10_000;
/// Default registry entry TTL (seconds).
pub const DEFAULT_DEDUP_TTL_SECS: u64 = 3600;
/// Default max events exported on `/metrics` inventory gauges.
pub const DEFAULT_METRICS_EVENT_LIMIT: usize = 500;
/// Default durable SQLite path (PVC mount in cluster).
pub const DEFAULT_STORAGE_PATH: &str = "/var/lib/clustersentinel/events.db";
/// Default durable retention (7 days).
pub const DEFAULT_STORAGE_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;
/// Default durable max events cap.
pub const DEFAULT_STORAGE_MAX_EVENTS: usize = 250_000;
/// Default bounded writer queue depth (backpressure).
pub const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 64;
/// Default logical cluster id for durable rows / checkpoints.
pub const DEFAULT_CLUSTER_ID: &str = "default";
/// Default prune batch size.
pub const DEFAULT_STORAGE_PRUNE_BATCH: usize = 1_000;
/// Default MCP Host allow-list (DNS-rebinding protection).
pub const DEFAULT_MCP_ALLOWED_HOSTS: &[&str] = &[
    "localhost",
    "127.0.0.1",
    "::1",
    "clustersentinel",
    "clustersentinel.clustersentinel.svc",
    "clustersentinel.clustersentinel.svc.cluster.local",
];

/// Event ingestion backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventsMode {
    /// Use Kubernetes list/watch APIs.
    Kubernetes,
    /// Seed synthetic events (local/CI without a cluster).
    Demo,
}

impl EventsMode {
    fn parse(raw: &str) -> AppResult<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "kubernetes" | "k8s" => Ok(Self::Kubernetes),
            "demo" => Ok(Self::Demo),
            other => Err(AppError::Config(format!(
                "unknown CLUSTERSENTINEL_EVENTS_MODE '{other}' (expected kubernetes|demo)"
            ))),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kubernetes => "kubernetes",
            Self::Demo => "demo",
        }
    }
}

/// Runtime configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub events_mode: EventsMode,
    pub list_limit: u32,
    pub watch_timeout: Duration,
    pub watch_backoff: Duration,
    pub watch_backoff_max: Duration,
    pub registry_capacity: usize,
    pub dedup_ttl: Duration,
    /// Max events exported as `clustersentinel_event_last_seen_timestamp`.
    pub metrics_event_limit: usize,
    pub namespaces: Vec<String>,
    /// Hostnames accepted by streamable MCP (`Host` / `:authority`).
    pub mcp_allowed_hosts: Vec<String>,
    /// Bearer token for streamable HTTP `/mcp`. Required in HTTP mode; unused for `--mcp-stdio`.
    pub mcp_auth_token: Option<String>,
    /// SQLite path. `None` → in-memory (demo/tests without PVC).
    pub storage_path: Option<PathBuf>,
    /// Logical cluster id stored with durable rows.
    pub cluster_id: String,
    pub storage_retention: Duration,
    pub storage_max_events: usize,
    pub storage_prune_batch: usize,
    pub writer_queue_capacity: usize,
}

impl Config {
    /// Load configuration from process environment.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] when a value cannot be parsed.
    pub fn from_env() -> AppResult<Self> {
        let bind_addr = env::var("CLUSTERSENTINEL_BIND")
            .unwrap_or_else(|_| DEFAULT_BIND.to_owned())
            .parse()
            .map_err(|err| AppError::Config(format!("invalid CLUSTERSENTINEL_BIND: {err}")))?;

        let events_mode = EventsMode::parse(
            &env::var("CLUSTERSENTINEL_EVENTS_MODE").unwrap_or_else(|_| "kubernetes".to_owned()),
        )?;

        let list_limit = parse_u32("CLUSTERSENTINEL_LIST_LIMIT", DEFAULT_LIST_LIMIT)?;
        let watch_timeout_secs = parse_u64(
            "CLUSTERSENTINEL_WATCH_TIMEOUT_SECS",
            DEFAULT_WATCH_TIMEOUT_SECS,
        )?;
        let watch_backoff_secs = parse_u64(
            "CLUSTERSENTINEL_WATCH_BACKOFF_SECS",
            DEFAULT_WATCH_BACKOFF_SECS,
        )?;
        let watch_backoff_max_secs = parse_u64(
            "CLUSTERSENTINEL_WATCH_BACKOFF_MAX_SECS",
            DEFAULT_WATCH_BACKOFF_MAX_SECS,
        )?;
        let registry_capacity = parse_usize(
            "CLUSTERSENTINEL_REGISTRY_CAPACITY",
            DEFAULT_REGISTRY_CAPACITY,
        )?;
        let dedup_ttl_secs = parse_u64("CLUSTERSENTINEL_DEDUP_TTL_SECS", DEFAULT_DEDUP_TTL_SECS)?;
        let metrics_event_limit = parse_usize(
            "CLUSTERSENTINEL_METRICS_EVENT_LIMIT",
            DEFAULT_METRICS_EVENT_LIMIT,
        )?;

        if watch_backoff_max_secs < watch_backoff_secs {
            return Err(AppError::Config(
                "CLUSTERSENTINEL_WATCH_BACKOFF_MAX_SECS must be >= CLUSTERSENTINEL_WATCH_BACKOFF_SECS"
                    .to_owned(),
            ));
        }

        let namespaces = env::var("CLUSTERSENTINEL_NAMESPACES")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();

        let mcp_allowed_hosts = env::var("CLUSTERSENTINEL_MCP_ALLOWED_HOSTS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .filter(|hosts| !hosts.is_empty())
            .unwrap_or_else(|| {
                DEFAULT_MCP_ALLOWED_HOSTS
                    .iter()
                    .map(|host| (*host).to_owned())
                    .collect()
            });

        let mcp_auth_token = env::var("CLUSTERSENTINEL_MCP_AUTH_TOKEN")
            .ok()
            .map(|raw| raw.trim().to_owned())
            .filter(|token| !token.is_empty());

        let storage_path = parse_storage_path(events_mode)?;
        let cluster_id = env::var("CLUSTERSENTINEL_CLUSTER_ID")
            .ok()
            .map(|raw| raw.trim().to_owned())
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| DEFAULT_CLUSTER_ID.to_owned());
        let storage_retention_secs = parse_u64(
            "CLUSTERSENTINEL_STORAGE_RETENTION_SECS",
            DEFAULT_STORAGE_RETENTION_SECS,
        )?;
        let storage_max_events = parse_usize(
            "CLUSTERSENTINEL_STORAGE_MAX_EVENTS",
            DEFAULT_STORAGE_MAX_EVENTS,
        )?;
        let storage_prune_batch = parse_usize(
            "CLUSTERSENTINEL_STORAGE_PRUNE_BATCH",
            DEFAULT_STORAGE_PRUNE_BATCH,
        )?;
        let writer_queue_capacity = parse_usize(
            "CLUSTERSENTINEL_WRITER_QUEUE_CAPACITY",
            DEFAULT_WRITER_QUEUE_CAPACITY,
        )?;

        Ok(Self {
            bind_addr,
            events_mode,
            list_limit,
            watch_timeout: Duration::from_secs(watch_timeout_secs),
            watch_backoff: Duration::from_secs(watch_backoff_secs),
            watch_backoff_max: Duration::from_secs(watch_backoff_max_secs),
            registry_capacity,
            dedup_ttl: Duration::from_secs(dedup_ttl_secs),
            metrics_event_limit,
            namespaces,
            mcp_allowed_hosts,
            mcp_auth_token,
            storage_path,
            cluster_id,
            storage_retention: Duration::from_secs(storage_retention_secs),
            storage_max_events,
            storage_prune_batch,
            writer_queue_capacity,
        })
    }

    /// Bearer token required for streamable HTTP MCP (cannot be disabled).
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] when `CLUSTERSENTINEL_MCP_AUTH_TOKEN` is missing/empty.
    pub fn require_mcp_auth_token(&self) -> AppResult<&str> {
        self.mcp_auth_token
            .as_deref()
            .filter(|token| !token.is_empty())
            .ok_or_else(|| {
                AppError::Config(
                    "CLUSTERSENTINEL_MCP_AUTH_TOKEN is required for HTTP mode (MCP Bearer auth cannot be disabled)"
                        .to_owned(),
                )
            })
    }
}

fn parse_storage_path(events_mode: EventsMode) -> AppResult<Option<PathBuf>> {
    match env::var("CLUSTERSENTINEL_STORAGE_PATH") {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty()
                || trimmed.eq_ignore_ascii_case("memory")
                || trimmed.eq_ignore_ascii_case(":memory:")
            {
                Ok(None)
            } else {
                Ok(Some(PathBuf::from(trimmed)))
            }
        }
        Err(_) => match events_mode {
            EventsMode::Demo => Ok(None),
            EventsMode::Kubernetes => Ok(Some(PathBuf::from(DEFAULT_STORAGE_PATH))),
        },
    }
}

fn parse_u32(key: &str, default: u32) -> AppResult<u32> {
    match env::var(key) {
        Ok(raw) => raw
            .parse()
            .map_err(|err| AppError::Config(format!("invalid {key}: {err}"))),
        Err(_) => Ok(default),
    }
}

fn parse_u64(key: &str, default: u64) -> AppResult<u64> {
    match env::var(key) {
        Ok(raw) => raw
            .parse()
            .map_err(|err| AppError::Config(format!("invalid {key}: {err}"))),
        Err(_) => Ok(default),
    }
}

fn parse_usize(key: &str, default: usize) -> AppResult<usize> {
    match env::var(key) {
        Ok(raw) => raw
            .parse()
            .map_err(|err| AppError::Config(format!("invalid {key}: {err}"))),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_events_mode() {
        assert_eq!(EventsMode::parse("k8s").unwrap(), EventsMode::Kubernetes);
        assert_eq!(EventsMode::parse("demo").unwrap(), EventsMode::Demo);
        assert!(EventsMode::parse("prod").is_err());
    }

    #[test]
    fn default_mcp_allowed_hosts_include_service_dns_names() {
        assert!(
            DEFAULT_MCP_ALLOWED_HOSTS.contains(&"clustersentinel.clustersentinel.svc"),
            "namespace-qualified in-cluster Service DNS must be allowed"
        );
        assert!(
            DEFAULT_MCP_ALLOWED_HOSTS
                .contains(&"clustersentinel.clustersentinel.svc.cluster.local"),
            "fully qualified in-cluster Service DNS must be allowed"
        );
    }
}

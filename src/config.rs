//! Application configuration (env-first for Kubernetes).

use std::env;
use std::net::SocketAddr;
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
/// Default MCP Host allow-list (DNS-rebinding protection).
pub const DEFAULT_MCP_ALLOWED_HOSTS: &[&str] =
    &["localhost", "127.0.0.1", "::1", "clustersentinel"];

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
    fn default_mcp_allowed_hosts_include_local_and_service_names() {
        assert!(DEFAULT_MCP_ALLOWED_HOSTS.contains(&"localhost"));
        assert!(DEFAULT_MCP_ALLOWED_HOSTS.contains(&"clustersentinel"));
    }
}

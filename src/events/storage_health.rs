//! Storage readiness and diagnostic state for health/MCP/probes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use chrono::{DateTime, Utc};

/// Coarse durable-storage status for readiness and MCP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageStatus {
    /// Open/migrate in progress (before HTTP serve).
    Starting = 0,
    /// Writer can commit; probes should pass.
    Ready = 1,
    /// Transient failures observed; still attempting writes.
    Degraded = 2,
    /// DB unavailable / writer cannot commit; readiness must fail.
    Unavailable = 3,
}

impl StorageStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
        }
    }

    /// Whether Kubernetes readiness should succeed.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }
}

/// Which durable backend is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackend {
    Sqlite,
    Memory,
}

impl StorageBackend {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Memory => "memory",
        }
    }
}

/// Snapshot exported on `/health`, `/ready`, and MCP `get_health`.
#[derive(Debug, Clone)]
pub struct StorageHealthSnapshot {
    pub status: StorageStatus,
    pub backend: StorageBackend,
    pub last_storage_error_at: Option<DateTime<Utc>>,
    pub last_storage_success_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub checkpoint_updated_at: HashMap<String, DateTime<Utc>>,
}

/// Shared storage health handle (cheap to clone).
#[derive(Clone, Debug)]
pub struct StorageHealthHandle {
    status: Arc<AtomicU8>,
    backend: StorageBackend,
    last_storage_error_at: Arc<std::sync::Mutex<Option<DateTime<Utc>>>>,
    last_storage_success_at: Arc<std::sync::Mutex<Option<DateTime<Utc>>>>,
    last_error: Arc<std::sync::Mutex<Option<String>>>,
    consecutive_failures: Arc<std::sync::atomic::AtomicU32>,
    checkpoint_updated_at: Arc<std::sync::Mutex<HashMap<String, DateTime<Utc>>>>,
}

impl StorageHealthHandle {
    /// Create a handle for the given backend; starts in [`StorageStatus::Starting`].
    #[must_use]
    pub fn new(backend: StorageBackend) -> Self {
        Self {
            status: Arc::new(AtomicU8::new(StorageStatus::Starting as u8)),
            backend,
            last_storage_error_at: Arc::new(std::sync::Mutex::new(None)),
            last_storage_success_at: Arc::new(std::sync::Mutex::new(None)),
            last_error: Arc::new(std::sync::Mutex::new(None)),
            consecutive_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            checkpoint_updated_at: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    #[must_use]
    pub const fn backend(&self) -> StorageBackend {
        self.backend
    }

    pub fn set_status(&self, status: StorageStatus) {
        self.status.store(status as u8, Ordering::Relaxed);
    }

    /// Mark open+migrate complete and ready for traffic.
    pub fn mark_ready(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.set_status(StorageStatus::Ready);
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = None;
        }
    }

    #[must_use]
    pub fn status(&self) -> StorageStatus {
        match self.status.load(Ordering::Relaxed) {
            1 => StorageStatus::Ready,
            2 => StorageStatus::Degraded,
            3 => StorageStatus::Unavailable,
            _ => StorageStatus::Starting,
        }
    }

    /// Record a successful durable commit.
    pub fn note_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.set_status(StorageStatus::Ready);
        if let Ok(mut guard) = self.last_storage_success_at.lock() {
            *guard = Some(Utc::now());
        }
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = None;
        }
    }

    /// Record a durable write/open failure (readiness becomes false).
    pub fn note_error(&self, error: &str) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        self.set_status(StorageStatus::Unavailable);
        if let Ok(mut guard) = self.last_storage_error_at.lock() {
            *guard = Some(Utc::now());
        }
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = Some(sanitize_error(error));
        }
    }

    /// Remember checkpoint `updated_at` for age gauges (low-cardinality scopes only).
    pub fn note_checkpoint(&self, scope: &str, updated_at: DateTime<Utc>) {
        if scope.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.checkpoint_updated_at.lock() {
            guard.insert(scope.to_owned(), updated_at);
        }
    }

    /// Clear a scope checkpoint timestamp (e.g. after 410 Gone).
    pub fn clear_checkpoint(&self, scope: &str) {
        if let Ok(mut guard) = self.checkpoint_updated_at.lock() {
            guard.remove(scope);
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> StorageHealthSnapshot {
        StorageHealthSnapshot {
            status: self.status(),
            backend: self.backend,
            last_storage_error_at: self.last_storage_error_at.lock().ok().and_then(|g| *g),
            last_storage_success_at: self.last_storage_success_at.lock().ok().and_then(|g| *g),
            last_error: self.last_error.lock().ok().and_then(|g| g.clone()),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            checkpoint_updated_at: self
                .checkpoint_updated_at
                .lock()
                .map(|g| g.clone())
                .unwrap_or_default(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_transitions_on_error_and_success() {
        let health = StorageHealthHandle::new(StorageBackend::Memory);
        assert_eq!(health.status(), StorageStatus::Starting);
        assert!(!health.status().is_ready());

        health.mark_ready();
        assert!(health.status().is_ready());

        health.note_error("sqlite busy");
        assert_eq!(health.status(), StorageStatus::Unavailable);
        assert!(!health.status().is_ready());
        let snap = health.snapshot();
        assert!(snap.last_storage_error_at.is_some());
        assert_eq!(snap.last_error.as_deref(), Some("sqlite busy"));
        assert_eq!(snap.consecutive_failures, 1);

        health.note_success();
        assert!(health.status().is_ready());
        assert_eq!(health.snapshot().consecutive_failures, 0);
        assert!(health.snapshot().last_error.is_none());
    }

    #[test]
    fn checkpoint_map_tracks_scopes() {
        let health = StorageHealthHandle::new(StorageBackend::Sqlite);
        let t = Utc::now();
        health.note_checkpoint("all", t);
        health.note_checkpoint("dev", t);
        assert_eq!(health.snapshot().checkpoint_updated_at.len(), 2);
        health.clear_checkpoint("dev");
        assert_eq!(health.snapshot().checkpoint_updated_at.len(), 1);
        assert!(health.snapshot().checkpoint_updated_at.contains_key("all"));
    }
}

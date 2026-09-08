//! Single-writer durable persistence with bounded backpressure.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{
    ClusterEvent, EventStore, PruneResult, SqliteEventStore, StorageHealthHandle, StoreError,
    UpsertOutcome, WatchCheckpoint,
};
use crate::metrics::Metrics;

/// Errors from the durable writer handle (channel closed or storage failure).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WriterError {
    #[error("durable writer channel closed")]
    Closed,
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[allow(clippy::large_enum_variant)]
enum WriterRequest {
    Upsert {
        cluster_id: String,
        event: ClusterEvent,
        checkpoint: Option<WatchCheckpoint>,
        reply: oneshot::Sender<Result<UpsertOutcome, StoreError>>,
    },
    SaveCheckpoint {
        checkpoint: WatchCheckpoint,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    ClearCheckpoint {
        cluster_id: String,
        scope: String,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    LoadCheckpoint {
        cluster_id: String,
        scope: String,
        reply: oneshot::Sender<Result<Option<WatchCheckpoint>, StoreError>>,
    },
    LoadRecent {
        cluster_id: String,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<ClusterEvent>, StoreError>>,
    },
    Prune {
        now: DateTime<Utc>,
        retention: Duration,
        max_events: usize,
        batch_size: usize,
        reply: oneshot::Sender<Result<PruneResult, StoreError>>,
    },
}

/// Cloneable handle that applies write-before-ack against the single writer task.
#[derive(Clone, Debug)]
pub struct DurableWriterHandle {
    tx: mpsc::Sender<WriterRequest>,
}

impl DurableWriterHandle {
    /// Idempotent upsert + optional checkpoint commit; caller awaits before updating cache.
    ///
    /// # Errors
    ///
    /// Returns [`WriterError`] when the writer is shut down or SQLite fails.
    pub async fn upsert_with_checkpoint(
        &self,
        cluster_id: impl Into<String>,
        event: ClusterEvent,
        checkpoint: Option<WatchCheckpoint>,
    ) -> Result<UpsertOutcome, WriterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(WriterRequest::Upsert {
                cluster_id: cluster_id.into(),
                event,
                checkpoint,
                reply,
            })
            .await
            .map_err(|_| WriterError::Closed)?;
        rx.await
            .map_err(|_| WriterError::Closed)?
            .map_err(WriterError::from)
    }

    /// Persist a bookmark/checkpoint without an event payload.
    ///
    /// # Errors
    ///
    /// Returns [`WriterError`] when the writer is shut down or SQLite fails.
    pub async fn save_checkpoint(&self, checkpoint: WatchCheckpoint) -> Result<(), WriterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(WriterRequest::SaveCheckpoint { checkpoint, reply })
            .await
            .map_err(|_| WriterError::Closed)?;
        rx.await
            .map_err(|_| WriterError::Closed)?
            .map_err(WriterError::from)
    }

    /// Drop a scope checkpoint (e.g. after HTTP 410 Gone).
    ///
    /// # Errors
    ///
    /// Returns [`WriterError`] when the writer is shut down or SQLite fails.
    pub async fn clear_checkpoint(
        &self,
        cluster_id: impl Into<String>,
        scope: impl Into<String>,
    ) -> Result<(), WriterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(WriterRequest::ClearCheckpoint {
                cluster_id: cluster_id.into(),
                scope: scope.into(),
                reply,
            })
            .await
            .map_err(|_| WriterError::Closed)?;
        rx.await
            .map_err(|_| WriterError::Closed)?
            .map_err(WriterError::from)
    }

    /// Load the last committed resourceVersion for a watch scope.
    ///
    /// # Errors
    ///
    /// Returns [`WriterError`] when the writer is shut down or SQLite fails.
    pub async fn load_checkpoint(
        &self,
        cluster_id: impl Into<String>,
        scope: impl Into<String>,
    ) -> Result<Option<WatchCheckpoint>, WriterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(WriterRequest::LoadCheckpoint {
                cluster_id: cluster_id.into(),
                scope: scope.into(),
                reply,
            })
            .await
            .map_err(|_| WriterError::Closed)?;
        rx.await
            .map_err(|_| WriterError::Closed)?
            .map_err(WriterError::from)
    }

    /// Load a bounded hot set for read-cache warm-up.
    ///
    /// # Errors
    ///
    /// Returns [`WriterError`] when the writer is shut down or SQLite fails.
    pub async fn load_recent(
        &self,
        cluster_id: impl Into<String>,
        limit: usize,
    ) -> Result<Vec<ClusterEvent>, WriterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(WriterRequest::LoadRecent {
                cluster_id: cluster_id.into(),
                limit,
                reply,
            })
            .await
            .map_err(|_| WriterError::Closed)?;
        rx.await
            .map_err(|_| WriterError::Closed)?
            .map_err(WriterError::from)
    }

    /// Run retention prune on the durable store.
    ///
    /// # Errors
    ///
    /// Returns [`WriterError`] when the writer is shut down or SQLite fails.
    pub async fn prune(
        &self,
        now: DateTime<Utc>,
        retention: Duration,
        max_events: usize,
        batch_size: usize,
    ) -> Result<PruneResult, WriterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(WriterRequest::Prune {
                now,
                retention,
                max_events,
                batch_size,
                reply,
            })
            .await
            .map_err(|_| WriterError::Closed)?;
        rx.await
            .map_err(|_| WriterError::Closed)?
            .map_err(WriterError::from)
    }
}

/// Spawn the single writer task sharing the SQLite store with read handles.
pub fn spawn_durable_writer(
    store: Arc<Mutex<SqliteEventStore>>,
    queue_capacity: usize,
    cancel: CancellationToken,
    metrics: Metrics,
    storage_health: StorageHealthHandle,
) -> (DurableWriterHandle, tokio::task::JoinHandle<()>) {
    let capacity = queue_capacity.max(1);
    let (tx, mut rx) = mpsc::channel::<WriterRequest>(capacity);
    let handle = DurableWriterHandle { tx };

    let join = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    debug!("durable writer shutting down");
                    break;
                }
                maybe_req = rx.recv() => {
                    let Some(req) = maybe_req else {
                        break;
                    };
                    dispatch(&store, &metrics, &storage_health, req).await;
                }
            }
        }
        while let Ok(req) = rx.try_recv() {
            dispatch(&store, &metrics, &storage_health, req).await;
        }
    });

    (handle, join)
}

async fn dispatch(
    store: &Arc<Mutex<SqliteEventStore>>,
    metrics: &Metrics,
    storage_health: &StorageHealthHandle,
    req: WriterRequest,
) {
    // Lock is held only for sync SQLite work — never across other `.await` points.
    let mut guard = store.lock().await;
    match req {
        WriterRequest::Upsert {
            cluster_id,
            event,
            checkpoint,
            reply,
        } => {
            let started = std::time::Instant::now();
            let result = guard.upsert_with_checkpoint(&cluster_id, &event, checkpoint.as_ref());
            let elapsed = started.elapsed().as_secs_f64();
            match &result {
                Ok(outcome) => {
                    metrics.observe_storage_upsert_ok(elapsed, *outcome == UpsertOutcome::Deduped);
                    storage_health.note_success();
                    if let Some(cp) = checkpoint.as_ref() {
                        storage_health.note_checkpoint(&cp.scope, cp.updated_at);
                    }
                    if let Ok(stats) = guard.stats() {
                        metrics.set_storage_stats(stats.rows, stats.bytes);
                    }
                }
                Err(err) => {
                    metrics.observe_storage_op("upsert", elapsed, false);
                    storage_health.note_error(&err.to_string());
                }
            }
            if reply.send(result).is_err() {
                warn!("durable writer upsert reply dropped");
            }
        }
        WriterRequest::SaveCheckpoint { checkpoint, reply } => {
            let started = std::time::Instant::now();
            let scope = checkpoint.scope.clone();
            let updated_at = checkpoint.updated_at;
            let result = guard.save_checkpoint(&checkpoint);
            let elapsed = started.elapsed().as_secs_f64();
            match &result {
                Ok(()) => {
                    metrics.observe_storage_op("checkpoint", elapsed, true);
                    storage_health.note_success();
                    storage_health.note_checkpoint(&scope, updated_at);
                }
                Err(err) => {
                    metrics.observe_storage_op("checkpoint", elapsed, false);
                    storage_health.note_error(&err.to_string());
                }
            }
            let _ = reply.send(result);
        }
        WriterRequest::ClearCheckpoint {
            cluster_id,
            scope,
            reply,
        } => {
            let started = std::time::Instant::now();
            let result = guard.clear_checkpoint(&cluster_id, &scope);
            let elapsed = started.elapsed().as_secs_f64();
            match &result {
                Ok(()) => {
                    metrics.observe_storage_op("checkpoint", elapsed, true);
                    storage_health.note_success();
                    storage_health.clear_checkpoint(&scope);
                }
                Err(err) => {
                    metrics.observe_storage_op("checkpoint", elapsed, false);
                    storage_health.note_error(&err.to_string());
                }
            }
            let _ = reply.send(result);
        }
        WriterRequest::LoadCheckpoint {
            cluster_id,
            scope,
            reply,
        } => {
            let result = guard.load_checkpoint(&cluster_id, &scope);
            if let Ok(Some(cp)) = &result {
                storage_health.note_checkpoint(&cp.scope, cp.updated_at);
            }
            let _ = reply.send(result);
        }
        WriterRequest::LoadRecent {
            cluster_id,
            limit,
            reply,
        } => {
            let result = guard.load_recent(&cluster_id, limit);
            let _ = reply.send(result);
        }
        WriterRequest::Prune {
            now,
            retention,
            max_events,
            batch_size,
            reply,
        } => {
            let started = std::time::Instant::now();
            let result = guard.prune(now, retention, max_events, batch_size);
            let elapsed = started.elapsed().as_secs_f64();
            match &result {
                Ok(pruned) => {
                    metrics.observe_storage_op("prune", elapsed, true);
                    storage_health.note_success();
                    match guard.stats() {
                        Ok(stats) => metrics.observe_prune(
                            pruned.age_pruned,
                            pruned.overflow_pruned,
                            stats.rows,
                            stats.bytes,
                        ),
                        Err(_) => {
                            metrics.observe_prune(pruned.age_pruned, pruned.overflow_pruned, 0, 0)
                        }
                    }
                }
                Err(err) => {
                    metrics.observe_storage_op("prune", elapsed, false);
                    storage_health.note_error(&err.to_string());
                }
            }
            let _ = reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::InvolvedObject;
    use crate::events::storage_health::{StorageBackend, StorageHealthHandle};
    use crate::metrics::Metrics;

    fn sample_event(uid: &str, rv: &str) -> ClusterEvent {
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

    fn checkpoint(scope: &str, rv: &str) -> WatchCheckpoint {
        WatchCheckpoint {
            cluster_id: "default".to_owned(),
            scope: scope.to_owned(),
            resource_version: rv.to_owned(),
            updated_at: Utc::now(),
        }
    }

    fn spawn_test_writer(
        store: Arc<Mutex<SqliteEventStore>>,
        capacity: usize,
        cancel: CancellationToken,
    ) -> (
        DurableWriterHandle,
        tokio::task::JoinHandle<()>,
        Metrics,
        StorageHealthHandle,
    ) {
        let metrics = Metrics::try_new().expect("metrics");
        let health = StorageHealthHandle::new(StorageBackend::Memory);
        health.mark_ready();
        let (writer, join) =
            spawn_durable_writer(store, capacity, cancel, metrics.clone(), health.clone());
        (writer, join, metrics, health)
    }

    #[tokio::test]
    async fn write_before_ack_commits_then_readable() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let (writer, join, metrics, health) = spawn_test_writer(store, 8, cancel.child_token());

        let outcome = writer
            .upsert_with_checkpoint(
                "default",
                sample_event("a", "10"),
                Some(checkpoint("all", "10")),
            )
            .await
            .expect("upsert");
        assert_eq!(outcome, UpsertOutcome::Inserted);
        assert!(health.status().is_ready());
        let text = metrics.gather_text().expect("metrics");
        assert!(text.contains("clustersentinel_storage_writes_total{result=\"ok\"}"));

        let cp = writer
            .load_checkpoint("default", "all")
            .await
            .expect("load")
            .expect("present");
        assert_eq!(cp.resource_version, "10");

        cancel.cancel();
        let _ = join.await;
    }

    #[tokio::test]
    async fn storage_failure_surfaces_without_commit() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let (writer, join, metrics, health) = spawn_test_writer(store, 4, cancel.child_token());

        let bad = WatchCheckpoint {
            cluster_id: "default".to_owned(),
            scope: String::new(),
            resource_version: "1".to_owned(),
            updated_at: Utc::now(),
        };
        let err = writer
            .upsert_with_checkpoint("default", sample_event("x", "1"), Some(bad))
            .await
            .expect_err("must fail");
        assert!(matches!(err, WriterError::Store(_)));
        assert!(!health.status().is_ready());
        let text = metrics.gather_text().expect("metrics");
        assert!(text.contains("clustersentinel_storage_writes_total{result=\"error\"}"));
        assert!(text.contains("clustersentinel_storage_errors_total{operation=\"upsert\"}"));

        let recent = writer.load_recent("default", 10).await.expect("recent");
        assert!(recent.iter().all(|e| e.uid != "x"));

        cancel.cancel();
        let _ = join.await;
    }

    #[tokio::test]
    async fn retry_after_failure_succeeds() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let (writer, join, _metrics, health) = spawn_test_writer(store, 4, cancel.child_token());

        let bad = WatchCheckpoint {
            cluster_id: "default".to_owned(),
            scope: String::new(),
            resource_version: "1".to_owned(),
            updated_at: Utc::now(),
        };
        assert!(
            writer
                .upsert_with_checkpoint("default", sample_event("r", "1"), Some(bad))
                .await
                .is_err()
        );
        assert!(!health.status().is_ready());

        let ok = writer
            .upsert_with_checkpoint(
                "default",
                sample_event("r", "1"),
                Some(checkpoint("dev", "1")),
            )
            .await
            .expect("retry");
        assert_eq!(ok, UpsertOutcome::Inserted);
        assert!(health.status().is_ready());

        cancel.cancel();
        let _ = join.await;
    }

    #[tokio::test]
    async fn duplicate_watch_event_is_deduped() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let (writer, join, metrics, _health) = spawn_test_writer(store, 4, cancel.child_token());

        let event = sample_event("dup", "5");
        let cp = checkpoint("all", "5");
        assert_eq!(
            writer
                .upsert_with_checkpoint("default", event.clone(), Some(cp.clone()))
                .await
                .unwrap(),
            UpsertOutcome::Inserted
        );
        assert_eq!(
            writer
                .upsert_with_checkpoint("default", event, Some(cp))
                .await
                .unwrap(),
            UpsertOutcome::Deduped
        );
        let text = metrics.gather_text().expect("metrics");
        assert!(text.contains("clustersentinel_storage_writes_total{result=\"deduped\"}"));

        cancel.cancel();
        let _ = join.await;
    }

    #[tokio::test]
    async fn scope_isolation_keeps_independent_checkpoints() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let (writer, join, _metrics, _health) = spawn_test_writer(store, 4, cancel.child_token());

        writer
            .upsert_with_checkpoint(
                "default",
                sample_event("ns-a", "11"),
                Some(checkpoint("alpha", "11")),
            )
            .await
            .unwrap();
        writer
            .upsert_with_checkpoint(
                "default",
                sample_event("ns-b", "22"),
                Some(checkpoint("beta", "22")),
            )
            .await
            .unwrap();

        assert_eq!(
            writer
                .load_checkpoint("default", "alpha")
                .await
                .unwrap()
                .unwrap()
                .resource_version,
            "11"
        );
        assert_eq!(
            writer
                .load_checkpoint("default", "beta")
                .await
                .unwrap()
                .unwrap()
                .resource_version,
            "22"
        );

        writer.clear_checkpoint("default", "alpha").await.unwrap();
        assert!(
            writer
                .load_checkpoint("default", "alpha")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            writer
                .load_checkpoint("default", "beta")
                .await
                .unwrap()
                .is_some()
        );

        cancel.cancel();
        let _ = join.await;
    }

    #[tokio::test]
    async fn cancellation_stops_writer_and_rejects_new_writes() {
        let store = Arc::new(Mutex::new(
            SqliteEventStore::open_in_memory().expect("store"),
        ));
        let cancel = CancellationToken::new();
        let (writer, join, _metrics, _health) = spawn_test_writer(store, 2, cancel.child_token());

        cancel.cancel();
        let _ = join.await;

        let err = writer
            .upsert_with_checkpoint(
                "default",
                sample_event("late", "1"),
                Some(checkpoint("all", "1")),
            )
            .await
            .expect_err("closed");
        assert!(matches!(err, WriterError::Closed));
    }
}

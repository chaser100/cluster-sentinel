//! Cluster event models, registry, and watchers.

mod cursor;
mod model;
mod registry;
mod storage_health;
mod store;
mod watcher;
mod writer;

pub use cursor::{
    ParsedCursor, encode_keyset_cursor, encode_keyset_from_event, is_after_keyset, parse_cursor,
};
pub use model::{ClusterEvent, InvolvedObject};
pub use registry::{
    EventQuery, EventRegistry, EventSearchQuery, EventSearchResult, EventSummaryGroup,
    EventSummaryKey, SummaryGroupBy, UpsertOutcome,
};
pub use storage_health::{
    StorageBackend, StorageHealthHandle, StorageHealthSnapshot, StorageStatus,
};
pub use store::{
    DEFAULT_MAX_EVENTS, DEFAULT_RETENTION, EventStore, EventStoreHandle, ObservedBounds,
    PruneResult, SqliteEventStore, StorageStats, StoreError, WatchCheckpoint,
};
pub use watcher::{
    SCOPE_ALL, WatchResumeAction, WatchRuntimeStatus, WatchScope, WatchState, WatchStateHandle,
    classify_watch_error, next_backoff, persist_then_cache, run_event_pipeline,
};
pub use writer::{DurableWriterHandle, WriterError, spawn_durable_writer};

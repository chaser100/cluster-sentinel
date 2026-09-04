//! Cluster event models, registry, and watchers.

mod model;
mod registry;
mod watcher;

pub use model::{ClusterEvent, InvolvedObject};
pub use registry::{
    EventQuery, EventRegistry, EventSearchQuery, EventSearchResult, EventSummaryGroup,
    EventSummaryKey, SummaryGroupBy, UpsertOutcome,
};
pub use watcher::{
    WatchResumeAction, WatchRuntimeStatus, WatchState, WatchStateHandle, classify_watch_error,
    next_backoff, run_event_pipeline,
};

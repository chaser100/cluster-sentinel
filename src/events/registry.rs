//! In-memory event registry with UID dedup, capacity, and TTL eviction.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use super::cursor::paginate_events;
use super::model::ClusterEvent;

/// Filter for listing registered events.
#[derive(Debug, Clone, Default)]
pub struct EventQuery {
    pub limit: usize,
    pub namespace: Option<String>,
    pub reason: Option<String>,
    pub type_filter: Option<String>,
}

/// Rich filter for MCP `search_events`.
#[derive(Debug, Clone, Default)]
pub struct EventSearchQuery {
    pub limit: usize,
    /// Deprecated offset into the filtered set. Ignored when [`Self::after`] is set.
    pub offset: usize,
    /// Keyset resume point `(observed_at, event_uid)` exclusive in DESC order.
    pub after: Option<(DateTime<Utc>, String)>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub namespaces: Vec<String>,
    pub types: Vec<String>,
    pub reasons: Vec<String>,
    pub involved_kind: Option<String>,
    pub involved_name: Option<String>,
    pub involved_uid: Option<String>,
    pub source_component: Option<String>,
    pub message_contains: Option<String>,
}

/// Paginated search result.
#[derive(Debug, Clone)]
pub struct EventSearchResult {
    pub events: Vec<ClusterEvent>,
    pub matched: usize,
    pub returned: usize,
    pub truncated: bool,
    /// Deprecated absolute offset for clients still using decimal cursors.
    pub next_offset: Option<usize>,
    /// Next keyset resume point when more rows remain.
    pub next_after: Option<(DateTime<Utc>, String)>,
}

/// Aggregation key for `summarize_events`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventSummaryKey {
    pub namespace: Option<String>,
    pub reason: Option<String>,
    pub involved_kind: Option<String>,
    pub event_type: Option<String>,
}

/// One aggregated group.
#[derive(Debug, Clone)]
pub struct EventSummaryGroup {
    pub key: EventSummaryKey,
    pub event_objects: usize,
    pub occurrences: i64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub affected_objects: usize,
    pub sample_message: String,
}

/// Fields that can be used in `group_by`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SummaryGroupBy {
    Namespace,
    Reason,
    InvolvedKind,
    Type,
}

/// Outcome of inserting or updating an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertOutcome {
    Inserted,
    Updated,
    Deduped,
}

#[derive(Debug)]
struct RegistryInner {
    by_uid: HashMap<String, ClusterEvent>,
    order: VecDeque<String>,
    capacity: usize,
    dedup_ttl: Duration,
}

impl RegistryInner {
    fn new(capacity: usize, dedup_ttl: Duration) -> Self {
        Self {
            by_uid: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
            dedup_ttl,
        }
    }

    fn upsert(&mut self, mut event: ClusterEvent, now: DateTime<Utc>) -> UpsertOutcome {
        self.evict_expired(now);

        if let Some(existing) = self.by_uid.get(&event.uid) {
            if existing.resource_version == event.resource_version
                && existing.count == event.count
                && existing.message == event.message
            {
                return UpsertOutcome::Deduped;
            }

            // Preserve first registration time for TTL; refresh recency via order.
            event.registered_at = existing.registered_at;
            let uid = event.uid.clone();
            self.by_uid.insert(uid.clone(), event);
            self.touch_order(&uid);
            return UpsertOutcome::Updated;
        }

        while self.by_uid.len() >= self.capacity {
            if let Some(old_uid) = self.order.pop_front() {
                self.by_uid.remove(&old_uid);
            } else {
                break;
            }
        }

        self.order.push_back(event.uid.clone());
        self.by_uid.insert(event.uid.clone(), event);
        UpsertOutcome::Inserted
    }

    fn touch_order(&mut self, uid: &str) {
        self.order.retain(|existing| existing != uid);
        self.order.push_back(uid.to_owned());
    }

    fn evict_expired(&mut self, now: DateTime<Utc>) {
        let ttl = chrono::Duration::from_std(self.dedup_ttl)
            .unwrap_or_else(|_| chrono::Duration::seconds(3600));
        let expired: Vec<String> = self
            .by_uid
            .iter()
            .filter(|(_, event)| now.signed_duration_since(event.registered_at) > ttl)
            .map(|(uid, _)| uid.clone())
            .collect();
        for uid in expired {
            self.by_uid.remove(&uid);
            self.order.retain(|existing| existing != &uid);
        }
    }

    fn list(&self, query: &EventQuery) -> Vec<ClusterEvent> {
        let limit = query.limit.clamp(1, 500);
        let mut events: Vec<ClusterEvent> = self
            .by_uid
            .values()
            .filter(|event| match &query.namespace {
                Some(ns) => event.namespace == *ns,
                None => true,
            })
            .filter(|event| match &query.reason {
                Some(reason) => event.reason == *reason,
                None => true,
            })
            .filter(|event| match &query.type_filter {
                Some(type_filter) => event.event_type == *type_filter,
                None => true,
            })
            .cloned()
            .collect();
        events.sort_by_key(|event| std::cmp::Reverse(event.observed_at()));
        events.truncate(limit);
        events
    }

    fn search(&self, query: &EventSearchQuery) -> EventSearchResult {
        let limit = query.limit.clamp(1, 500);
        let message_needle = query
            .message_contains
            .as_ref()
            .map(|value| value.to_ascii_lowercase());

        let mut matched: Vec<ClusterEvent> = self
            .by_uid
            .values()
            .filter(|event| self.matches_search(event, query, message_needle.as_deref()))
            .cloned()
            .collect();
        matched.sort_by(|left, right| {
            right
                .observed_at()
                .cmp(&left.observed_at())
                .then_with(|| right.uid.cmp(&left.uid))
        });

        paginate_events(matched, query.offset, query.after.as_ref(), limit)
    }

    fn matches_search(
        &self,
        event: &ClusterEvent,
        query: &EventSearchQuery,
        message_needle: Option<&str>,
    ) -> bool {
        let observed = event.observed_at();
        if query.since.is_some_and(|since| observed < since) {
            return false;
        }
        if query.until.is_some_and(|until| observed > until) {
            return false;
        }
        if !query.namespaces.is_empty() && !query.namespaces.iter().any(|ns| ns == &event.namespace)
        {
            return false;
        }
        if !query.types.is_empty() && !query.types.iter().any(|ty| ty == &event.event_type) {
            return false;
        }
        if !query.reasons.is_empty() && !query.reasons.iter().any(|reason| reason == &event.reason)
        {
            return false;
        }
        if query
            .involved_kind
            .as_ref()
            .is_some_and(|kind| kind != &event.involved_object.kind)
        {
            return false;
        }
        if query
            .involved_name
            .as_ref()
            .is_some_and(|name| !event.involved_object.name.contains(name.as_str()))
        {
            return false;
        }
        if let Some(uid) = &query.involved_uid {
            match &event.involved_object.uid {
                Some(event_uid) if event_uid == uid => {}
                _ => return false,
            }
        }
        if query
            .source_component
            .as_ref()
            .is_some_and(|source| event.source_component.as_ref() != Some(source))
        {
            return false;
        }
        if let Some(needle) = message_needle
            && !event.message.to_ascii_lowercase().contains(needle)
        {
            return false;
        }
        true
    }

    fn summarize(
        &self,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        type_filter: Option<&str>,
        group_by: &[SummaryGroupBy],
        limit: usize,
    ) -> Vec<EventSummaryGroup> {
        let limit = limit.clamp(1, 200);
        let mut groups: HashMap<EventSummaryKey, AggregateScratch> = HashMap::new();

        for event in self.by_uid.values() {
            let observed = event.observed_at();
            if since.is_some_and(|bound| observed < bound) {
                continue;
            }
            if until.is_some_and(|bound| observed > bound) {
                continue;
            }
            if type_filter.is_some_and(|ty| ty != event.event_type) {
                continue;
            }

            let key = EventSummaryKey {
                namespace: group_by
                    .contains(&SummaryGroupBy::Namespace)
                    .then(|| event.namespace.clone()),
                reason: group_by
                    .contains(&SummaryGroupBy::Reason)
                    .then(|| event.reason.clone()),
                involved_kind: group_by
                    .contains(&SummaryGroupBy::InvolvedKind)
                    .then(|| event.involved_object.kind.clone()),
                event_type: group_by
                    .contains(&SummaryGroupBy::Type)
                    .then(|| event.event_type.clone()),
            };

            let object_key = format!(
                "{}/{}/{}",
                event.involved_object.kind,
                event.involved_object.namespace,
                event.involved_object.name
            );
            let scratch = groups.entry(key).or_insert_with(|| AggregateScratch {
                event_objects: 0,
                occurrences: 0,
                first_seen: observed,
                last_seen: observed,
                affected: HashSet::new(),
                sample_message: event.message.clone(),
                sample_at: observed,
            });
            scratch.event_objects = scratch.event_objects.saturating_add(1);
            scratch.occurrences = scratch
                .occurrences
                .saturating_add(i64::from(event.count.max(0)));
            if observed < scratch.first_seen {
                scratch.first_seen = observed;
            }
            if observed > scratch.last_seen {
                scratch.last_seen = observed;
            }
            if observed >= scratch.sample_at {
                scratch.sample_at = observed;
                scratch.sample_message = event.message.clone();
            }
            scratch.affected.insert(object_key);
        }

        let mut out: Vec<EventSummaryGroup> = groups
            .into_iter()
            .map(|(key, scratch)| EventSummaryGroup {
                key,
                event_objects: scratch.event_objects,
                occurrences: scratch.occurrences,
                first_seen: scratch.first_seen,
                last_seen: scratch.last_seen,
                affected_objects: scratch.affected.len(),
                sample_message: scratch.sample_message,
            })
            .collect();
        out.sort_by(|left, right| {
            right
                .occurrences
                .cmp(&left.occurrences)
                .then_with(|| right.last_seen.cmp(&left.last_seen))
        });
        out.truncate(limit);
        out
    }

    fn oldest_newest(&self) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
        let mut oldest = None;
        let mut newest = None;
        for event in self.by_uid.values() {
            let observed = event.observed_at();
            oldest = Some(oldest.map_or(observed, |cur: DateTime<Utc>| cur.min(observed)));
            newest = Some(newest.map_or(observed, |cur: DateTime<Utc>| cur.max(observed)));
        }
        (oldest, newest)
    }
}

#[derive(Debug)]
struct AggregateScratch {
    event_objects: usize,
    occurrences: i64,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    affected: HashSet<String>,
    sample_message: String,
    sample_at: DateTime<Utc>,
}

/// Shared event registry.
#[derive(Clone, Debug)]
pub struct EventRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    capacity: usize,
    dedup_ttl: Duration,
}

impl EventRegistry {
    /// Create a registry with max entries and TTL for eviction.
    #[must_use]
    pub fn new(capacity: usize, dedup_ttl: Duration) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner: Arc::new(RwLock::new(RegistryInner::new(capacity, dedup_ttl))),
            capacity,
            dedup_ttl,
        }
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[must_use]
    pub fn retention(&self) -> Duration {
        self.dedup_ttl
    }

    /// Insert or update an event.
    pub async fn upsert(&self, event: ClusterEvent) -> UpsertOutcome {
        let now = Utc::now();
        let mut guard = self.inner.write().await;
        guard.upsert(event, now)
    }

    /// Fetch a single event by UID.
    pub async fn get(&self, uid: &str) -> Option<ClusterEvent> {
        let now = Utc::now();
        let mut guard = self.inner.write().await;
        guard.evict_expired(now);
        guard.by_uid.get(uid).cloned()
    }

    /// List events matching the query (newest `observed_at` first).
    pub async fn list(&self, query: EventQuery) -> Vec<ClusterEvent> {
        let now = Utc::now();
        let mut guard = self.inner.write().await;
        guard.evict_expired(now);
        guard.list(&query)
    }

    /// Search with rich filters and offset pagination.
    pub async fn search(&self, query: EventSearchQuery) -> EventSearchResult {
        let now = Utc::now();
        let mut guard = self.inner.write().await;
        guard.evict_expired(now);
        guard.search(&query)
    }

    /// Aggregate matching events.
    pub async fn summarize(
        &self,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        type_filter: Option<&str>,
        group_by: &[SummaryGroupBy],
        limit: usize,
    ) -> Vec<EventSummaryGroup> {
        let now = Utc::now();
        let mut guard = self.inner.write().await;
        guard.evict_expired(now);
        guard.summarize(since, until, type_filter, group_by, limit)
    }

    /// Current number of registered events.
    pub async fn len(&self) -> usize {
        let now = Utc::now();
        let mut guard = self.inner.write().await;
        guard.evict_expired(now);
        guard.by_uid.len()
    }

    /// Oldest/newest `observed_at` among retained events.
    pub async fn observed_bounds(&self) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
        let now = Utc::now();
        let mut guard = self.inner.write().await;
        guard.evict_expired(now);
        guard.oldest_newest()
    }

    /// Whether the registry is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::model::InvolvedObject;

    fn sample(uid: &str, rv: &str, count: i32) -> ClusterEvent {
        ClusterEvent {
            uid: uid.to_owned(),
            namespace: "dev".to_owned(),
            name: format!("evt-{uid}"),
            resource_version: rv.to_owned(),
            event_type: "Warning".to_owned(),
            reason: "BackOff".to_owned(),
            message: "msg".to_owned(),
            count,
            involved_object: InvolvedObject {
                kind: "Pod".to_owned(),
                namespace: "dev".to_owned(),
                name: "web".to_owned(),
                uid: Some("pod".to_owned()),
                api_version: Some("v1".to_owned()),
            },
            source_component: Some("kubelet".to_owned()),
            first_timestamp: None,
            last_timestamp: None,
            event_time: None,
            registered_at: Utc::now(),
        }
    }

    fn sample_at(
        uid: &str,
        rv: &str,
        count: i32,
        last: DateTime<Utc>,
        registered_at: DateTime<Utc>,
    ) -> ClusterEvent {
        let mut event = sample(uid, rv, count);
        event.last_timestamp = Some(last);
        event.registered_at = registered_at;
        event
    }

    #[tokio::test]
    async fn dedups_identical_revisions() {
        let registry = EventRegistry::new(10, Duration::from_secs(3600));
        assert_eq!(
            registry.upsert(sample("a", "1", 1)).await,
            UpsertOutcome::Inserted
        );
        assert_eq!(
            registry.upsert(sample("a", "1", 1)).await,
            UpsertOutcome::Deduped
        );
        assert_eq!(
            registry.upsert(sample("a", "2", 2)).await,
            UpsertOutcome::Updated
        );
        assert_eq!(registry.len().await, 1);
    }

    #[tokio::test]
    async fn respects_capacity() {
        let registry = EventRegistry::new(2, Duration::from_secs(3600));
        registry.upsert(sample("1", "1", 1)).await;
        registry.upsert(sample("2", "1", 1)).await;
        registry.upsert(sample("3", "1", 1)).await;
        assert_eq!(registry.len().await, 2);
        assert!(registry.get("1").await.is_none());
        assert!(registry.get("3").await.is_some());
    }

    #[tokio::test]
    async fn update_moves_event_to_newest() {
        let registry = EventRegistry::new(10, Duration::from_secs(3600));
        let t0 = Utc::now() - chrono::Duration::minutes(10);
        let t1 = Utc::now();
        registry.upsert(sample_at("old", "1", 1, t0, t0)).await;
        registry.upsert(sample_at("mid", "1", 1, t0, t0)).await;
        let mut updated = sample_at("old", "2", 2, t1, t0);
        updated.message = "updated".into();
        registry.upsert(updated).await;

        let listed = registry
            .list(EventQuery {
                limit: 10,
                ..EventQuery::default()
            })
            .await;
        assert_eq!(listed[0].uid, "old");
        assert_eq!(listed[0].observed_at(), t1);
    }

    #[tokio::test]
    async fn ttl_evicts_on_list_without_upsert() {
        let registry = EventRegistry::new(10, Duration::from_millis(20));
        let past = Utc::now() - chrono::Duration::seconds(10);
        registry
            .upsert(sample_at("stale", "1", 1, past, past))
            .await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            registry
                .list(EventQuery {
                    limit: 10,
                    ..EventQuery::default()
                })
                .await
                .is_empty()
        );
        assert_eq!(registry.len().await, 0);
    }

    #[tokio::test]
    async fn updated_old_uid_does_not_block_ttl_eviction() {
        let registry = EventRegistry::new(10, Duration::from_millis(100));
        registry.upsert(sample("old", "1", 1)).await;
        tokio::time::sleep(Duration::from_millis(70)).await;
        registry.upsert(sample("new", "1", 1)).await;

        let mut updated = sample("old", "2", 2);
        updated.message = "updated".into();
        assert_eq!(registry.upsert(updated).await, UpsertOutcome::Updated);

        tokio::time::sleep(Duration::from_millis(50)).await;
        let listed = registry
            .list(EventQuery {
                limit: 10,
                ..EventQuery::default()
            })
            .await;
        assert_eq!(
            listed
                .iter()
                .map(|event| event.uid.as_str())
                .collect::<Vec<_>>(),
            vec!["new"]
        );
    }

    #[tokio::test]
    async fn search_filters_and_paginates() {
        let registry = EventRegistry::new(10, Duration::from_secs(3600));
        let t0 = Utc::now() - chrono::Duration::minutes(5);
        let t1 = Utc::now();
        registry.upsert(sample_at("a", "1", 1, t0, t0)).await;
        let mut warning = sample_at("b", "1", 3, t1, t1);
        warning.message = "connection refused".into();
        warning.reason = "FailedMount".into();
        registry.upsert(warning).await;

        let result = registry
            .search(EventSearchQuery {
                limit: 1,
                offset: 0,
                reasons: vec!["FailedMount".into()],
                message_contains: Some("refused".into()),
                ..EventSearchQuery::default()
            })
            .await;
        assert_eq!(result.matched, 1);
        assert_eq!(result.returned, 1);
        assert!(!result.truncated);
        assert_eq!(result.events[0].uid, "b");
    }

    #[tokio::test]
    async fn search_applies_all_filters_with_inclusive_time_bounds() {
        let registry = EventRegistry::new(10, Duration::from_secs(3600));
        let observed = Utc::now();
        let mut target = sample_at("target", "1", 2, observed, observed);
        target.namespace = "prod".into();
        target.reason = "FailedMount".into();
        target.message = "Connection REFUSED by storage".into();
        target.involved_object.kind = "Pod".into();
        target.involved_object.name = "api-7b84c8-worker".into();
        target.involved_object.uid = Some("pod-target".into());
        target.source_component = Some("kubelet".into());
        registry.upsert(target).await;

        let mut distractor = sample_at(
            "distractor",
            "1",
            1,
            observed - chrono::Duration::seconds(1),
            observed,
        );
        distractor.namespace = "dev".into();
        registry.upsert(distractor).await;

        let result = registry
            .search(EventSearchQuery {
                limit: 10,
                since: Some(observed),
                until: Some(observed),
                namespaces: vec!["prod".into()],
                types: vec!["Warning".into()],
                reasons: vec!["FailedMount".into()],
                involved_kind: Some("Pod".into()),
                involved_name: Some("7b84c8".into()),
                involved_uid: Some("pod-target".into()),
                source_component: Some("kubelet".into()),
                message_contains: Some("connection refused".into()),
                ..EventSearchQuery::default()
            })
            .await;

        assert_eq!(result.matched, 1);
        assert_eq!(result.events[0].uid, "target");
    }

    #[tokio::test]
    async fn cursor_pages_do_not_skip_or_duplicate_events() {
        let registry = EventRegistry::new(10, Duration::from_secs(3600));
        let now = Utc::now();
        for (index, uid) in ["old", "middle", "new"].into_iter().enumerate() {
            let observed = now + chrono::Duration::seconds(index as i64);
            registry.upsert(sample_at(uid, "1", 1, observed, now)).await;
        }

        let first = registry
            .search(EventSearchQuery {
                limit: 2,
                ..EventSearchQuery::default()
            })
            .await;
        let second = registry
            .search(EventSearchQuery {
                limit: 2,
                offset: first.next_offset.expect("next offset"),
                ..EventSearchQuery::default()
            })
            .await;

        assert_eq!(first.matched, 3);
        assert_eq!(first.returned, 2);
        assert!(first.truncated);
        assert_eq!(second.returned, 1);
        assert!(!second.truncated);
        let mut uids = first
            .events
            .iter()
            .chain(&second.events)
            .map(|event| event.uid.as_str())
            .collect::<Vec<_>>();
        assert_eq!(uids, vec!["new", "middle", "old"]);
        uids.sort_unstable();
        uids.dedup();
        assert_eq!(uids.len(), 3);
    }

    #[tokio::test]
    async fn summary_separates_event_objects_from_occurrences() {
        let registry = EventRegistry::new(10, Duration::from_secs(3600));
        let old = Utc::now() - chrono::Duration::minutes(1);
        let new = Utc::now();
        let mut first = sample_at("first", "1", 2, old, old);
        first.message = "older".into();
        registry.upsert(first).await;
        let mut second = sample_at("second", "1", 3, new, new);
        second.message = "newer".into();
        registry.upsert(second).await;

        let groups = registry
            .summarize(
                Some(old),
                Some(new),
                Some("Warning"),
                &[
                    SummaryGroupBy::Namespace,
                    SummaryGroupBy::Reason,
                    SummaryGroupBy::InvolvedKind,
                ],
                20,
            )
            .await;

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].event_objects, 2);
        assert_eq!(groups[0].occurrences, 5);
        assert_eq!(groups[0].affected_objects, 1);
        assert_eq!(groups[0].first_seen, old);
        assert_eq!(groups[0].last_seen, new);
        assert_eq!(groups[0].sample_message, "newer");
    }
}

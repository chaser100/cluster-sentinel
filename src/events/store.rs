//! Durable event persistence contract and SQLite implementation.
use super::{
    ClusterEvent, EventQuery, EventSearchQuery, EventSearchResult, EventSummaryGroup,
    EventSummaryKey, SummaryGroupBy, UpsertOutcome,
};
use chrono::{DateTime, Utc};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;

pub const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_MAX_EVENTS: usize = 250_000;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    #[error("sqlite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("event payload serialization failed")]
    Payload(#[from] serde_json::Error),
    #[error("invalid persisted timestamp: {0}")]
    Timestamp(String),
    #[error("numeric value exceeds supported range")]
    NumericRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchCheckpoint {
    pub cluster_id: String,
    pub scope: String,
    pub resource_version: String,
    pub updated_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneResult {
    pub age_pruned: usize,
    pub overflow_pruned: usize,
}
impl PruneResult {
    #[must_use]
    pub fn total(self) -> usize {
        self.age_pruned.saturating_add(self.overflow_pruned)
    }
}

/// Lightweight durable store size snapshot for gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageStats {
    pub rows: usize,
    pub bytes: u64,
}

pub type ObservedBounds = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

pub trait EventStore: Send {
    fn upsert_with_checkpoint(
        &mut self,
        cluster_id: &str,
        event: &ClusterEvent,
        checkpoint: Option<&WatchCheckpoint>,
    ) -> Result<UpsertOutcome, StoreError>;
    fn get(&self, cluster_id: &str, uid: &str) -> Result<Option<ClusterEvent>, StoreError>;
    fn search(
        &self,
        cluster_id: &str,
        query: &EventSearchQuery,
    ) -> Result<EventSearchResult, StoreError>;
    fn summarize(
        &self,
        cluster_id: &str,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        event_type: Option<&str>,
        group_by: &[SummaryGroupBy],
        limit: usize,
    ) -> Result<Vec<EventSummaryGroup>, StoreError>;
    fn load_recent(&self, cluster_id: &str, limit: usize) -> Result<Vec<ClusterEvent>, StoreError>;
    fn load_checkpoint(
        &self,
        cluster_id: &str,
        scope: &str,
    ) -> Result<Option<WatchCheckpoint>, StoreError>;
    fn save_checkpoint(&mut self, checkpoint: &WatchCheckpoint) -> Result<(), StoreError>;
    fn clear_checkpoint(&mut self, cluster_id: &str, scope: &str) -> Result<(), StoreError>;
    fn prune(
        &mut self,
        now: DateTime<Utc>,
        retention: Duration,
        max_events: usize,
        batch_size: usize,
    ) -> Result<PruneResult, StoreError>;
    fn stats(&self) -> Result<StorageStats, StoreError>;
    fn count(&self, cluster_id: &str) -> Result<usize, StoreError>;
    fn observed_bounds(&self, cluster_id: &str) -> Result<ObservedBounds, StoreError>;
    fn list_checkpoints(&self, cluster_id: &str) -> Result<Vec<WatchCheckpoint>, StoreError>;
}

#[derive(Debug)]
pub struct SqliteEventStore {
    connection: Connection,
}
impl SqliteEventStore {
    /// Open, configure, and migrate a file-backed SQLite store.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::from_connection(Connection::open(path)?)
    }
    /// Open, configure, and migrate an in-memory SQLite store.
    pub fn open_in_memory() -> Result<Self, StoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }
    fn from_connection(connection: Connection) -> Result<Self, StoreError> {
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;",
        )?;
        connection.execute_batch("BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS events(cluster_id TEXT NOT NULL,event_uid TEXT NOT NULL,namespace TEXT NOT NULL,name TEXT NOT NULL,resource_version TEXT NOT NULL,event_type TEXT NOT NULL,reason TEXT NOT NULL,message TEXT NOT NULL,count INTEGER NOT NULL,involved_kind TEXT NOT NULL,involved_namespace TEXT NOT NULL,involved_name TEXT NOT NULL,involved_uid TEXT,involved_api_version TEXT,source_component TEXT,first_timestamp TEXT,last_timestamp TEXT,event_time TEXT,registered_at TEXT NOT NULL,updated_at TEXT NOT NULL,observed_at TEXT NOT NULL,payload_version INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(cluster_id,event_uid));
CREATE TABLE IF NOT EXISTS watch_checkpoints(cluster_id TEXT NOT NULL,scope TEXT NOT NULL CHECK(scope<>''),resource_version TEXT NOT NULL CHECK(resource_version<>''),updated_at TEXT NOT NULL,PRIMARY KEY(cluster_id,scope));
CREATE INDEX IF NOT EXISTS events_observed_idx ON events(observed_at DESC,event_uid DESC); CREATE INDEX IF NOT EXISTS events_namespace_idx ON events(namespace,observed_at DESC); CREATE INDEX IF NOT EXISTS events_reason_idx ON events(reason,observed_at DESC); CREATE INDEX IF NOT EXISTS events_type_idx ON events(event_type,observed_at DESC); CREATE INDEX IF NOT EXISTS events_involved_uid_idx ON events(involved_uid);
CREATE INDEX IF NOT EXISTS events_cluster_observed_idx ON events(cluster_id,observed_at DESC,event_uid DESC); CREATE INDEX IF NOT EXISTS events_cluster_namespace_idx ON events(cluster_id,namespace,observed_at DESC); CREATE INDEX IF NOT EXISTS events_cluster_reason_idx ON events(cluster_id,reason,observed_at DESC); CREATE INDEX IF NOT EXISTS events_cluster_type_idx ON events(cluster_id,event_type,observed_at DESC); CREATE INDEX IF NOT EXISTS events_cluster_involved_uid_idx ON events(cluster_id,involved_uid); PRAGMA user_version=1; COMMIT;")?;
        Ok(Self { connection })
    }
}

impl EventStore for SqliteEventStore {
    fn upsert_with_checkpoint(
        &mut self,
        cluster_id: &str,
        event: &ClusterEvent,
        checkpoint: Option<&WatchCheckpoint>,
    ) -> Result<UpsertOutcome, StoreError> {
        let tx = self.connection.transaction()?;
        let old: Option<(String,i64,String)>=tx.query_row("SELECT resource_version,count,message FROM events WHERE cluster_id=?1 AND event_uid=?2",params![cluster_id,event.uid],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let outcome = match old {
            None => UpsertOutcome::Inserted,
            Some((rv, count, msg))
                if rv == event.resource_version
                    && count == i64::from(event.count)
                    && msg == event.message =>
            {
                UpsertOutcome::Deduped
            }
            Some(_) => UpsertOutcome::Updated,
        };
        if outcome != UpsertOutcome::Deduped {
            write_event(&tx, cluster_id, event)?;
        }
        if let Some(c) = checkpoint {
            tx.execute("INSERT INTO watch_checkpoints VALUES(?1,?2,?3,?4) ON CONFLICT(cluster_id,scope) DO UPDATE SET resource_version=excluded.resource_version,updated_at=excluded.updated_at",params![c.cluster_id,c.scope,c.resource_version,ts(c.updated_at)])?;
        }
        tx.commit()?;
        Ok(outcome)
    }
    fn get(&self, cluster_id: &str, uid: &str) -> Result<Option<ClusterEvent>, StoreError> {
        Ok(self
            .connection
            .query_row(
                "SELECT payload FROM events WHERE cluster_id=?1 AND event_uid=?2",
                params![cluster_id, uid],
                decode,
            )
            .optional()?)
    }
    fn search(
        &self,
        cluster_id: &str,
        q: &EventSearchQuery,
    ) -> Result<EventSearchResult, StoreError> {
        let (where_clause, filter_params) = query_filter(cluster_id, q);
        let count_sql = format!("SELECT COUNT(*) FROM events WHERE {where_clause}");
        let matched: i64 = self.connection.query_row(
            &count_sql,
            params_from_iter(filter_params.iter()),
            |row| row.get(0),
        )?;
        let matched = usize::try_from(matched).map_err(|_| StoreError::NumericRange)?;

        let limit = q.limit.clamp(1, 500);
        let mut page_params = filter_params;
        let mut cursor_clause = String::new();
        if let Some((observed_at, event_uid)) = &q.after {
            cursor_clause.push_str(" AND (observed_at < ? OR (observed_at = ? AND event_uid < ?))");
            let observed_at = ts(*observed_at);
            page_params.push(Value::Text(observed_at.clone()));
            page_params.push(Value::Text(observed_at));
            page_params.push(Value::Text(event_uid.clone()));
        }
        page_params.push(Value::Integer(to_i64(limit.saturating_add(1))?));
        page_params.push(Value::Integer(to_i64(if q.after.is_some() {
            0
        } else {
            q.offset
        })?));

        let page_sql = format!(
            "SELECT payload FROM events WHERE {where_clause}{cursor_clause} ORDER BY observed_at DESC,event_uid DESC LIMIT ? OFFSET ?"
        );
        let mut statement = self.connection.prepare(&page_sql)?;
        let mut events = statement
            .query_map(params_from_iter(page_params.iter()), decode)?
            .collect::<Result<Vec<_>, _>>()?;
        let truncated = events.len() > limit;
        events.truncate(limit);
        let returned = events.len();
        let next_after = truncated
            .then(|| {
                events
                    .last()
                    .map(|event| (event.observed_at(), event.uid.clone()))
            })
            .flatten();
        let next_offset = if q.after.is_some() {
            None
        } else {
            truncated.then_some(q.offset.saturating_add(returned))
        };

        Ok(EventSearchResult {
            events,
            matched,
            returned,
            truncated,
            next_offset,
            next_after,
        })
    }
    fn summarize(
        &self,
        cluster_id: &str,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        event_type: Option<&str>,
        group_by: &[SummaryGroupBy],
        limit: usize,
    ) -> Result<Vec<EventSummaryGroup>, StoreError> {
        let q = EventSearchQuery {
            limit: 500,
            since,
            until,
            types: event_type.map_or_else(Vec::new, |v| vec![v.to_owned()]),
            ..Default::default()
        };
        let mut groups: HashMap<EventSummaryKey, Scratch> = HashMap::new();
        for event in self.filtered(cluster_id, &q)? {
            accumulate(&mut groups, event, group_by);
        }
        let mut out: Vec<_> = groups
            .into_iter()
            .map(|(key, v)| EventSummaryGroup {
                key,
                event_objects: v.objects,
                occurrences: v.occurrences,
                first_seen: v.first,
                last_seen: v.last,
                affected_objects: v.affected.len(),
                sample_message: v.sample,
            })
            .collect();
        out.sort_by(|a, b| {
            b.occurrences
                .cmp(&a.occurrences)
                .then_with(|| b.last_seen.cmp(&a.last_seen))
        });
        out.truncate(limit.clamp(1, 200));
        Ok(out)
    }
    fn load_recent(&self, cluster_id: &str, limit: usize) -> Result<Vec<ClusterEvent>, StoreError> {
        let mut s=self.connection.prepare("SELECT payload FROM events WHERE cluster_id=?1 ORDER BY observed_at DESC,event_uid DESC LIMIT ?2")?;
        Ok(
            s.query_map(params![cluster_id, to_i64(limit.clamp(1, 10_000))?], decode)?
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
    fn load_checkpoint(
        &self,
        cluster_id: &str,
        scope: &str,
    ) -> Result<Option<WatchCheckpoint>, StoreError> {
        let raw:Option<(String,String)>=self.connection.query_row("SELECT resource_version,updated_at FROM watch_checkpoints WHERE cluster_id=?1 AND scope=?2",params![cluster_id,scope],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        raw.map(|(rv, t)| {
            Ok(WatchCheckpoint {
                cluster_id: cluster_id.into(),
                scope: scope.into(),
                resource_version: rv,
                updated_at: parse_ts(t)?,
            })
        })
        .transpose()
    }
    fn save_checkpoint(&mut self, checkpoint: &WatchCheckpoint) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO watch_checkpoints VALUES(?1,?2,?3,?4) ON CONFLICT(cluster_id,scope) DO UPDATE SET resource_version=excluded.resource_version,updated_at=excluded.updated_at",
            params![
                checkpoint.cluster_id,
                checkpoint.scope,
                checkpoint.resource_version,
                ts(checkpoint.updated_at)
            ],
        )?;
        Ok(())
    }
    fn clear_checkpoint(&mut self, cluster_id: &str, scope: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "DELETE FROM watch_checkpoints WHERE cluster_id=?1 AND scope=?2",
            params![cluster_id, scope],
        )?;
        Ok(())
    }
    fn prune(
        &mut self,
        now: DateTime<Utc>,
        retention: Duration,
        max_events: usize,
        batch_size: usize,
    ) -> Result<PruneResult, StoreError> {
        let cutoff =
            now - chrono::Duration::from_std(retention).map_err(|_| StoreError::NumericRange)?;
        let tx = self.connection.transaction()?;
        let batch = batch_size.max(1);
        let age=tx.execute("DELETE FROM events WHERE rowid IN(SELECT rowid FROM events WHERE observed_at<?1 ORDER BY observed_at,event_uid LIMIT ?2)",params![ts(cutoff),to_i64(batch)?])?;
        let count: usize =
            usize::try_from(
                tx.query_row::<i64, _, _>("SELECT COUNT(*) FROM events", [], |r| r.get(0))?,
            )
            .map_err(|_| StoreError::NumericRange)?;
        let overflow = count
            .saturating_sub(max_events)
            .min(batch.saturating_sub(age));
        let removed=tx.execute("DELETE FROM events WHERE rowid IN(SELECT rowid FROM events ORDER BY observed_at,event_uid LIMIT ?1)",params![to_i64(overflow)?])?;
        tx.commit()?;
        if age + removed > 0 {
            self.connection
                .execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        }
        Ok(PruneResult {
            age_pruned: age,
            overflow_pruned: removed,
        })
    }
    fn stats(&self) -> Result<StorageStats, StoreError> {
        self.compute_stats()
    }
    fn count(&self, cluster_id: &str) -> Result<usize, StoreError> {
        let rows: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM events WHERE cluster_id=?1",
            params![cluster_id],
            |row| row.get(0),
        )?;
        usize::try_from(rows).map_err(|_| StoreError::NumericRange)
    }
    fn observed_bounds(&self, cluster_id: &str) -> Result<ObservedBounds, StoreError> {
        let raw: (Option<String>, Option<String>) = self.connection.query_row(
            "SELECT MIN(observed_at), MAX(observed_at) FROM events WHERE cluster_id=?1",
            params![cluster_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((
            raw.0.map(parse_ts).transpose()?,
            raw.1.map(parse_ts).transpose()?,
        ))
    }
    fn list_checkpoints(&self, cluster_id: &str) -> Result<Vec<WatchCheckpoint>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT scope, resource_version, updated_at FROM watch_checkpoints WHERE cluster_id=?1",
        )?;
        let rows = stmt.query_map(params![cluster_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (scope, resource_version, updated_at) = row?;
            out.push(WatchCheckpoint {
                cluster_id: cluster_id.to_owned(),
                scope,
                resource_version,
                updated_at: parse_ts(updated_at)?,
            });
        }
        Ok(out)
    }
}

impl SqliteEventStore {
    fn compute_stats(&self) -> Result<StorageStats, StoreError> {
        let rows: i64 = self
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))?;
        let page_count: i64 = self
            .connection
            .query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let page_size: i64 = self
            .connection
            .query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let bytes = u64::try_from(page_count.saturating_mul(page_size)).unwrap_or(u64::MAX);
        Ok(StorageStats {
            rows: usize::try_from(rows).map_err(|_| StoreError::NumericRange)?,
            bytes,
        })
    }

    fn filtered(
        &self,
        cluster_id: &str,
        q: &EventSearchQuery,
    ) -> Result<Vec<ClusterEvent>, StoreError> {
        let (where_clause, filter_params) = query_filter(cluster_id, q);
        let sql = format!(
            "SELECT payload FROM events WHERE {where_clause} ORDER BY observed_at DESC,event_uid DESC"
        );
        let mut statement = self.connection.prepare(&sql)?;
        Ok(statement
            .query_map(params_from_iter(filter_params.iter()), decode)?
            .collect::<Result<Vec<_>, _>>()?)
    }
}

fn query_filter(cluster_id: &str, query: &EventSearchQuery) -> (String, Vec<Value>) {
    let mut conditions = vec!["cluster_id = ?".to_owned()];
    let mut values = vec![Value::Text(cluster_id.to_owned())];

    if let Some(since) = query.since {
        conditions.push("observed_at >= ?".to_owned());
        values.push(Value::Text(ts(since)));
    }
    if let Some(until) = query.until {
        conditions.push("observed_at <= ?".to_owned());
        values.push(Value::Text(ts(until)));
    }
    push_list_filter("namespace", &query.namespaces, &mut conditions, &mut values);
    push_list_filter("event_type", &query.types, &mut conditions, &mut values);
    push_list_filter("reason", &query.reasons, &mut conditions, &mut values);
    push_exact_filter(
        "involved_kind",
        query.involved_kind.as_deref(),
        &mut conditions,
        &mut values,
    );
    if let Some(involved_name) = &query.involved_name {
        conditions.push("instr(involved_name, ?) > 0".to_owned());
        values.push(Value::Text(involved_name.clone()));
    }
    push_exact_filter(
        "involved_uid",
        query.involved_uid.as_deref(),
        &mut conditions,
        &mut values,
    );
    push_exact_filter(
        "source_component",
        query.source_component.as_deref(),
        &mut conditions,
        &mut values,
    );
    if let Some(message) = &query.message_contains {
        conditions.push("instr(lower(message), ?) > 0".to_owned());
        values.push(Value::Text(message.to_lowercase()));
    }

    (conditions.join(" AND "), values)
}

fn push_list_filter(
    column: &str,
    items: &[String],
    conditions: &mut Vec<String>,
    values: &mut Vec<Value>,
) {
    if items.is_empty() {
        return;
    }
    let placeholders = (0..items.len()).map(|_| "?").collect::<Vec<_>>().join(",");
    conditions.push(format!("{column} IN ({placeholders})"));
    values.extend(items.iter().cloned().map(Value::Text));
}

fn push_exact_filter(
    column: &str,
    value: Option<&str>,
    conditions: &mut Vec<String>,
    values: &mut Vec<Value>,
) {
    if let Some(value) = value {
        conditions.push(format!("{column} = ?"));
        values.push(Value::Text(value.to_owned()));
    }
}
fn write_event(tx: &Transaction<'_>, cluster_id: &str, e: &ClusterEvent) -> Result<(), StoreError> {
    let payload = serde_json::to_string(e)?;
    tx.execute("INSERT INTO events VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,1,?22) ON CONFLICT(cluster_id,event_uid) DO UPDATE SET namespace=excluded.namespace,name=excluded.name,resource_version=excluded.resource_version,event_type=excluded.event_type,reason=excluded.reason,message=excluded.message,count=excluded.count,involved_kind=excluded.involved_kind,involved_namespace=excluded.involved_namespace,involved_name=excluded.involved_name,involved_uid=excluded.involved_uid,involved_api_version=excluded.involved_api_version,source_component=excluded.source_component,first_timestamp=excluded.first_timestamp,last_timestamp=excluded.last_timestamp,event_time=excluded.event_time,updated_at=excluded.updated_at,observed_at=excluded.observed_at,payload_version=excluded.payload_version,payload=excluded.payload",params![cluster_id,e.uid,e.namespace,e.name,e.resource_version,e.event_type,e.reason,e.message,i64::from(e.count),e.involved_object.kind,e.involved_object.namespace,e.involved_object.name,e.involved_object.uid,e.involved_object.api_version,e.source_component,e.first_timestamp.map(ts),e.last_timestamp.map(ts),e.event_time.map(ts),ts(e.registered_at),ts(Utc::now()),ts(e.observed_at()),payload])?;
    Ok(())
}
fn decode(r: &rusqlite::Row<'_>) -> rusqlite::Result<ClusterEvent> {
    let p: String = r.get(0)?;
    serde_json::from_str(&p).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}
#[derive(Debug)]
struct Scratch {
    objects: usize,
    occurrences: i64,
    first: DateTime<Utc>,
    last: DateTime<Utc>,
    affected: HashSet<String>,
    sample: String,
    sample_at: DateTime<Utc>,
}
fn accumulate(
    g: &mut HashMap<EventSummaryKey, Scratch>,
    e: ClusterEvent,
    fields: &[SummaryGroupBy],
) {
    let at = e.observed_at();
    let key = EventSummaryKey {
        namespace: fields
            .contains(&SummaryGroupBy::Namespace)
            .then(|| e.namespace.clone()),
        reason: fields
            .contains(&SummaryGroupBy::Reason)
            .then(|| e.reason.clone()),
        involved_kind: fields
            .contains(&SummaryGroupBy::InvolvedKind)
            .then(|| e.involved_object.kind.clone()),
        event_type: fields
            .contains(&SummaryGroupBy::Type)
            .then(|| e.event_type.clone()),
    };
    let obj = format!(
        "{}/{}/{}",
        e.involved_object.kind, e.involved_object.namespace, e.involved_object.name
    );
    let v = g.entry(key).or_insert_with(|| Scratch {
        objects: 0,
        occurrences: 0,
        first: at,
        last: at,
        affected: HashSet::new(),
        sample: e.message.clone(),
        sample_at: at,
    });
    v.objects = v.objects.saturating_add(1);
    v.occurrences = v.occurrences.saturating_add(i64::from(e.count.max(0)));
    v.first = v.first.min(at);
    v.last = v.last.max(at);
    v.affected.insert(obj);
    if at >= v.sample_at {
        v.sample_at = at;
        v.sample = e.message;
    }
}
fn ts(v: DateTime<Utc>) -> String {
    v.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}
fn parse_ts(v: String) -> Result<DateTime<Utc>, StoreError> {
    DateTime::parse_from_rfc3339(&v)
        .map(|v| v.with_timezone(&Utc))
        .map_err(|_| StoreError::Timestamp(v))
}
fn to_i64(v: usize) -> Result<i64, StoreError> {
    i64::try_from(v).map_err(|_| StoreError::NumericRange)
}

/// Async shareable handle around [`SqliteEventStore`] for HTTP/MCP/pipeline tasks.
#[derive(Clone, Debug)]
pub struct EventStoreHandle {
    inner: Arc<tokio::sync::Mutex<SqliteEventStore>>,
    cluster_id: Arc<str>,
}

impl EventStoreHandle {
    /// Open a file-backed store.
    pub fn open(
        path: impl AsRef<Path>,
        cluster_id: impl Into<Arc<str>>,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(SqliteEventStore::open(path)?)),
            cluster_id: cluster_id.into(),
        })
    }

    /// Open an in-memory store (tests / demo without PVC).
    pub fn open_in_memory(cluster_id: impl Into<Arc<str>>) -> Result<Self, StoreError> {
        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(SqliteEventStore::open_in_memory()?)),
            cluster_id: cluster_id.into(),
        })
    }

    /// Wrap an already-opened shared SQLite mutex (writer + readers).
    #[must_use]
    pub fn from_shared(
        inner: Arc<tokio::sync::Mutex<SqliteEventStore>>,
        cluster_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            inner,
            cluster_id: cluster_id.into(),
        }
    }

    /// Shared SQLite mutex for the durable writer task.
    #[must_use]
    pub fn shared(&self) -> Arc<tokio::sync::Mutex<SqliteEventStore>> {
        Arc::clone(&self.inner)
    }

    #[must_use]
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Idempotent upsert without checkpoint (query-path dual-write / tests).
    pub async fn upsert(&self, event: &ClusterEvent) -> Result<UpsertOutcome, StoreError> {
        let mut guard = self.inner.lock().await;
        guard.upsert_with_checkpoint(&self.cluster_id, event, None)
    }

    pub async fn get(&self, uid: &str) -> Result<Option<ClusterEvent>, StoreError> {
        let guard = self.inner.lock().await;
        guard.get(&self.cluster_id, uid)
    }

    pub async fn search(&self, query: &EventSearchQuery) -> Result<EventSearchResult, StoreError> {
        let guard = self.inner.lock().await;
        guard.search(&self.cluster_id, query)
    }

    pub async fn summarize(
        &self,
        since: Option<DateTime<Utc>>,
        until: Option<DateTime<Utc>>,
        event_type: Option<&str>,
        group_by: &[SummaryGroupBy],
        limit: usize,
    ) -> Result<Vec<EventSummaryGroup>, StoreError> {
        let guard = self.inner.lock().await;
        guard.summarize(&self.cluster_id, since, until, event_type, group_by, limit)
    }

    /// List newest-first with the same filters as HTTP/MCP `list_recent_events`.
    pub async fn list(&self, query: &EventQuery) -> Result<Vec<ClusterEvent>, StoreError> {
        let search = EventSearchQuery {
            limit: query.limit.clamp(1, 500),
            namespaces: query.namespace.clone().map_or_else(Vec::new, |ns| vec![ns]),
            types: query
                .type_filter
                .clone()
                .map_or_else(Vec::new, |ty| vec![ty]),
            reasons: query
                .reason
                .clone()
                .map_or_else(Vec::new, |reason| vec![reason]),
            ..EventSearchQuery::default()
        };
        Ok(self.search(&search).await?.events)
    }

    pub async fn load_recent(&self, limit: usize) -> Result<Vec<ClusterEvent>, StoreError> {
        let guard = self.inner.lock().await;
        guard.load_recent(&self.cluster_id, limit)
    }

    pub async fn count(&self) -> Result<usize, StoreError> {
        let guard = self.inner.lock().await;
        guard.count(&self.cluster_id)
    }

    pub async fn observed_bounds(&self) -> Result<ObservedBounds, StoreError> {
        let guard = self.inner.lock().await;
        guard.observed_bounds(&self.cluster_id)
    }

    /// Row count and approximate on-disk size (page_count × page_size).
    pub async fn stats(&self) -> Result<StorageStats, StoreError> {
        let guard = self.inner.lock().await;
        guard.stats()
    }

    /// All checkpoints for this handle's cluster id.
    pub async fn list_checkpoints(&self) -> Result<Vec<WatchCheckpoint>, StoreError> {
        let guard = self.inner.lock().await;
        guard.list_checkpoints(&self.cluster_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::InvolvedObject;
    use tempfile::TempDir;
    fn event(uid: &str, rv: &str, at: DateTime<Utc>) -> ClusterEvent {
        ClusterEvent {
            uid: uid.into(),
            namespace: "dev".into(),
            name: uid.into(),
            resource_version: rv.into(),
            event_type: "Warning".into(),
            reason: "BackOff".into(),
            message: "restart".into(),
            count: 2,
            involved_object: InvolvedObject {
                kind: "Pod".into(),
                namespace: "dev".into(),
                name: "web".into(),
                uid: Some("pod".into()),
                api_version: Some("v1".into()),
            },
            source_component: Some("kubelet".into()),
            first_timestamp: None,
            last_timestamp: Some(at),
            event_time: None,
            registered_at: at,
        }
    }
    fn cp(rv: &str, at: DateTime<Utc>) -> WatchCheckpoint {
        WatchCheckpoint {
            cluster_id: "default".into(),
            scope: "all".into(),
            resource_version: rv.into(),
            updated_at: at,
        }
    }
    #[test]
    fn idempotency_and_atomic_checkpoint() -> Result<(), StoreError> {
        let n = Utc::now();
        let mut s = SqliteEventStore::open_in_memory()?;
        let e = event("a", "1", n);
        assert_eq!(
            s.upsert_with_checkpoint("default", &e, None)?,
            UpsertOutcome::Inserted
        );
        assert_eq!(
            s.upsert_with_checkpoint("default", &e, None)?,
            UpsertOutcome::Deduped
        );
        assert!(
            s.upsert_with_checkpoint("default", &event("b", "1", n), Some(&cp("", n)))
                .is_err()
        );
        assert!(s.get("default", "b")?.is_none());
        Ok(())
    }
    #[test]
    fn restart_recovery() -> Result<(), StoreError> {
        let d = TempDir::new().map_err(|e| {
            StoreError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
        })?;
        let p = d.path().join("e.db");
        let n = Utc::now();
        {
            let mut s = SqliteEventStore::open(&p)?;
            s.upsert_with_checkpoint("default", &event("a", "42", n), Some(&cp("42", n)))?;
            s.upsert_with_checkpoint("default", &event("b", "43", n), Some(&cp("43", n)))?;
        }
        let s = SqliteEventStore::open(&p)?;
        assert!(s.get("default", "a")?.is_some());
        assert!(s.get("default", "b")?.is_some());
        assert_eq!(s.load_recent("default", 10)?.len(), 2);
        assert_eq!(
            s.search(
                "default",
                &EventSearchQuery {
                    limit: 10,
                    reasons: vec!["BackOff".into()],
                    ..EventSearchQuery::default()
                }
            )?
            .returned,
            2
        );
        assert_eq!(
            s.load_checkpoint("default", "all")?
                .map(|v| v.resource_version),
            Some("43".into())
        );
        Ok(())
    }

    #[test]
    fn corrupt_db_open_fails() {
        let d = TempDir::new().expect("tmpdir");
        let p = d.path().join("corrupt.db");
        std::fs::write(&p, b"not-a-sqlite-database").expect("write");
        let err = SqliteEventStore::open(&p).expect_err("corrupt open must fail");
        assert!(matches!(err, StoreError::Sqlite(_)));
    }
    #[test]
    fn bounded_prune() -> Result<(), StoreError> {
        let n = Utc::now();
        let mut s = SqliteEventStore::open_in_memory()?;
        for i in 0..8 {
            s.upsert_with_checkpoint(
                "default",
                &event(
                    &i.to_string(),
                    "1",
                    n - chrono::Duration::days(10) + chrono::Duration::seconds(i),
                ),
                None,
            )?;
        }
        for i in 8..13 {
            s.upsert_with_checkpoint("default", &event(&i.to_string(), "1", n), None)?;
        }
        assert_eq!(s.prune(n, DEFAULT_RETENTION, 3, 4)?.total(), 4);
        assert_eq!(s.prune(n, DEFAULT_RETENTION, 3, 20)?.total(), 6);
        assert_eq!(s.load_recent("default", 10)?.len(), 3);
        Ok(())
    }
}

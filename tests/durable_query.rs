//! Durable EventStore query contract: keyset pagination + restart recovery.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use clustersentinel::config::EventsMode;
use clustersentinel::events::{
    ClusterEvent, EventQuery, EventRegistry, EventSearchQuery, EventStore, EventStoreHandle,
    InvolvedObject, SqliteEventStore, StorageBackend, StorageHealthHandle, WatchCheckpoint,
    WatchStateHandle, encode_keyset_cursor, parse_cursor,
};
use clustersentinel::http::{HttpState, router};
use clustersentinel::metrics::Metrics;
use http_body_util::BodyExt;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const TOKEN: &str = "durable-query-token";

fn sample(uid: &str, at: DateTime<Utc>, reason: &str) -> ClusterEvent {
    ClusterEvent {
        uid: uid.into(),
        namespace: "dev".into(),
        name: format!("evt-{uid}"),
        resource_version: "1".into(),
        event_type: "Warning".into(),
        reason: reason.into(),
        message: format!("message-{uid}"),
        count: 1,
        involved_object: InvolvedObject {
            kind: "Pod".into(),
            namespace: "dev".into(),
            name: "web".into(),
            uid: Some(format!("pod-{uid}")),
            api_version: Some("v1".into()),
        },
        source_component: Some("kubelet".into()),
        first_timestamp: Some(at),
        last_timestamp: Some(at),
        event_time: Some(at),
        registered_at: at,
    }
}

#[tokio::test]
async fn keyset_pagination_has_no_skips_or_duplicates() {
    let store = EventStoreHandle::open_in_memory("default").expect("store");
    let base = DateTime::parse_from_rfc3339("2026-09-08T12:00:00.000000Z")
        .expect("ts")
        .with_timezone(&Utc);
    for i in 0..5 {
        let at = base + ChronoDuration::seconds(i);
        store
            .upsert(&sample(&format!("e{i}"), at, "BackOff"))
            .await
            .expect("upsert");
    }

    let page1 = store
        .search(&EventSearchQuery {
            limit: 2,
            ..EventSearchQuery::default()
        })
        .await
        .expect("page1");
    assert_eq!(page1.returned, 2);
    assert!(page1.truncated);
    assert_eq!(page1.events[0].uid, "e4");
    assert_eq!(page1.events[1].uid, "e3");
    let (obs, uid) = page1.next_after.expect("next_after");
    let cursor = encode_keyset_cursor(obs, &uid);
    assert!(cursor.starts_with("v1:"));

    let page2 = store
        .search(&EventSearchQuery {
            limit: 2,
            after: Some((obs, uid)),
            ..EventSearchQuery::default()
        })
        .await
        .expect("page2");
    assert_eq!(page2.events[0].uid, "e2");
    assert_eq!(page2.events[1].uid, "e1");

    let page3 = store
        .search(&EventSearchQuery {
            limit: 2,
            after: page2.next_after.clone(),
            ..EventSearchQuery::default()
        })
        .await
        .expect("page3");
    assert_eq!(page3.events.len(), 1);
    assert_eq!(page3.events[0].uid, "e0");
    assert!(!page3.truncated);
    assert!(page3.next_after.is_none());

    let mut seen = Vec::new();
    seen.extend(page1.events.iter().map(|e| e.uid.clone()));
    seen.extend(page2.events.iter().map(|e| e.uid.clone()));
    seen.extend(page3.events.iter().map(|e| e.uid.clone()));
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(seen.len(), unique.len(), "no duplicates across pages");
    assert_eq!(seen.len(), 5, "no skips across pages");

    // Deprecated decimal offset still works and emits keyset next_after.
    let offset_page = store
        .search(&EventSearchQuery {
            limit: 2,
            offset: 2,
            ..EventSearchQuery::default()
        })
        .await
        .expect("offset page");
    assert_eq!(offset_page.next_offset, Some(4));
    assert!(offset_page.next_after.is_some());
    assert!(parse_cursor(Some(&cursor)).is_ok());
}

#[tokio::test]
async fn restart_recovery_preserves_uid_query_count_checkpoint() {
    // Simulate process stop/start on the same SQLite file (DEV-294).
    let dir = TempDir::new().expect("tmpdir");
    let path = dir.path().join("events.db");
    let now = Utc::now().with_nanosecond(0).expect("whole second");

    {
        let mut store = SqliteEventStore::open(&path).expect("open write");
        store
            .upsert_with_checkpoint(
                "default",
                &sample("persist-1", now, "FailedMount"),
                Some(&WatchCheckpoint {
                    cluster_id: "default".into(),
                    scope: "all".into(),
                    resource_version: "42".into(),
                    updated_at: now,
                }),
            )
            .expect("upsert+checkpoint");
        store
            .upsert_with_checkpoint(
                "default",
                &sample("persist-2", now - ChronoDuration::seconds(1), "BackOff"),
                Some(&WatchCheckpoint {
                    cluster_id: "default".into(),
                    scope: "all".into(),
                    resource_version: "43".into(),
                    updated_at: now,
                }),
            )
            .expect("upsert+checkpoint");
    }

    let store = EventStoreHandle::open(&path, "default").expect("reopen");
    assert!(store.get("persist-1").await.expect("get").is_some());
    assert!(store.get("persist-2").await.expect("get").is_some());
    assert_eq!(store.count().await.expect("count"), 2);
    assert_eq!(
        store.observed_bounds().await.expect("bounds"),
        (Some(now - ChronoDuration::seconds(1)), Some(now),)
    );

    let listed = store
        .list(&EventQuery {
            limit: 10,
            reason: Some("FailedMount".into()),
            ..EventQuery::default()
        })
        .await
        .expect("query");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].uid, "persist-1");

    let checkpoints = store.list_checkpoints().await.expect("checkpoints");
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].scope, "all");
    assert_eq!(checkpoints[0].resource_version, "43");

    let metrics = Metrics::try_new().expect("metrics");
    let registry = EventRegistry::new(10, Duration::from_secs(60));
    let app = router(
        HttpState {
            registry,
            store,
            metrics,
            watch_state: WatchStateHandle::new(),
            storage_health: {
                let h = StorageHealthHandle::new(StorageBackend::Sqlite);
                h.mark_ready();
                h
            },
            events_mode: EventsMode::Demo,
            mcp_allowed_hosts: vec!["localhost".into()],
            mcp_auth_token: Arc::from(TOKEN),
            metrics_event_limit: 50,
            configured_namespaces: Vec::new(),
            build_version: "0.1.0-test".into(),
            git_sha: "deadbeef".into(),
        },
        CancellationToken::new().child_token(),
    );

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/events?limit=10")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(
        resp.into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec(),
    )
    .expect("utf8");
    assert!(body.contains("\"count\":2"));
    assert!(body.contains("persist-1"));
    assert!(body.contains("persist-2"));
    assert!(body.contains("FailedMount"));
}

#[tokio::test]
async fn list_and_summarize_read_from_store() {
    let store = EventStoreHandle::open_in_memory("default").expect("store");
    let now = Utc::now();
    store
        .upsert(&sample("a", now, "BackOff"))
        .await
        .expect("upsert");
    store
        .upsert(&sample(
            "b",
            now - ChronoDuration::seconds(5),
            "FailedMount",
        ))
        .await
        .expect("upsert");

    let listed = store
        .list(&EventQuery {
            limit: 10,
            reason: Some("BackOff".into()),
            ..EventQuery::default()
        })
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].uid, "a");

    let groups = store
        .summarize(None, None, Some("Warning"), &[], 10)
        .await
        .expect("summarize");
    assert!(!groups.is_empty());
}

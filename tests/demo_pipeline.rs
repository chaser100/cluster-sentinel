//! Integration tests without a live Kubernetes cluster.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use clustersentinel::config::EventsMode;
use clustersentinel::events::{
    EventQuery, EventRegistry, EventStoreHandle, StorageBackend, StorageHealthHandle,
    WatchResumeAction, WatchStateHandle, classify_watch_error, next_backoff, run_event_pipeline,
    spawn_durable_writer,
};
use clustersentinel::http::{HttpState, router};
use clustersentinel::metrics::Metrics;
use http_body_util::BodyExt;
use kube::Error as KubeError;
use kube::core::Status;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const TEST_MCP_TOKEN: &str = "test-mcp-bearer-token-dev-275";

fn test_store() -> EventStoreHandle {
    EventStoreHandle::open_in_memory("default").expect("in-memory store")
}

fn test_storage_health() -> StorageHealthHandle {
    let health = StorageHealthHandle::new(StorageBackend::Memory);
    health.mark_ready();
    health
}

fn test_http_state(
    registry: EventRegistry,
    store: EventStoreHandle,
    metrics: Metrics,
    watch_state: WatchStateHandle,
) -> HttpState {
    HttpState {
        registry,
        store,
        metrics,
        watch_state,
        storage_health: test_storage_health(),
        events_mode: EventsMode::Demo,
        mcp_allowed_hosts: vec!["localhost".into(), "127.0.0.1".into()],
        mcp_auth_token: Arc::from(TEST_MCP_TOKEN),
        metrics_event_limit: 500,
        configured_namespaces: Vec::new(),
        build_version: "0.1.0-test".into(),
        git_sha: "deadbeef".into(),
    }
}

fn test_config() -> clustersentinel::Config {
    clustersentinel::Config {
        bind_addr: "127.0.0.1:0".parse().expect("addr"),
        events_mode: EventsMode::Demo,
        list_limit: 500,
        watch_timeout: Duration::from_secs(290),
        watch_backoff: Duration::from_secs(5),
        watch_backoff_max: Duration::from_secs(60),
        registry_capacity: 100,
        dedup_ttl: Duration::from_secs(3600),
        metrics_event_limit: 500,
        namespaces: Vec::new(),
        mcp_allowed_hosts: vec!["localhost".into()],
        mcp_auth_token: Some(TEST_MCP_TOKEN.to_owned()),
        storage_path: None,
        cluster_id: "default".into(),
        storage_retention: Duration::from_secs(7 * 24 * 60 * 60),
        storage_max_events: 250_000,
        storage_prune_batch: 1_000,
        writer_queue_capacity: 64,
    }
}

async fn response_bytes(response: axum::response::Response) -> (StatusCode, String, Vec<u8>) {
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes()
        .to_vec();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    (status, text, bytes)
}

fn parse_jsonrpc_payload(raw: &str) -> Value {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        panic!("empty MCP response body");
    }
    if trimmed.starts_with('{') {
        return serde_json::from_str(trimmed).unwrap_or_else(|err| {
            panic!("json parse failed: {err}; body={trimmed}");
        });
    }
    // Streamable HTTP may return SSE: `event: message\ndata: {...}\n\n`
    for line in trimmed.lines() {
        let line = line.trim();
        if let Some(data) = line.strip_prefix("data:") {
            let data = data.trim();
            if data.starts_with('{') {
                return serde_json::from_str(data).unwrap_or_else(|err| {
                    panic!("sse data json parse failed: {err}; data={data}");
                });
            }
        }
    }
    panic!("unsupported MCP response body: {trimmed}");
}

async fn mcp_post(
    app: &axum::Router,
    session: &str,
    id: u64,
    method: &str,
    params: Option<Value>,
) -> Value {
    let mut payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
    });
    if let Some(params) = params {
        payload["params"] = params;
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("mcp-session-id", session)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    serde_json::to_vec(&payload).expect("MCP request JSON"),
                ))
                .expect("request"),
        )
        .await
        .expect("MCP request");
    assert_eq!(response.status(), StatusCode::OK, "method={method}");
    parse_jsonrpc_payload(&response_bytes(response).await.1)
}

#[tokio::test]
async fn demo_pipeline_seeds_registry_and_http_surfaces() {
    let metrics = Metrics::try_new().expect("metrics");
    let registry = EventRegistry::new(100, Duration::from_secs(3600));
    let store = test_store();
    let watch_state = WatchStateHandle::new();
    let cancel = CancellationToken::new();
    let storage_health = test_storage_health();
    let (writer, writer_join) = spawn_durable_writer(
        store.shared(),
        64,
        cancel.child_token(),
        metrics.clone(),
        storage_health,
    );

    let pipeline_cancel = cancel.child_token();
    let pipeline_registry = registry.clone();
    let pipeline_metrics = metrics.clone();
    let pipeline_watch = watch_state.clone();
    let pipeline = tokio::spawn(async move {
        let mut config = test_config();
        config.registry_capacity = 100;
        run_event_pipeline(
            config,
            pipeline_registry,
            writer,
            pipeline_metrics,
            pipeline_watch,
            pipeline_cancel,
        )
        .await;
    });

    // Demo seed is synchronous before waiting on cancel; give the task a tick.
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(registry.len().await, 3);
    assert!(registry.get("demo-uid-1").await.is_some());
    assert!(store.get("demo-uid-1").await.expect("store get").is_some());
    let listed = registry
        .list(EventQuery {
            limit: 10,
            namespace: Some("demo".into()),
            reason: None,
            type_filter: Some("Warning".into()),
        })
        .await;
    assert_eq!(listed.len(), 2);

    let app = router(
        test_http_state(
            registry.clone(),
            store.clone(),
            metrics.clone(),
            watch_state.clone(),
        ),
        cancel.child_token(),
    );

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("health response");
    assert_eq!(health.status(), StatusCode::OK);
    let health_body = String::from_utf8(
        health
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec(),
    )
    .expect("utf8");
    assert!(health_body.contains("\"events_mode\":\"demo\""));
    assert!(health_body.contains("\"registry_size\":3"));
    assert!(health_body.contains("\"storage_status\":\"ready\""));
    assert!(health_body.contains("\"storage_backend\":\"memory\""));
    assert!(
        health_body.contains("\"status\":\"healthy\"")
            || health_body.contains("\"status\":\"starting\"")
    );

    let ready = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("ready response");
    assert_eq!(ready.status(), StatusCode::OK);

    let not_ready_health = test_storage_health();
    not_ready_health.note_error("simulated writer failure");
    let not_ready_app = router(
        HttpState {
            registry: registry.clone(),
            store: store.clone(),
            metrics: metrics.clone(),
            watch_state: watch_state.clone(),
            storage_health: not_ready_health,
            events_mode: EventsMode::Demo,
            mcp_allowed_hosts: vec!["localhost".into(), "127.0.0.1".into()],
            mcp_auth_token: Arc::from(TEST_MCP_TOKEN),
            metrics_event_limit: 500,
            configured_namespaces: Vec::new(),
            build_version: "0.1.0-test".into(),
            git_sha: "deadbeef".into(),
        },
        cancel.child_token(),
    );
    let ready_fail = not_ready_app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("ready fail response");
    assert_eq!(ready_fail.status(), StatusCode::SERVICE_UNAVAILABLE);

    let metrics_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics_resp.status(), StatusCode::OK);
    let metrics_body = String::from_utf8(
        metrics_resp
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec(),
    )
    .expect("utf8");
    assert!(metrics_body.contains("clustersentinel_events_registered_total"));
    assert!(metrics_body.contains("clustersentinel_storage_writes_total"));
    assert!(metrics_body.contains("event_namespace=\"demo\""));
    assert!(metrics_body.contains("involved_kind=\"Pod\""));
    assert!(metrics_body.contains("reason=\"BackOff\""));
    assert!(metrics_body.contains("clustersentinel_events_registry_size"));
    assert!(metrics_body.contains("clustersentinel_event_last_seen_timestamp{"));
    assert!(metrics_body.contains("involved_object=\"Pod/"));
    assert!(metrics_body.contains("message=\""));

    let events_no_auth = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/events?limit=10&type=Warning")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("events response");
    assert_eq!(events_no_auth.status(), StatusCode::UNAUTHORIZED);
    let metrics_after_auth = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics after auth");
    let metrics_after_auth_body = String::from_utf8(
        metrics_after_auth
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec(),
    )
    .expect("utf8");
    assert!(metrics_after_auth_body.contains("clustersentinel_mcp_auth_failures_total"));

    let events_resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/events?limit=10&type=Warning")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("events response");
    assert_eq!(events_resp.status(), StatusCode::OK);
    let events_body = String::from_utf8(
        events_resp
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes()
            .to_vec(),
    )
    .expect("utf8");
    assert!(events_body.contains("\"count\":2"));
    assert!(events_body.contains("demo pod crashloop"));
    assert!(events_body.contains("demo volume mount failure"));

    cancel.cancel();
    pipeline.await.expect("pipeline join");
    writer_join.await.expect("writer join");
}

#[tokio::test]
async fn mcp_requires_valid_bearer_token() {
    let metrics = Metrics::try_new().expect("metrics");
    let registry = EventRegistry::new(10, Duration::from_secs(60));
    let watch_state = WatchStateHandle::new();
    let cancel = CancellationToken::new();
    let app = router(
        test_http_state(registry, test_store(), metrics, watch_state),
        cancel.child_token(),
    );

    let no_auth = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(no_auth.status(), StatusCode::UNAUTHORIZED);
    assert!(
        no_auth
            .headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("Bearer"))
    );

    let bad_auth = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", "Bearer wrong-token")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(bad_auth.status(), StatusCode::UNAUTHORIZED);

    let with_auth = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.0.1"}}}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        with_auth.status(),
        StatusCode::OK,
        "valid Bearer + allowed Host must initialize"
    );
    assert!(
        with_auth.headers().get("mcp-session-id").is_some(),
        "initialize must mint Mcp-Session-Id"
    );

    // GET without session → 400 (Kotlin/Cursor: "Expected status code 200 but was 400").
    let get_no_session = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("accept", "text/event-stream")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        get_no_session.status(),
        StatusCode::BAD_REQUEST,
        "GET /mcp without Mcp-Session-Id must be 400"
    );

    // Valid Bearer + Host outside allow-list → rmcp 403 (not 401).
    let disallowed_host = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "clustersentinel.svc.cluster.local")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.0.1"}}}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(
        disallowed_host.status(),
        StatusCode::FORBIDDEN,
        "prod Host must be 403 when test allow-list is localhost-only"
    );

    // /health stays open for probes.
    let health = router(
        test_http_state(
            EventRegistry::new(1, Duration::from_secs(60)),
            test_store(),
            Metrics::try_new().expect("metrics"),
            WatchStateHandle::new(),
        ),
        cancel.child_token(),
    )
    .oneshot(
        Request::builder()
            .uri("/health")
            .body(Body::empty())
            .expect("request"),
    )
    .await
    .expect("health");
    assert_eq!(health.status(), StatusCode::OK);
}

#[tokio::test]
async fn mcp_streamable_handshake_tools_and_resources() {
    let metrics = Metrics::try_new().expect("metrics");
    let registry = EventRegistry::new(100, Duration::from_secs(3600));
    let store = test_store();
    let watch_state = WatchStateHandle::new();
    watch_state.set(clustersentinel::events::WatchState::Watching);
    let cancel = CancellationToken::new();

    // Seed one event for tools/call (durable store is the query source).
    let now = chrono::Utc::now();
    let event = clustersentinel::events::ClusterEvent {
        uid: "mcp-test-uid".into(),
        namespace: "demo".into(),
        name: "evt.mcp".into(),
        resource_version: "1".into(),
        event_type: "Warning".into(),
        reason: "BackOff".into(),
        message: "handshake probe".into(),
        count: 2,
        involved_object: clustersentinel::events::InvolvedObject {
            kind: "Pod".into(),
            namespace: "demo".into(),
            name: "web".into(),
            uid: Some("pod-1".into()),
            api_version: Some("v1".into()),
        },
        source_component: Some("kubelet".into()),
        first_timestamp: Some(now),
        last_timestamp: Some(now),
        event_time: Some(now),
        registered_at: now,
    };
    registry.upsert(event.clone()).await;
    store.upsert(&event).await.expect("store upsert");

    let app = router(
        test_http_state(registry, store, metrics, watch_state),
        cancel.child_token(),
    );

    let init = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.0.1"}}}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("initialize");
    assert_eq!(init.status(), StatusCode::OK);
    let session = init
        .headers()
        .get("mcp-session-id")
        .expect("session")
        .to_str()
        .expect("session utf8")
        .to_owned();
    let init_json = parse_jsonrpc_payload(&response_bytes(init).await.1);
    assert_eq!(init_json["result"]["serverInfo"]["name"], "clustersentinel");

    let initialized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("mcp-session-id", &session)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("initialized");
    assert!(
        initialized.status().is_success() || initialized.status() == StatusCode::ACCEPTED,
        "initialized notification status={}",
        initialized.status()
    );

    let tools = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("mcp-session-id", &session)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("tools/list");
    assert_eq!(tools.status(), StatusCode::OK);
    let tools_json = parse_jsonrpc_payload(&response_bytes(tools).await.1);
    let tool_names: Vec<String> = tools_json["result"]["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|tool| tool["name"].as_str().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(tool_names.len(), 6, "unexpected tool set: {tool_names:?}");
    for expected in [
        "get_health",
        "list_recent_events",
        "get_event",
        "search_events",
        "summarize_events",
        "get_metrics_summary",
    ] {
        assert!(
            tool_names.iter().any(|name| name == expected),
            "missing tool {expected} in {tool_names:?}"
        );
    }
    for tool in tools_json["result"]["tools"].as_array().expect("tools") {
        assert!(
            tool["outputSchema"].is_object(),
            "missing outputSchema: {tool}"
        );
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(tool["annotations"]["destructiveHint"], false);
        assert_eq!(tool["annotations"]["idempotentHint"], true);
        assert_eq!(tool["annotations"]["openWorldHint"], false);
    }

    let call = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("mcp-session-id", &session)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search_events","arguments":{"limit":5,"reasons":["BackOff"]}}}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("tools/call");
    assert_eq!(call.status(), StatusCode::OK);
    let call_json = parse_jsonrpc_payload(&response_bytes(call).await.1);
    assert!(
        call_json["result"]["structuredContent"]["matched"]
            .as_u64()
            .unwrap_or(0)
            >= 1
            || call_json["result"]["content"]
                .as_array()
                .is_some_and(|c| !c.is_empty()),
        "search_events must return matches: {call_json}"
    );

    for (id, name, arguments) in [
        (10, "get_health", serde_json::json!({})),
        (
            11,
            "list_recent_events",
            serde_json::json!({"limit": 5, "type_filter": "Warning"}),
        ),
        (12, "get_event", serde_json::json!({"uid": "mcp-test-uid"})),
        (
            13,
            "summarize_events",
            serde_json::json!({"type": "Warning"}),
        ),
        (14, "get_metrics_summary", serde_json::json!({})),
    ] {
        let response = mcp_post(
            &app,
            &session,
            id,
            "tools/call",
            Some(serde_json::json!({"name": name, "arguments": arguments})),
        )
        .await;
        assert!(
            !response["result"]["structuredContent"].is_null(),
            "tool {name} must return structuredContent: {response}"
        );
    }

    for (id, name, arguments) in [
        (
            20,
            "search_events",
            serde_json::json!({"types": ["Critical"]}),
        ),
        (
            21,
            "summarize_events",
            serde_json::json!({"type": "Critical"}),
        ),
        (
            22,
            "search_events",
            serde_json::json!({"cursor": "not-a-cursor"}),
        ),
        (
            23,
            "summarize_events",
            serde_json::json!({"types": ["Warning"]}),
        ),
    ] {
        let response = mcp_post(
            &app,
            &session,
            id,
            "tools/call",
            Some(serde_json::json!({"name": name, "arguments": arguments})),
        )
        .await;
        assert!(
            response["error"]["code"] == -32602 || response["result"]["isError"] == true,
            "invalid arguments must fail for {name}: {response}"
        );
    }

    let resources = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("mcp-session-id", &session)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":4,"method":"resources/list"}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("resources/list");
    assert_eq!(resources.status(), StatusCode::OK);
    let resources_json = parse_jsonrpc_payload(&response_bytes(resources).await.1);
    assert!(
        resources_json["result"]["resources"]
            .as_array()
            .is_some_and(|items| items.len() >= 2)
    );

    let read = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", "localhost")
                .header("authorization", format!("Bearer {TEST_MCP_TOKEN}"))
                .header("mcp-session-id", &session)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":5,"method":"resources/read","params":{"uri":"clustersentinel://status"}}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("resources/read");
    assert_eq!(read.status(), StatusCode::OK);
    let read_json = parse_jsonrpc_payload(&response_bytes(read).await.1);
    let text = read_json["result"]["contents"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(text.contains("\"status\":\"healthy\""));
    assert!(text.contains("build_version"));

    let recent = mcp_post(
        &app,
        &session,
        30,
        "resources/read",
        Some(serde_json::json!({
            "uri": "clustersentinel://events/recent"
        })),
    )
    .await;
    let recent_text = recent["result"]["contents"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(recent_text.contains("mcp-test-uid"));
}

#[test]
fn http_mode_config_requires_mcp_token() {
    let mut config = test_config();
    config.mcp_auth_token = None;
    assert!(config.require_mcp_auth_token().is_err());
    config.mcp_auth_token = Some(String::new());
    assert!(config.require_mcp_auth_token().is_err());
    config.mcp_auth_token = Some("ok".into());
    assert_eq!(config.require_mcp_auth_token().expect("token"), "ok");
}

#[test]
fn fake_watch_resume_classifier_matches_apiserver_semantics() {
    let expired = KubeError::Api(
        Status::failure("too old resource version: 12345", "Gone")
            .with_code(410)
            .boxed(),
    );
    assert_eq!(
        classify_watch_error(&expired),
        WatchResumeAction::Relist,
        "410 must force re-list"
    );

    let disconnect = KubeError::Api(
        Status::failure("unexpected EOF", "InternalError")
            .with_code(500)
            .boxed(),
    );
    assert_eq!(
        classify_watch_error(&disconnect),
        WatchResumeAction::Rewatch,
        "transient errors keep resourceVersion"
    );

    // Simulate resume decision path used by the kubernetes loop.
    let mut resource_version = String::from("42");
    match classify_watch_error(&expired) {
        WatchResumeAction::Relist => resource_version.clear(),
        WatchResumeAction::Rewatch => {}
    }
    assert!(resource_version.is_empty());

    resource_version = String::from("99");
    match classify_watch_error(&disconnect) {
        WatchResumeAction::Relist => resource_version.clear(),
        WatchResumeAction::Rewatch => {}
    }
    assert_eq!(resource_version, "99");

    assert_eq!(
        next_backoff(Duration::from_secs(5), Duration::from_secs(60), 3),
        Duration::from_secs(40)
    );
}

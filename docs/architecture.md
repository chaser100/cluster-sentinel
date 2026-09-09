# ClusterSentinel architecture

## Goal

Watch Kubernetes events efficiently, register them with durable dedup, expose
Prometheus metrics + alert/dashboard artifacts, and serve an embedded MCP
surface for agent callers. Deploy dev and prod through GitOps.

## Components

```text
┌──────────────────────────────────────────────────────────────┐
│                     clustersentinel process                   │
│                                                              │
│  EventWatcher ──► EventStore (SQLite) ──► Metrics            │
│       │               │                                      │
│       │               ├── EventRegistry (hot read cache)     │
│       │               └── MCP/HTTP event queries             │
│       └──────────► Axum HTTP                                 │
│                    /health  /metrics  /api/v1/events  /mcp   │
└──────────────────────────────────────────────────────────────┘
          ▲ kube watch/list (RBAC: events get/list/watch)
```

| Component | Responsibility |
|-----------|----------------|
| `EventWatcher` | Initial list + watch; backoff on errors; never tight-loop the apiserver |
| `EventStore` | Durable typed history and watch checkpoints in SQLite; idempotent upsert by `(cluster_id,event_uid)` |
| `EventRegistry` | Hot read cache; capacity and TTL eviction do not remove durable rows |
| `Metrics` | Prometheus counters/gauges/histograms |
| `HTTP` | Health, metrics scrape, event JSON API, streamable MCP at `/mcp` |
| `MCP` | Stable tools/resources for agents (see `docs/mcp.md`) |

## Event pipeline

1. **Bootstrap list** with paginated `limit` + `continue` when `resourceVersion` is empty (cold start or after 410 Gone). Persist all pages first, then commit the list snapshot checkpoint once; a crash between pages causes a safe deduplicated re-list instead of skipping unprocessed pages.
2. **Watch** from the last known resourceVersion with timeout; on clean EOF/timeout **resume watch without re-list**.
3. On watch/list errors: exponential backoff (`base * 2^n`, capped). HTTP **410** clears RV → re-list; other errors keep RV and rewatch.
4. Map `core/v1 Event` to typed `ClusterEvent` data.
5. Send the event to a bounded single-writer queue. SQLite commits the idempotent event upsert and its watch checkpoint in one transaction before acknowledging it.
6. After the commit, update the in-memory registry and metrics. On restart, warm the registry from recent durable rows and resume each watch scope from its checkpoint. Prune expired and overflow rows at startup and every five minutes, processing bounded batches until the store is current.
7. Agents query durable history through MCP (`search_events`, `summarize_events`, `list_recent_events`, `get_event`) and the HTTP API.

HTTP auth: `/mcp` and `/api/v1/events` require Bearer; `/health` public; `/metrics` for in-cluster scrape only (do not put on external HTTPRoute).

Modes:

- `kubernetes` — in-cluster / kubeconfig client (deployed environments).
- `demo` — synthetic events for local/CI without a cluster.

## Configuration (env)

| Variable | Default | Purpose |
|----------|---------|---------|
| `CLUSTERSENTINEL_BIND` | `0.0.0.0:8080` | HTTP listen address |
| `CLUSTERSENTINEL_EVENTS_MODE` | `kubernetes` | `kubernetes` \| `demo` |
| `CLUSTERSENTINEL_LIST_LIMIT` | `500` | Bootstrap list page size |
| `CLUSTERSENTINEL_WATCH_TIMEOUT_SECS` | `290` | Watch call timeout (&lt;295 required by kube) |
| `CLUSTERSENTINEL_WATCH_BACKOFF_SECS` | `5` | Initial backoff |
| `CLUSTERSENTINEL_WATCH_BACKOFF_MAX_SECS` | `60` | Max exponential backoff |
| `CLUSTERSENTINEL_REGISTRY_CAPACITY` | `10000` | Max retained events |
| `CLUSTERSENTINEL_DEDUP_TTL_SECS` | `3600` | Registry eviction TTL |
| `CLUSTERSENTINEL_METRICS_EVENT_LIMIT` | `500` | Max retained events considered when building inventory gauge series on `/metrics` |
| `CLUSTERSENTINEL_NAMESPACES` | _(empty=all)_ | Comma-separated namespace filter |
| `CLUSTERSENTINEL_BUILD_VERSION` | crate version | MCP `serverInfo.version` / health `build_version` |
| `CLUSTERSENTINEL_GIT_SHA` | `unknown` | health `git_sha` |
| `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` | loopback + in-cluster Service DNS | MCP DNS-rebinding Host allow-list; add public hostnames per env |
| `CLUSTERSENTINEL_MCP_AUTH_TOKEN` | _(required for HTTP)_ | Bearer for `/mcp`; fail-fast if missing. Not used by `--mcp-stdio`. Never store in Git. |
| `CLUSTERSENTINEL_STORAGE_PATH` | `/var/lib/clustersentinel/events.db` in Kubernetes; memory in demo mode | SQLite path; `memory` and `:memory:` select an in-memory database |
| `CLUSTERSENTINEL_CLUSTER_ID` | `default` | Logical cluster identifier for rows and checkpoints |
| `CLUSTERSENTINEL_STORAGE_RETENTION_SECS` | `604800` | Durable event retention window |
| `CLUSTERSENTINEL_STORAGE_MAX_EVENTS` | `250000` | Durable row limit |
| `CLUSTERSENTINEL_STORAGE_PRUNE_BATCH` | `1000` | Maximum rows removed per prune operation |
| `CLUSTERSENTINEL_WRITER_QUEUE_CAPACITY` | `64` | Bounded writer queue capacity |

## Observability

| Artifact | Path |
|----------|------|
| Metrics endpoint | `GET /metrics` |
| Alert rules | `observability/alerts/clustersentinel.yaml` |
| Grafana dashboard | `observability/dashboards/clustersentinel.json` — title `ClusterSentinel`, uid `clustersentinel` |
| ServiceMonitor | GitOps values (`serviceMonitor.enabled: true`) |

The Helm chart embeds the dashboard in a ConfigMap. Its discovery labels must match the Grafana sidecar selector.

Key series:

- `clustersentinel_events_registered_total{type,reason,event_namespace,involved_kind,source}`
  (`event_namespace` avoids colliding with Prometheus scrape `namespace`)
- `clustersentinel_event_last_seen_timestamp{event_namespace,type,reason,involved_object,message,count,source}`
  (bounded inventory for Grafana tables; identical label sets are merged using the latest timestamp; `message` truncated to 256 chars; full text via `/api/v1/events` or MCP)
- `clustersentinel_events_deduped_total`
- `clustersentinel_events_registry_size`
- `clustersentinel_watch_restarts_total`
- `clustersentinel_watch_errors_total`
- `clustersentinel_http_requests_total{path,status}`
- `clustersentinel_storage_writes_total{result}`
- `clustersentinel_storage_write_duration_seconds{operation}`
- `clustersentinel_storage_errors_total{operation}`
- `clustersentinel_storage_rows`
- `clustersentinel_storage_bytes`
- `clustersentinel_storage_pruned_total{reason}`
- `clustersentinel_storage_last_success_timestamp`
- `clustersentinel_watch_checkpoint_age_seconds{scope}`

`/health` is the liveness endpoint and always returns `200`; its body includes storage state. `/ready` returns `503` while storage is unavailable and is the chart's default readiness probe.

## RBAC model (least privilege)

ClusterRole (each environment):

- `apiGroups: [""]`, `resources: ["events"]`, `verbs: ["get","list","watch"]`
- Optional: `namespaces` `get/list` for agent context (no secrets, no write verbs)

No cluster-admin. No secret/read access. GitOps binds the ClusterRole to the environment's
ServiceAccount.

## Distribution

- Docker images are published at `chaser420/cluster-sentinel` with semantic version tags.
- The Helm repository is served from `https://chaser100.github.io/cluster-sentinel`.
- The chart contains the event RBAC, SQLite PVC configuration, PrometheusRule, Grafana dashboard ConfigMap, and MCP Secret template.
- External routes must exclude `/metrics`; Prometheus should scrape it through the in-cluster ServiceMonitor.

## Out of scope

- Multi-cluster fan-out
- Shared infra (Vault HA, ESO platform, ArgoCD) changes

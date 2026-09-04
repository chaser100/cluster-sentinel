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
│  EventWatcher ──► EventRegistry (dedup ring) ──► Metrics     │
│       │                  │                                   │
│       │                  ▼                                   │
│       │            MCP tools/resources                       │
│       │                  │                                   │
│       └──────────► Axum HTTP                                 │
│                    /health  /metrics  /api/v1/events  /mcp   │
└──────────────────────────────────────────────────────────────┘
          ▲ kube watch/list (RBAC: events get/list/watch)
```

| Component | Responsibility |
|-----------|----------------|
| `EventWatcher` | Initial list + watch; backoff on errors; never tight-loop the apiserver |
| `EventRegistry` | Typed store; dedup by UID; capacity + TTL eviction |
| `Metrics` | Prometheus counters/gauges/histograms |
| `HTTP` | Health, metrics scrape, event JSON API, streamable MCP at `/mcp` |
| `MCP` | Stable tools/resources for agents (see `docs/mcp.md`) |

## Event pipeline

1. **Bootstrap list** with paginated `limit` + `continue` when `resourceVersion` is empty (cold start or after 410 Gone).
2. **Watch** from the last known resourceVersion with timeout; on clean EOF/timeout **resume watch without re-list**.
3. On watch/list errors: exponential backoff (`base * 2^n`, capped). HTTP **410** clears RV → re-list; other errors keep RV and rewatch.
4. Map `core/v1 Event` → typed `ClusterEvent` (identity, reason, message, timestamps).
5. `EventRegistry::upsert`:
   - skip if UID already seen and unchanged (`resource_version`/`count`/`message`);
   - on update, move UID to newest order and preserve first `registered_at` for TTL;
   - list/search sort by `observed_at`; TTL eviction runs on list/get/len/search;
   - emit metrics (`events_registered_total`, `events_deduped_total`).
6. Agents query via MCP (`search_events`, `summarize_events`, `list_recent_events`, `get_event`) — read-only.

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
| `CLUSTERSENTINEL_METRICS_EVENT_LIMIT` | `500` | Max events exported as inventory gauges on `/metrics` |
| `CLUSTERSENTINEL_NAMESPACES` | _(empty=all)_ | Comma-separated namespace filter |
| `CLUSTERSENTINEL_BUILD_VERSION` | crate version | MCP `serverInfo.version` / health `build_version` |
| `CLUSTERSENTINEL_GIT_SHA` | `unknown` | health `git_sha` |
| `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` | loopback + in-cluster Service DNS | MCP DNS-rebinding Host allow-list; add public hostnames per env |
| `CLUSTERSENTINEL_MCP_AUTH_TOKEN` | _(required for HTTP)_ | Bearer for `/mcp`; fail-fast if missing. Not used by `--mcp-stdio`. Never store in Git. |

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
  (bounded inventory for Grafana tables; `message` truncated to 256 chars; full text via `/api/v1/events` or MCP)
- `clustersentinel_events_deduped_total`
- `clustersentinel_events_registry_size`
- `clustersentinel_watch_restarts_total`
- `clustersentinel_watch_errors_total`
- `clustersentinel_http_requests_total{path,status}`

## RBAC model (least privilege)

ClusterRole (each environment):

- `apiGroups: [""]`, `resources: ["events"]`, `verbs: ["get","list","watch"]`
- Optional: `namespaces` `get/list` for agent context (no secrets, no write verbs)

No cluster-admin. No secret/read access. GitOps binds the ClusterRole to the environment's
ServiceAccount.

## Distribution

- Docker images are published at `chaser420/cluster-sentinel` with semantic version tags.
- The Helm repository is served from `https://chaser100.github.io/cluster-sentinel`.
- The chart contains the event RBAC, PrometheusRule, Grafana dashboard ConfigMap, and MCP Secret template.
- External routes must exclude `/metrics`; Prometheus should scrape it through the in-cluster ServiceMonitor.

## Out of scope

- Multi-cluster fan-out
- Shared infra (Vault HA, ESO platform, ArgoCD) changes

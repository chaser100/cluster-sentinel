# ClusterSentinel MCP contract

Transport: **streamable HTTP** at `http://<host>:8080/mcp` (primary).
Fallback: `--mcp-stdio` for local agent wiring (same tools/resources).

## Authentication (mandatory on HTTP)

Streamable HTTP `/mcp` **requires** `Authorization: Bearer <token>`. Auth cannot be
disabled in HTTP mode. Missing/invalid token → **401** + `WWW-Authenticate: Bearer`.

| Env | Required | Notes |
|-----|----------|-------|
| `CLUSTERSENTINEL_MCP_AUTH_TOKEN` | **yes** (HTTP) | Fail-fast on startup if empty. Never commit the value. |
| `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` | no | DNS-rebinding Host allow-list (comma-separated). |

`--mcp-stdio` does **not** require Bearer (local pipe, not network).

### How the token reaches the process

1. A Kubernetes `Secret` holds the Bearer value (key name configurable; default `token`).
2. The Deployment mounts it into the pod as `CLUSTERSENTINEL_MCP_AUTH_TOKEN` via `secretKeyRef`.
3. Callers (adapters, `curl`) send the **same** value in `Authorization: Bearer …`.

Provisioning options (pick one; do **not** put the plaintext token in Git):

| Mode | Behavior |
|------|----------|
| Helm-managed Secret | Chart creates the Secret once. On later syncs, `lookup` reuses the existing data — the token is **not** rotated every sync. |
| Pre-existing Secret | Point values at an already-managed Secret (`existingSecret`); the chart does not create one. |
| Manual / out-of-band | Create or rotate the Secret with `kubectl` (or ExternalSecret/Vault). Never commit the value. |

`/health` stays unauthenticated (probes). `/api/v1/events` requires the same Bearer
token as `/mcp`. `/metrics` stays unauthenticated for in-cluster Prometheus scrape
and **must not** be exposed on the external HTTPRoute (ServiceMonitor only).

### Host allow-list

Default allow-list covers loopback and in-cluster Service DNS forms. Override with
`CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` (comma-separated) to add public hostnames or
extra Service aliases.

A valid Bearer against a Host **not** on the list returns **403** (`Forbidden: Host
header is not allowed`) — clients often misread this as “auth broken”. Auth rejects
are **401** with empty body + `WWW-Authenticate: Bearer realm="clustersentinel-mcp"`.

### HTTP status cheat-sheet (`/mcp`)

| Code | Meaning | Typical cause |
|------|---------|---------------|
| **401** | Bearer rejected | missing/wrong `Authorization` |
| **403** | Host not allow-listed | request Host missing from `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` |
| **400** | Bad Request | `GET /mcp` without `Mcp-Session-Id`; or unsupported `MCP-Protocol-Version` |
| **404** | Session not found | `Mcp-Session-Id` unknown on this pod — **multi-replica + in-memory sessions**; keep `replicaCount: 1` (or sticky/shared store) |
| **200** | OK | `POST initialize` with valid Bearer + allowed Host; then `GET`/`POST` **with** `Mcp-Session-Id` |

### Smoke (never print the token)

```bash
NS=<namespace>
MCP_URL=<https-or-http-base>/mcp   # e.g. public URL or http://clustersentinel.<ns>.svc:8080/mcp
TOKEN="$(kubectl -n "${NS}" get secret clustersentinel-mcp-auth \
  -o jsonpath='{.data.token}' | base64 -d)"

# expect 401
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  "${MCP_URL}"

# expect non-401 (MCP session)
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0.0.1"}}}' \
  "${MCP_URL}"
unset TOKEN
```

Client flow (streamable HTTP):

1. `POST /mcp` `initialize` with `Accept: application/json, text/event-stream` + Bearer.
2. Read `Mcp-Session-Id` from the response headers.
3. Subsequent `GET` (SSE) / `POST` **must** send that session id. Do **not** open `GET /mcp` before initialize.

Server info: `name=clustersentinel`, `version` from `CLUSTERSENTINEL_BUILD_VERSION` (default crate version).

All tools are **read-only** / **idempotent** (`readOnlyHint=true`, `destructiveHint=false`,
`idempotentHint=true`, `openWorldHint=false`). Each tool declares `outputSchema` and returns
matching `structuredContent` JSON.

## Tools

### `get_health`

Extended health JSON:

- `status`: `starting` | `healthy` | `degraded` | `unhealthy` (from watch state)
- `watch_state`, `events_mode`, `registry_size`
- `started_at`, `uptime_seconds`, `last_event_at`, `last_watch_success_at`, `last_watch_error_at`
- `last_error`, `consecutive_failures`
- `configured_namespaces`, `registry_capacity`, `retention_seconds`
- `oldest_event_at`, `newest_event_at`, `build_version`, `git_sha`

Input: none.

### `list_recent_events`

List registered cluster events (newest `observed_at` first).

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `limit` | u32 | `50` | max `500` |
| `namespace` | string? | — | filter |
| `reason` | string? | — | exact match |
| `type_filter` | string? | — | `Normal` / `Warning`; other values return invalid params |

### `search_events`

Rich filter with opaque cursor pagination.

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `limit` | u32 | `50` | max `500` |
| `cursor` | string? | — | previous `next_cursor`; treat it as opaque |
| `since` / `until` | RFC3339? | — | filter by `observed_at` |
| `namespaces` / `types` / `reasons` | string[] | `[]` | any-of match; types are `Normal` / `Warning` |
| `involved_kind` / `involved_name` / `involved_uid` | string? | — | object filters (`name` substring) |
| `source_component` | string? | — | exact |
| `message_contains` | string? | — | case-insensitive |

Response: `events`, `matched`, `returned`, `truncated`, `next_cursor`, `generated_at`.

### `summarize_events`

Aggregate retained events.

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `since` / `until` | RFC3339? | — | window |
| `type` | string? | — | `Normal` / `Warning`; other values return invalid params |
| `group_by` | string[] | `namespace,reason,involved_kind` | also `type` |
| `limit` | u32 | `20` | max groups |

Each group: `event_objects`, `occurrences` (sum of Kubernetes `count`), `first_seen`, `last_seen`, `affected_objects`, `sample_message`.

### `get_event`

Fetch one event by Kubernetes UID.

| Field | Type | Required |
|-------|------|----------|
| `uid` | string | yes |

Returns JSON event or MCP error `not_found`.

### `get_metrics_summary`

Snapshot of registry + counters useful for agent triage (not full Prometheus text).

Input: none.

## Resources

| URI | Description |
|-----|-------------|
| `clustersentinel://status` | Same payload as `get_health` |
| `clustersentinel://events/recent` | Last 50 events as JSON array |

## Stability

- Tool names and resource URIs are part of the public contract.
- Additive fields in JSON responses are allowed; renames require a version bump.
- No write or mutation tools.

## Agent / adapter connection

Codex uses `bearer_token_env_var` for a streamable HTTP server:

```toml
[mcp_servers.clustersentinel]
url = "https://<clustersentinel-host>/mcp"
bearer_token_env_var = "CLUSTERSENTINEL_MCP_AUTH_TOKEN"
```

Export `CLUSTERSENTINEL_MCP_AUTH_TOKEN` before starting Codex. Do not put the token value in
`config.toml`. `codex mcp login` is for OAuth servers, not static Bearer authentication.

Generic adapter configuration uses an environment reference in the authorization header:

```json
{
  "mcpServers": {
    "clustersentinel": {
      "type": "streamable-http",
      "url": "https://<clustersentinel-host>/mcp",
      "headers": {
        "Authorization": "Bearer ${env:CLUSTERSENTINEL_MCP_AUTH_TOKEN}"
      }
    }
  }
}
```

In-cluster Service URL pattern: `http://clustersentinel.<namespace>.svc:8080/mcp`.

Tokens are **per environment/Secret** — do not assume one shared value across namespaces.
Rotate a token if any client or IDE writes its `Authorization` header to logs.

Paperclip (or similar) wiring:

1. Store the cluster Secret value in the MCP client's secret manager or environment; **never paste it into issues**.
2. Bind that secret to the agent(s) that call ClusterSentinel MCP.
3. Reference `${env:CLUSTERSENTINEL_MCP_AUTH_TOKEN}` (or the adapter’s env name) in `headers.Authorization`.

Local HTTP (demo) still needs a token:

```bash
export CLUSTERSENTINEL_EVENTS_MODE=demo
export CLUSTERSENTINEL_MCP_AUTH_TOKEN="$(openssl rand -hex 32)"
cargo run
curl -sS -H "Authorization: Bearer ${CLUSTERSENTINEL_MCP_AUTH_TOKEN}" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  http://127.0.0.1:8080/mcp
```

Local stdio fallback (no Bearer; laptop / CI without HTTP MCP client):

```bash
export CLUSTERSENTINEL_EVENTS_MODE=demo
cargo run -- --mcp-stdio
```

```json
{
  "mcpServers": {
    "clustersentinel": {
      "command": "cargo",
      "args": ["run", "--", "--mcp-stdio"],
      "env": {
        "CLUSTERSENTINEL_EVENTS_MODE": "demo"
      }
    }
  }
}
```

Assumption: adapters that speak MCP over streamable HTTP must send Bearer;
stdio is only for local adapters that cannot reach the Service.

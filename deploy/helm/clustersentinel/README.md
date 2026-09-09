<p align="center">
  <img src="https://chaser100.github.io/cluster-sentinel/logo.svg" width="160" alt="Cluster Sentinel logo">
</p>

# Cluster Sentinel

Cluster Sentinel watches Kubernetes `core/v1` Events and keeps a queryable history after the pod restarts. It combines a durable SQLite event store, Prometheus metrics and alerts, a Grafana dashboard, an authenticated HTTP API, and an embedded Model Context Protocol server in one deployment.

Use it when an event may disappear from the Kubernetes API before an engineer or an automation agent investigates the incident. The collector stores each event together with its cluster identity and persists watch checkpoints, so a restart does not reset the investigation window or force an unsafe watch resume.

## What the chart installs

| Component | Default | Purpose |
| --- | --- | --- |
| Cluster Sentinel Deployment | enabled | Lists and watches Kubernetes Events, writes SQLite, and serves HTTP on port `8080` |
| PersistentVolumeClaim | enabled, `5Gi`, `ReadWriteOnce` | Stores event history and watch checkpoints in `/var/lib/clustersentinel/events.db` |
| ServiceAccount and cluster RBAC | enabled | Grants read-only Event collection and Namespace discovery permissions |
| MCP authentication Secret | enabled | Stores the Bearer token used by `/mcp` and `/api/v1/events` |
| Grafana dashboard ConfigMap | enabled | Supplies the bundled dashboard to a Grafana sidecar |
| ServiceMonitor | disabled | Configures Prometheus Operator scraping for `/metrics` |
| PrometheusRule | disabled | Installs seven watch, storage, registry, and PVC alerts |

The application is a statically linked MUSL binary running on `distroless/static-debian13:nonroot`. The runtime image does not contain OpenSSL, glibc, GCC runtime libraries, a package manager, or a shell. The container runs as UID `65532`, drops all Linux capabilities, uses a read-only root filesystem, and writes only to the mounted data volume. The default `Recreate` deployment strategy and single replica prevent two processes from opening the same SQLite database.

## Prerequisites

- Kubernetes `1.23` or newer
- Helm 3
- a default StorageClass, or an explicit `clustersentinel.persistentVolumeClaims[0].storageClassName`
- Prometheus Operator CRDs when enabling `ServiceMonitor` or `PrometheusRule`
- a Grafana sidecar configured to discover ConfigMaps labeled `grafana_dashboard: "1"` when using the bundled dashboard

## Install

```bash
helm repo add cluster-sentinel https://chaser100.github.io/cluster-sentinel
helm repo update

helm upgrade --install clustersentinel cluster-sentinel/clustersentinel \
  --version 0.9.4 \
  --namespace clustersentinel \
  --create-namespace
```

Check the rollout and the storage claim:

```bash
kubectl --namespace clustersentinel rollout status deployment/clustersentinel
kubectl --namespace clustersentinel get pods,pvc
```

The default installation generates an MCP token once and reuses it during Helm upgrades. Export it without printing it, then forward the Service from another terminal:

```bash
export CLUSTERSENTINEL_MCP_AUTH_TOKEN="$(
  kubectl --namespace clustersentinel get secret clustersentinel-mcp-auth \
    --output jsonpath='{.data.token}' | base64 --decode
)"

kubectl --namespace clustersentinel port-forward service/clustersentinel 8080:8080
```

Verify the public probes and authenticated event API:

```bash
curl --fail --silent http://127.0.0.1:8080/health | jq
curl --fail --silent http://127.0.0.1:8080/ready | jq
curl --fail --silent \
  --header "Authorization: Bearer ${CLUSTERSENTINEL_MCP_AUTH_TOKEN}" \
  "http://127.0.0.1:8080/api/v1/events?limit=20&type=Warning" | jq
```

## Data flow and restart behavior

```text
Kubernetes Events
       |
       v
list/watch -> bounded writer queue -> SQLite event + checkpoint transaction
                                         |
                    +--------------------+-------------------+
                    |                    |                   |
                    v                    v                   v
              HTTP event API         MCP tools       in-memory hot cache
                                                              |
                                                              v
                                                    Prometheus metrics
```

During the initial paginated list, Cluster Sentinel persists events first and commits the list snapshot checkpoint only after every page is durable. During watch processing, each event and its resource version are committed in one transaction before the in-memory cache is updated. On restart, the collector restores recent events from SQLite and resumes each watch scope from its last checkpoint.

Retention pruning runs at startup and every five minutes. It removes expired rows and then enforces the maximum row count in bounded transactions.

## Persistent event storage

The default PVC has both `helm.sh/resource-policy: keep` and `argocd.argoproj.io/sync-options: Prune=false`. Helm uninstall and Argo CD pruning therefore leave the event database behind unless an operator deletes the claim explicitly.

Override capacity, StorageClass, cluster identity, or retention with values such as:

```yaml
clustersentinel:
  persistentVolumeClaims:
    - name: clustersentinel-data
      size: 20Gi
      storageClassName: fast-ssd
      accessModes:
        - ReadWriteOnce
      mountPath: /var/lib/clustersentinel
      readOnly: false
      annotations:
        helm.sh/resource-policy: keep
        argocd.argoproj.io/sync-options: Prune=false

  env:
    - name: CLUSTERSENTINEL_STORAGE_PATH
      value: /var/lib/clustersentinel/events.db
    - name: CLUSTERSENTINEL_CLUSTER_ID
      value: production-eu-1
    - name: CLUSTERSENTINEL_STORAGE_RETENTION_SECS
      value: "1209600"
    - name: CLUSTERSENTINEL_STORAGE_MAX_EVENTS
      value: "500000"
```

Keep `clustersentinel.replicaCount: 1` and `clustersentinel.deploymentStrategy.type: Recreate` when using SQLite on the default RWO claim. The pod security context sets `fsGroup: 65532` and `fsGroupChangePolicy: OnRootMismatch`; the CSI driver must support Kubernetes volume ownership management or provision an equivalent writable directory.

For an intentionally ephemeral deployment:

```yaml
clustersentinel:
  persistentVolumeClaims: []
  env:
    - name: CLUSTERSENTINEL_STORAGE_PATH
      value: memory
```

An ephemeral deployment loses events and checkpoints on every restart.

## Prometheus and Grafana

Enable Prometheus Operator resources and set labels that match your Prometheus rule and ServiceMonitor selectors:

```yaml
prometheusRule:
  enabled: true
  labels:
    release: kube-prometheus-stack

clustersentinel:
  serviceMonitor:
    enabled: true
    labels:
      release: kube-prometheus-stack
```

The dashboard ConfigMap is enabled by default. Change `grafanaDashboard.labels` when your Grafana sidecar uses another selector.

The bundled rules cover:

- repeated Kubernetes watch failures and restarts;
- an empty in-memory registry;
- SQLite write errors and stalled writes;
- stale watch checkpoints;
- high filesystem usage on the Cluster Sentinel PVC.

`/metrics` includes Kubernetes object names and event message labels. Keep this endpoint inside the cluster and apply the same access controls used for other operational telemetry.

## MCP and HTTP API

| Endpoint | Authentication | Purpose |
| --- | --- | --- |
| `/health` | none | Process, watch, registry, and storage health details |
| `/ready` | none | Kubernetes readiness; fails when the collector or storage is not ready |
| `/metrics` | none | Prometheus exposition endpoint |
| `/api/v1/events` | Bearer token | Filtered, paginated event history from SQLite |
| `/mcp` | Bearer token and allowed `Host` | Streamable HTTP MCP server |

The MCP server provides health, recent-event, search, summary, single-event, and metrics-summary tools. Event search uses a stable versioned keyset cursor; deprecated decimal offsets remain accepted for older clients. See the complete [MCP contract](https://github.com/chaser100/cluster-sentinel/blob/main/docs/mcp.md) for tool schemas and client examples.

MCP validates the HTTP `Host` or `:authority` value to reduce DNS-rebinding risk. Add every externally used hostname to `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS`. A valid Bearer token with an unlisted host still receives `403`.

## Use an existing Secret

Disable Secret management when Vault, External Secrets Operator, or another controller owns the token:

```yaml
mcpAuth:
  manageSecret: false
  existingSecret: my-clustersentinel-token

clustersentinel:
  envSecrets:
    enableEnv: true
    envs:
      - name: CLUSTERSENTINEL_MCP_AUTH_TOKEN
        secretName: my-clustersentinel-token
        secretKey: token
```

The referenced key must contain a non-empty token. If you change `mcpAuth.secretKey`, change `clustersentinel.envSecrets.envs[].secretKey` to the same key.

## Limit collection to selected namespaces

Set a comma-separated namespace allow-list. An empty value watches the entire cluster.

```yaml
clustersentinel:
  env:
    - name: CLUSTERSENTINEL_NAMESPACES
      value: production,ingress-nginx
    - name: CLUSTERSENTINEL_CLUSTER_ID
      value: production-eu-1
```

The supplied ClusterRole remains cluster-wide and read-only. Set `rbac.create: false` and provide your own RBAC objects when the collector must not have permission outside those namespaces.

## Expose the API

The Universal Helm Chart dependency supports Gateway API `HTTPRoute` and Ingress. Keep `/metrics` private. Expose `/mcp` and `/api/v1/events` only through a proxy that preserves the expected hostname and does not log the Authorization header.

Example `HTTPRoute` configuration:

```yaml
clustersentinel:
  route:
    enabled: true
    spec:
      parentRefs:
        - name: external
          namespace: gateway-system
      hostnames:
        - clustersentinel.example.com
      rules:
        - matches:
            - path:
                type: Exact
                value: /health
            - path:
                type: Exact
                value: /ready
            - path:
                type: PathPrefix
                value: /mcp
            - path:
                type: PathPrefix
                value: /api/v1/events
          backendRefs:
            - name: clustersentinel
              port: 8080

  env:
    - name: CLUSTERSENTINEL_MCP_ALLOWED_HOSTS
      value: localhost,127.0.0.1,::1,clustersentinel,clustersentinel.example.com
```

Gateway API and Prometheus Operator CRDs are not installed by this chart.

## Main values

| Value | Default | Description |
| --- | --- | --- |
| `mcpAuth.manageSecret` | `true` | Create and retain the MCP Bearer Secret |
| `mcpAuth.existingSecret` | `""` | Use an externally managed Secret |
| `mcpAuth.secretName` | `clustersentinel-mcp-auth` | Name of the managed or default referenced Secret |
| `mcpAuth.secretKey` | `token` | Secret data key containing the Bearer token |
| `rbac.create` | `true` | Create cluster-wide read-only Event and Namespace permissions |
| `grafanaDashboard.enabled` | `true` | Create the Grafana sidecar dashboard ConfigMap |
| `grafanaDashboard.labels` | `grafana_dashboard: "1"` | Labels used for dashboard discovery |
| `prometheusRule.enabled` | `false` | Create the bundled PrometheusRule |
| `prometheusRule.labels` | `release: kube-prometheus-stack` | Labels used by the Prometheus rule selector |
| `clustersentinel.image` | `chaser420/cluster-sentinel` | Container image repository |
| `clustersentinel.imageTag` | `0.9.4` | Container image tag; release tags match the chart version |
| `clustersentinel.replicaCount` | `1` | Replica count; keep one replica with the default SQLite database |
| `clustersentinel.deploymentStrategy.type` | `Recreate` | Prevent concurrent access to the RWO SQLite volume during upgrades |
| `clustersentinel.persistentVolumeClaims` | `clustersentinel-data`, `5Gi`, `ReadWriteOnce` | Create and mount durable SQLite storage |
| `clustersentinel.readinessProbe.httpGet.path` | `/ready` | Remove the pod from Service endpoints when durable storage is unavailable |
| `clustersentinel.livenessProbe.httpGet.path` | `/health` | Restart a process whose health endpoint stops responding |
| `clustersentinel.serviceMonitor.enabled` | `false` | Create a Prometheus Operator ServiceMonitor |
| `clustersentinel.resources.requests` | `50m`, `128Mi` | Default CPU and memory requests |
| `clustersentinel.resources.limits` | `500m`, `512Mi` | Default CPU and memory limits |

`clustersentinel` is the alias of the bundled Universal Helm Chart dependency. Its workload, scheduling, Service, Ingress, Gateway API, ServiceMonitor, and security values remain available under this key. See the [Universal Helm Chart values](https://github.com/chaser100/u-helm-chart/tree/main/helm-charts/application) for additional options.

## Runtime environment variables

| Variable | Application default | Purpose |
| --- | --- | --- |
| `CLUSTERSENTINEL_BIND` | `0.0.0.0:8080` | HTTP listen address; the Service and probes must use the same port |
| `CLUSTERSENTINEL_EVENTS_MODE` | `kubernetes` | Event source; `kubernetes` and `k8s` use the Kubernetes API, while `demo` generates test events |
| `CLUSTERSENTINEL_LIST_LIMIT` | `500` | Page size for the initial Kubernetes Events list |
| `CLUSTERSENTINEL_WATCH_TIMEOUT_SECS` | `290` | Kubernetes watch timeout; it must remain below the client limit of 295 seconds |
| `CLUSTERSENTINEL_WATCH_BACKOFF_SECS` | `5` | Initial retry delay after a list or watch failure |
| `CLUSTERSENTINEL_WATCH_BACKOFF_MAX_SECS` | `60` | Maximum exponential retry delay; must be at least the initial delay |
| `CLUSTERSENTINEL_REGISTRY_CAPACITY` | `10000` | Maximum deduplicated event objects retained in the in-memory hot cache |
| `CLUSTERSENTINEL_DEDUP_TTL_SECS` | `3600` | Inactivity period before an event leaves the in-memory cache |
| `CLUSTERSENTINEL_METRICS_EVENT_LIMIT` | `500` | Maximum retained events considered when building inventory gauge series; identical exported labels share one series with the latest timestamp |
| `CLUSTERSENTINEL_NAMESPACES` | all namespaces | Comma-separated namespace allow-list |
| `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` | loopback, `clustersentinel`, and Service DNS names | Comma-separated `Host` and `:authority` allow-list for the MCP endpoint |
| `CLUSTERSENTINEL_MCP_AUTH_TOKEN` | no default | Bearer token required by `/mcp` and `/api/v1/events` |
| `CLUSTERSENTINEL_STORAGE_PATH` | `/var/lib/clustersentinel/events.db` in Kubernetes | SQLite path; `memory` or `:memory:` disables file persistence |
| `CLUSTERSENTINEL_CLUSTER_ID` | `default` | Stable logical cluster identifier stored with events and checkpoints |
| `CLUSTERSENTINEL_STORAGE_RETENTION_SECS` | `604800` | Maximum event age; the default is seven days |
| `CLUSTERSENTINEL_STORAGE_MAX_EVENTS` | `250000` | Maximum durable event rows after pruning |
| `CLUSTERSENTINEL_STORAGE_PRUNE_BATCH` | `1000` | Maximum rows deleted per pruning transaction |
| `CLUSTERSENTINEL_WRITER_QUEUE_CAPACITY` | `64` | Bounded persistence queue; producers wait when it is full |
| `CLUSTERSENTINEL_BUILD_VERSION` | crate version | Build version reported by health and MCP server information |
| `CLUSTERSENTINEL_GIT_SHA` | `unknown` | Git revision reported by health output |
| `RUST_LOG` | `info` in chart values | Rust tracing filter, for example `debug` or `info,clustersentinel::mcp=debug` |

`clustersentinel.env` is an array. A values override replaces the complete array instead of merging entries. Omitted variables use the application defaults shown above.

## Upgrade and uninstall

Review values and rendered manifests before an upgrade:

```bash
helm repo update
helm show values cluster-sentinel/clustersentinel --version 0.9.4 > values-0.9.4.yaml
helm template clustersentinel cluster-sentinel/clustersentinel \
  --version 0.9.4 \
  --namespace clustersentinel \
  --values my-values.yaml > rendered.yaml
```

The `0.9.2` upgrade creates the first persistent store for installations coming from `0.9.1`; there is no older on-disk schema to migrate. Keep `CLUSTERSENTINEL_CLUSTER_ID` stable across upgrades and restores.

```bash
helm upgrade clustersentinel cluster-sentinel/clustersentinel \
  --version 0.9.4 \
  --namespace clustersentinel \
  --values my-values.yaml
```

Uninstalling the release does not remove the retained PVC:

```bash
helm uninstall clustersentinel --namespace clustersentinel
kubectl --namespace clustersentinel get pvc clustersentinel-data
```

Delete the PVC separately only when its event history is no longer needed.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Pod stays unready | Read `/ready`, inspect pod logs, and verify that the PVC is bound and writable by GID `65532` |
| Event API returns `401` | Supply `Authorization: Bearer <token>` from the configured Secret |
| MCP returns `403` with a valid token | Add the request hostname to `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` |
| No events appear | Check RBAC, `CLUSTERSENTINEL_NAMESPACES`, watch errors in logs, and `clustersentinel_watch_errors_total` |
| ServiceMonitor has no targets | Match `clustersentinel.serviceMonitor.labels` to the Prometheus selector |
| Dashboard is missing | Match `grafanaDashboard.labels` to the Grafana sidecar selector |
| PVC usage keeps growing | Reduce retention or row limits and monitor `ClusterSentinelStoragePvcHighUsage` |

Useful commands:

```bash
kubectl --namespace clustersentinel describe pod -l app.kubernetes.io/name=clustersentinel
kubectl --namespace clustersentinel logs deployment/clustersentinel
kubectl auth can-i list events --all-namespaces \
  --as system:serviceaccount:clustersentinel:clustersentinel
```

## Packaging

The complete `application:0.4.1` dependency is bundled under `charts/application/` in the Git repository and release archive. Installation does not require `helm dependency build`, `helm dependency update`, or access to the upstream dependency repository.

Source, architecture notes, alert rules, dashboard JSON, and release checks are available in the [Cluster Sentinel repository](https://github.com/chaser100/cluster-sentinel).

# Cluster Sentinel

[![Docker image](https://img.shields.io/docker/v/chaser420/cluster-sentinel?sort=semver&label=Docker%20Hub)](https://hub.docker.com/r/chaser420/cluster-sentinel)
[![Helm chart](https://img.shields.io/badge/Helm-0.9.5-0f1689)](https://chaser100.github.io/cluster-sentinel/index.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Cluster Sentinel watches Kubernetes Events, persists event history and watch checkpoints in SQLite, and keeps a bounded in-memory read cache. It exposes Prometheus metrics, a read-only HTTP API, and an embedded MCP server for operators and agents.

The project includes a Docker image, a Helm chart, Prometheus alert rules, and a Grafana dashboard.

## Endpoints

| Endpoint | Authentication | Purpose |
| --- | --- | --- |
| `/health` | none | Liveness, watcher state, storage state, and build information |
| `/ready` | none | Readiness; returns `503` while durable storage is unavailable |
| `/metrics` | none | Prometheus metrics; keep this endpoint inside the cluster |
| `/api/v1/events` | Bearer token | Filtered event inventory |
| `/mcp` | Bearer token | Streamable HTTP MCP server |

HTTP MCP and the events API require `CLUSTERSENTINEL_MCP_AUTH_TOKEN`. MCP also validates the request `Host` header to reduce DNS-rebinding risk.

## Run locally

The demo backend generates events without connecting to Kubernetes:

```bash
export CLUSTERSENTINEL_EVENTS_MODE=demo
export CLUSTERSENTINEL_MCP_AUTH_TOKEN="$(openssl rand -hex 32)"
cargo run
```

In another terminal:

```bash
curl --fail --silent http://127.0.0.1:8080/health
curl --fail --silent http://127.0.0.1:8080/metrics | head
curl --fail --silent \
  --header "Authorization: Bearer ${CLUSTERSENTINEL_MCP_AUTH_TOKEN}" \
  "http://127.0.0.1:8080/api/v1/events?limit=20"
```

The stdio transport does not use HTTP Bearer authentication:

```bash
cargo run -- --mcp-stdio
```

See [MCP configuration](docs/mcp.md) for client examples and [architecture](docs/architecture.md) for the event pipeline and environment variables.

## Docker

Release images are published to Docker Hub with the same version as the Helm chart:

```bash
docker pull chaser420/cluster-sentinel:0.9.5
```

Run the demo image locally:

```bash
docker run --rm \
  --publish 8080:8080 \
  --env CLUSTERSENTINEL_EVENTS_MODE=demo \
  --env CLUSTERSENTINEL_MCP_AUTH_TOKEN="$(openssl rand -hex 32)" \
  chaser420/cluster-sentinel:0.9.5
```

## Helm installation

Install Cluster Sentinel from the official Helm repository:

```bash
helm repo add cluster-sentinel https://chaser100.github.io/cluster-sentinel
helm repo update

helm upgrade --install clustersentinel cluster-sentinel/clustersentinel \
  --version 0.9.5 \
  --namespace clustersentinel \
  --create-namespace
```

The default installation creates one replica, a 5 GiB `ReadWriteOnce` PVC, a ClusterIP Service, read-only cluster RBAC, a Grafana dashboard `ConfigMap`, and a retained Secret with a generated MCP token. The application runs as UID `65532` with a read-only root filesystem and all Linux capabilities dropped.

Prometheus Operator resources are opt-in because their CRDs are not present in every cluster:

```bash
helm upgrade --install clustersentinel cluster-sentinel/clustersentinel \
  --version 0.9.5 \
  --namespace clustersentinel \
  --create-namespace \
  --set clustersentinel.serviceMonitor.enabled=true \
  --set prometheusRule.enabled=true
```

For an externally reachable MCP endpoint, add the public hostname to `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` and expose only `/health`, `/mcp`, and `/api/v1/events`. Do not expose `/metrics`; inventory labels can contain Kubernetes object names and event messages.

### Values structure

The chart has three configuration levels:

1. Top-level values configure resources owned by the Cluster Sentinel chart: MCP authentication, RBAC, Prometheus rules, and the Grafana dashboard.
2. Values under `clustersentinel` configure the Deployment, Service, ServiceAccount, probes, resources, routing, and ServiceMonitor.
3. Entries in `clustersentinel.env` become environment variables in the application container.

The chart includes [`values.schema.json`](deploy/helm/clustersentinel/values.schema.json). Commands such as `helm lint`, `helm install`, and `helm upgrade` reject invalid types and values before rendering Kubernetes resources.

### Chart-owned values

| Value | Default | Purpose |
| --- | --- | --- |
| `mcpAuth.manageSecret` | `true` | Creates the MCP Bearer token Secret. Set to `false` when the Secret is managed outside Helm. |
| `mcpAuth.existingSecret` | `""` | Identifies an externally managed Secret. When set, the chart does not generate a Secret. |
| `mcpAuth.secretName` | `clustersentinel-mcp-auth` | Name of the chart-managed Secret. |
| `mcpAuth.secretKey` | `token` | Secret data key containing the Bearer token. |
| `rbac.create` | `true` | Creates a ClusterRole and ClusterRoleBinding with read-only access to Events and Namespaces. |
| `prometheusRule.enabled` | `false` | Creates the bundled Prometheus alert rules. The Prometheus Operator CRDs must already exist. |
| `prometheusRule.labels` | `release: kube-prometheus-stack` | Labels used by the Prometheus rule selector. Change them to match the Prometheus installation. |
| `grafanaDashboard.enabled` | `true` | Creates the bundled Grafana dashboard ConfigMap. |
| `grafanaDashboard.labels` | `grafana_dashboard: "1"` | Labels used by the Grafana dashboard sidecar to discover the ConfigMap. |

The generated Secret has `helm.sh/resource-policy: keep`, so `helm uninstall` does not delete it. On an upgrade, Helm reuses the existing token through `lookup` instead of rotating it.

Read the generated token with:

```bash
kubectl --namespace clustersentinel get secret clustersentinel-mcp-auth \
  --output jsonpath='{.data.token}' | base64 --decode
echo
```

### Workload values

| Value | Default | Purpose |
| --- | --- | --- |
| `clustersentinel.fullnameOverride` | `clustersentinel` | Keeps Deployment, Service, and ServiceAccount names stable. |
| `clustersentinel.replicaCount` | `1` | Number of application pods. Keep one replica while MCP sessions are stored in memory. |
| `clustersentinel.image` | `chaser420/cluster-sentinel` | Container image repository. |
| `clustersentinel.imageTag` | `0.9.5` | Container image version. Release tags, chart versions, and this value must match. |
| `clustersentinel.imagePullPolicy` | `IfNotPresent` | Kubernetes image pull policy. |
| `clustersentinel.imagePullSecrets` | `[]` | Secret references required by a private container registry. Each item uses the form `name: secret-name`. |
| `clustersentinel.service.name` | `http` | Service port name used by probes and ServiceMonitor. |
| `clustersentinel.service.type` | `ClusterIP` | Kubernetes Service type. |
| `clustersentinel.service.port` | `8080` | Service port for the HTTP API, metrics, and MCP endpoint. |
| `clustersentinel.service.protocol` | `TCP` | Service port protocol. |
| `clustersentinel.route.enabled` | `false` | Creates a Gateway API HTTPRoute through the Universal Helm Chart. |
| `clustersentinel.route.spec` | not set | HTTPRoute specification, including `parentRefs`, `hostnames`, rules, and backends. |
| `clustersentinel.deploymentStrategy.type` | `Recreate` | Stops the old pod before starting the replacement so one SQLite database is never opened by two pods. |
| `clustersentinel.persistentVolumeClaims` | `clustersentinel-data`, `5Gi`, `ReadWriteOnce` | Creates and mounts the SQLite data volume at `/var/lib/clustersentinel`. An empty `storageClassName` uses the cluster default. |
| `clustersentinel.readinessProbe` | `/ready` | Removes the pod from Service endpoints when durable storage is unavailable. |
| `clustersentinel.livenessProbe` | `/health` | Restarts the container when the HTTP server stops responding. |
| `clustersentinel.resources.requests` | `50m`, `128Mi` | CPU and memory reserved for each pod. |
| `clustersentinel.resources.limits` | `500m`, `512Mi` | Maximum CPU and memory available to each pod. |
| `clustersentinel.serviceAccount.create` | `true` | Creates the ServiceAccount used by the Deployment and ClusterRoleBinding. |
| `clustersentinel.serviceAccount.automount` | `true` | Mounts the Kubernetes API token required by the event watcher. |
| `clustersentinel.serviceAccount.name` | `clustersentinel` | ServiceAccount name referenced by the Deployment and RBAC templates. |
| `clustersentinel.serviceMonitor.enabled` | `false` | Creates a ServiceMonitor. The Prometheus Operator CRDs must already exist. |
| `clustersentinel.serviceMonitor.endpoints` | `/metrics`, `30s` | Configures the metrics path, scrape interval, and timeout. |
| `clustersentinel.securityContext` | restricted | Runs the container without privilege escalation, capabilities, or a writable root filesystem. |
| `clustersentinel.podSecurityContext` | `fsGroup: 65532`, `RuntimeDefault` seccomp | Makes the mounted PVC writable by the non-root process and applies the default runtime syscall profile. |
| `clustersentinel.envSecrets` | MCP token reference | Maps Secret keys to container environment variables. |
| `clustersentinel.env` | runtime defaults | Supplies non-secret application environment variables. |

Run `helm show values cluster-sentinel/clustersentinel --version 0.9.5` to view the complete configuration.

### Persistent event storage

Version `0.9.2` stores event history and Kubernetes watch checkpoints in SQLite. The default chart creates `PersistentVolumeClaim/clustersentinel-data`, mounts it at `/var/lib/clustersentinel`, and writes `/var/lib/clustersentinel/events.db`. The PVC uses the cluster's default StorageClass unless `storageClassName` is set explicitly.

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

Keep `replicaCount: 1` and `deploymentStrategy.type: Recreate`. SQLite and the default `ReadWriteOnce` claim are not a shared multi-writer backend. Back up the PVC before destructive storage changes. The default claim carries Helm `keep` and Argo CD `Prune=false` annotations, so removing the release does not erase event history automatically; delete the PVC explicitly when the data is no longer needed.

To run without durable storage, replace the complete `clustersentinel.persistentVolumeClaims` and `clustersentinel.env` arrays:

```yaml
clustersentinel:
  persistentVolumeClaims: []
  env:
    - name: CLUSTERSENTINEL_STORAGE_PATH
      value: memory
```

This mode loses all events and checkpoints when the pod restarts. During an upgrade from `0.9.1`, Helm creates the PVC before the `0.9.2` pod starts; there is no older on-disk schema to migrate.

### Runtime environment variables

| Variable | Application default | Purpose |
| --- | --- | --- |
| `CLUSTERSENTINEL_BIND` | `0.0.0.0:8080` | Address and port used by the HTTP server. The Service and probes must target the same port. |
| `CLUSTERSENTINEL_EVENTS_MODE` | `kubernetes` | Event source. Use `kubernetes` or `k8s` in a cluster and `demo` for generated local or CI events. |
| `CLUSTERSENTINEL_LIST_LIMIT` | `500` | Page size used during the initial Kubernetes Events list operation. |
| `CLUSTERSENTINEL_WATCH_TIMEOUT_SECS` | `290` | Kubernetes watch request timeout. It must remain below the client limit of 295 seconds. |
| `CLUSTERSENTINEL_WATCH_BACKOFF_SECS` | `5` | Initial retry delay after list or watch failures. |
| `CLUSTERSENTINEL_WATCH_BACKOFF_MAX_SECS` | `60` | Maximum exponential retry delay. It must be greater than or equal to the initial delay. |
| `CLUSTERSENTINEL_REGISTRY_CAPACITY` | `10000` | Maximum number of deduplicated event objects retained in memory. |
| `CLUSTERSENTINEL_DEDUP_TTL_SECS` | `3600` | Time an inactive event remains in the in-memory registry. |
| `CLUSTERSENTINEL_METRICS_EVENT_LIMIT` | `500` | Maximum retained events considered when building inventory gauge series on `/metrics`. Events with identical exported labels share one series with the latest timestamp. |
| `CLUSTERSENTINEL_NAMESPACES` | all namespaces | Comma-separated namespace allow-list, for example `default,kube-system`. An empty value watches the whole cluster. |
| `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` | loopback, `clustersentinel`, and standard Service DNS forms | Comma-separated `Host` and `:authority` allow-list used by MCP DNS-rebinding protection. Add namespace-qualified Service DNS names and external hostnames used by clients. |
| `CLUSTERSENTINEL_MCP_AUTH_TOKEN` | no default | Bearer token required by `/mcp` and `/api/v1/events`. The default chart obtains it from the MCP Secret. |
| `CLUSTERSENTINEL_STORAGE_PATH` | `/var/lib/clustersentinel/events.db` in Kubernetes; memory in demo mode | SQLite database path. Use `memory` or `:memory:` to disable file-backed persistence. |
| `CLUSTERSENTINEL_CLUSTER_ID` | `default` | Logical cluster identifier stored with every event and watch checkpoint. Use a stable, unique value when a database is restored or shared across cluster identities. |
| `CLUSTERSENTINEL_STORAGE_RETENTION_SECS` | `604800` | Maximum event age before batched pruning; default is seven days. |
| `CLUSTERSENTINEL_STORAGE_MAX_EVENTS` | `250000` | Maximum durable event rows retained after pruning. |
| `CLUSTERSENTINEL_STORAGE_PRUNE_BATCH` | `1000` | Maximum rows removed per transaction during startup and five-minute periodic pruning. |
| `CLUSTERSENTINEL_WRITER_QUEUE_CAPACITY` | `64` | Bounded persistence queue capacity; producers wait when the queue is full. |
| `CLUSTERSENTINEL_BUILD_VERSION` | crate version | Version returned in health output and MCP server information. Release images set it during the Docker build. |
| `CLUSTERSENTINEL_GIT_SHA` | `unknown` | Git revision returned by the health endpoint. Release images set it during the Docker build. |
| `RUST_LOG` | `info` in chart values | Tracing filter. Examples: `debug` or `info,clustersentinel::mcp=debug`. |

`clustersentinel.env` is a YAML array. Helm replaces the complete array when it is overridden; it does not append entries. Variables omitted from the array use the application defaults shown above.

This example limits collection to two namespaces, reduces inventory metric cardinality, and allows an external MCP hostname:

```yaml
clustersentinel:
  env:
    - name: CLUSTERSENTINEL_EVENTS_MODE
      value: kubernetes
    - name: CLUSTERSENTINEL_NAMESPACES
      value: default,kube-system
    - name: CLUSTERSENTINEL_METRICS_EVENT_LIMIT
      value: "200"
    - name: CLUSTERSENTINEL_MCP_ALLOWED_HOSTS
      value: localhost,127.0.0.1,::1,clustersentinel,clustersentinel.example.com
    - name: RUST_LOG
      value: info
```

### Existing MCP secret

When another controller or an operator manages the token, set the Secret reference and map its key to the application environment variable.

```yaml
mcpAuth:
  manageSecret: false
  existingSecret: clustersentinel-external-auth

clustersentinel:
  envSecrets:
    enableEnv: true
    envs:
      - name: CLUSTERSENTINEL_MCP_AUTH_TOKEN
        secretName: clustersentinel-external-auth
        secretKey: token
```

The Secret key must contain a non-empty token. Do not place the token directly in `values.yaml`.

The complete chart configuration and Gateway API example are available in [the chart README](deploy/helm/clustersentinel/README.md).

## Observability

Cluster Sentinel exposes Prometheus metrics at `/metrics`. The Helm chart can create a `ServiceMonitor` and installs alert rules for watcher failures, storage errors, registry pressure, and PVC capacity when `prometheusRule.enabled` is enabled.

The bundled Grafana dashboard shows event rates, warning reasons, watcher state, storage health, retention activity, and PVC usage. It is published as a labeled `ConfigMap` for discovery by the Grafana sidecar.

## Documentation

- [MCP client configuration](docs/mcp.md)
- [Architecture and event flow](docs/architecture.md)
- [Helm chart configuration](deploy/helm/clustersentinel/README.md)
- [Release history](CHANGELOG.md)

## License

Cluster Sentinel is licensed under the [MIT License](LICENSE).

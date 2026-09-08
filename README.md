# Cluster Sentinel

[![CI](https://github.com/chaser100/cluster-sentinel/actions/workflows/ci.yml/badge.svg)](https://github.com/chaser100/cluster-sentinel/actions/workflows/ci.yml)
[![Docker image](https://img.shields.io/docker/v/chaser420/cluster-sentinel?sort=semver&label=Docker%20Hub)](https://hub.docker.com/r/chaser420/cluster-sentinel)
[![Helm chart](https://img.shields.io/badge/Helm-0.9.3-0f1689)](https://chaser100.github.io/cluster-sentinel/index.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Cluster Sentinel watches Kubernetes Events, persists event history and watch checkpoints in SQLite, and keeps a bounded in-memory read cache. It exposes Prometheus metrics, a read-only HTTP API, and an embedded MCP server for operators and agents.

The repository also ships alert rules, a Grafana dashboard, a Docker image, and a Helm chart built on [Universal Helm Chart](https://github.com/chaser100/u-helm-chart).

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
docker pull chaser420/cluster-sentinel:0.9.3
```

Run the demo image locally:

```bash
docker run --rm \
  --publish 8080:8080 \
  --env CLUSTERSENTINEL_EVENTS_MODE=demo \
  --env CLUSTERSENTINEL_MCP_AUTH_TOKEN="$(openssl rand -hex 32)" \
  chaser420/cluster-sentinel:0.9.3
```

## Helm installation

The chart repository is served by GitHub Pages. The release archive includes the `application` dependency; installation does not require adding the Universal Helm Chart repository or running `helm dependency update`.

```bash
helm repo add cluster-sentinel https://chaser100.github.io/cluster-sentinel
helm repo update

helm upgrade --install clustersentinel cluster-sentinel/clustersentinel \
  --version 0.9.3 \
  --namespace clustersentinel \
  --create-namespace
```

The default installation creates one replica, a 5 GiB `ReadWriteOnce` PVC, a ClusterIP Service, read-only cluster RBAC, a Grafana dashboard `ConfigMap`, and a retained Secret with a generated MCP token. The application runs as UID `65532` with a read-only root filesystem and all Linux capabilities dropped.

Prometheus Operator resources are opt-in because their CRDs are not present in every cluster:

```bash
helm upgrade --install clustersentinel cluster-sentinel/clustersentinel \
  --version 0.9.3 \
  --namespace clustersentinel \
  --create-namespace \
  --set clustersentinel.serviceMonitor.enabled=true \
  --set prometheusRule.enabled=true
```

For an externally reachable MCP endpoint, add the public hostname to `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS` and expose only `/health`, `/mcp`, and `/api/v1/events`. Do not expose `/metrics`; inventory labels can contain Kubernetes object names and event messages.

### Values structure

The chart has three configuration levels:

1. Top-level values configure resources owned by the Cluster Sentinel chart: MCP authentication, RBAC, Prometheus rules, and the Grafana dashboard.
2. Values under `clustersentinel` are passed to the aliased Universal Helm Chart dependency. They configure the Deployment, Service, ServiceAccount, probes, resources, routing, and ServiceMonitor.
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
| `clustersentinel.imageTag` | `0.9.3` | Container image version. Release tags, chart versions, and this value must match. |
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

Other values supported by the dependency can also be placed under `clustersentinel`. See the [Universal Helm Chart values](https://github.com/chaser100/u-helm-chart/tree/main/helm-charts/application) for Ingress, autoscaling, volumes, extra containers, annotations, and scheduling options.

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
| `CLUSTERSENTINEL_METRICS_EVENT_LIMIT` | `500` | Maximum number of retained events exported as per-event inventory gauges on `/metrics`. Lower it to reduce Prometheus cardinality. |
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

### Existing MCP Secret

When another controller or an operator manages the token, set both the chart-level Secret reference and the environment mapping. The two values are separate because the Deployment is rendered by the Universal Helm Chart dependency.

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

## Observability files

- `observability/alerts/clustersentinel.yaml` contains the Prometheus alert rules.
- `observability/dashboards/clustersentinel.json` is the Grafana dashboard bundled with the chart.

CI checks that the alert and dashboard files bundled in the chart match these source files.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --locked

helm lint deploy/helm/clustersentinel --strict
helm template clustersentinel deploy/helm/clustersentinel \
  --namespace clustersentinel \
  --values deploy/helm/clustersentinel/tests/values-observability.yaml

helm package deploy/helm/clustersentinel --destination /tmp
helm template clustersentinel /tmp/clustersentinel-0.9.3.tgz \
  --namespace clustersentinel
```

### Bundled dependency

The upstream `application:0.4.1` chart is committed under `deploy/helm/clustersentinel/charts/application/`. Both a fresh checkout and the published archive can be rendered without downloading that dependency. `Chart.yaml` retains the upstream URL and alias `clustersentinel`; `Chart.lock` records the dependency version.

Dependency updates are deliberate release changes. Download the chosen upstream release into a temporary directory, verify its archive digest against the upstream repository index, and replace the complete `charts/application/` directory with its extracted contents. Update the dependency declaration and regenerate `Chart.lock` in a temporary working copy. Commit the directory and lock file together, review the upstream changes, and bump the Cluster Sentinel release version. Do not leave an additional `application-*.tgz` in `charts/` alongside the extracted chart.

The local action `.github/actions/validate-chart` runs in CI, release validation, and chart publication after Helm setup. It checks dependency metadata, bundled observability files, schema validation, and rendering from the packaged chart with an empty Helm configuration. Test values cover Prometheus Operator resources and an externally managed MCP Secret.

## Release process

Application and chart versions move together. Before creating a release, update all of these values to the same semantic version:

- `Cargo.toml`: `package.version`
- `Cargo.lock`: the `clustersentinel` package version; keep other dependency versions unchanged
- `deploy/helm/clustersentinel/Chart.yaml`: `version` and `appVersion`
- `deploy/helm/clustersentinel/Chart.yaml`: image tag in `artifacthub.io/images`
- `deploy/helm/clustersentinel/values.yaml`: `clustersentinel.imageTag`
- `deploy/helm/clustersentinel/values.schema.json`: default `clustersentinel.imageTag`
- Root and chart `README.md`: installation examples and displayed version
- `CHANGELOG.md`: release notes

Use this order: **feature branch → pull request → main → successful main CI → tag**. For the uncommitted `0.9.3` changes, create the feature branch before committing:

```bash
git switch -c feature/release-0.9.3
git add -A
git diff --cached --stat
git diff --cached
git commit -m "Prepare Cluster Sentinel 0.9.3"
git push -u origin feature/release-0.9.3
gh pr create --base main --head feature/release-0.9.3 \
  --title "Release 0.9.3" --body "Harden the runtime image and add vulnerability gates to CI and release publication."
gh pr checks --watch
```

Review the staged diff before committing. Merge the PR only after its CI passes. Then update local `main` and find the push CI run for the exact merged commit:

```bash
git switch main
git pull --ff-only
gh run list --workflow ci.yml --branch main --event push \
  --commit "$(git rev-parse HEAD)" --limit 5
```

Wait for that run with `gh run watch <run-id> --exit-status`. Only after it succeeds, create and push the tag from clean, up-to-date `main`:

```bash
test "$(git branch --show-current)" = main
test -z "$(git status --porcelain)"
git fetch origin main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
git tag -a v0.9.3 -m "Cluster Sentinel 0.9.3"
git push origin v0.9.3
```

Stop if any command fails. Do not tag the feature branch. Release validation rejects a commit outside `main` or without a successful push CI run for that exact commit on `main`.

All CI and release jobs use the ephemeral ARC scale set `cluster-sentinel-k8s`, with one job at a time and no fallback to GitHub-hosted runners. The runner must provide Rust `1.89.0` with Clippy/rustfmt, Git, curl, jq, GitHub CLI, Docker and Buildx. Each job gets a new pod; Helm is installed by the workflow. If the cluster is unavailable, jobs wait in the queue.

The release workflow builds statically linked MUSL binaries for `linux/amd64` and `linux/arm64`, runs them on digest-pinned `distroless/static-debian13:nonroot`, and pushes `0.9.3` and `latest` to Docker Hub. Trivy scans the CI image and the published release image for OS and library vulnerabilities at every severity; any finding stops the workflow before chart publication. The release then checks that both image platforms are present. Cargo uses one build job, including inside the Dockerfile. Container builds default to `CARGO_PROFILE_RELEASE_LTO=thin`: full LTO exceeded the builder memory limit during local validation. Both CI and release use `.github/buildkitd.toml` to limit BuildKit parallelism to one; the builder container is capped at 1536 MiB memory, without extra swap, and two CPUs. These limits leave space within the 2 GiB DinD sidecar, but a full multiarch build still needs verification on the runner. Image publication has a 180-minute timeout. Only after it succeeds does the workflow validate and package the chart, create a GitHub Release, and attach the chart and SHA-256 checksum. Do not create a second release manually with `gh release create`.

The local amd64 build with ThinLTO passed under these resource limits. Builder settings use the existing SHA-pinned [docker/setup-buildx-action v4.3.0](https://github.com/docker/setup-buildx-action/tree/37fe631027851001ddb9b187196cc803df7f5f0e) and its documented [resource limits](https://docs.docker.com/build/builders/drivers/docker-container/). The existing [docker/setup-qemu-action v4.3.0](https://github.com/docker/setup-qemu-action/tree/1f40c72289eff860ee54a304f1438e3cff362e0a) installs only the arm64 emulator (sources verified 2026-09-07).

Pages is rebuilt from the published release archives. A failed download stops publication so an incomplete index cannot silently remove an earlier chart version. Release runs share a concurrency group to prevent simultaneous Pages deployments. The Pages artifact is retained for seven days. After deployment, the workflow checks the public index, downloads the new chart, and renders it with an empty Helm configuration.

Configure the repository before the first tag:

1. Add Actions secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`.
2. In **Settings > Pages**, select **GitHub Actions** as the source.
3. In **Settings > Environments > github-pages > Deployment branches and tags**, use **Selected branches and tags** and add a **Tag** rule with the pattern `v*.*.*`. A rule for the `main` branch alone does not permit tag-triggered releases to deploy.
4. Keep workflow permissions enabled for the repository. The release job requests only the scopes it needs.
5. Register the `cluster-sentinel-k8s` runner and set **Settings > Actions > General > Fork pull request workflows > Require approval for all outside collaborators**. CI skips fork PRs, but a PR can change the workflow itself: this condition does not replace repository-level approval. Do not approve external code on the shared cluster runner; use an isolated environment for those contributions.

If Pages rejects a release because of environment protection rules, add the tag rule and rerun the failed deployment job. If the `github-pages` artifact has expired, rerun `Build Helm repository` and its dependent deployment job to generate a fresh artifact.

The chart is registered in Artifact Hub from `https://chaser100.github.io/cluster-sentinel`. The repository ID `fd95efee-b435-4990-9f37-529a4ff5dba1` is stored in `docs/artifacthub-repo.yml` for the Verified publisher check. Artifact Hub renders the package description from `deploy/helm/clustersentinel/README.md`; the chart `icon` points to `docs/logo.svg`, published as `/logo.svg` on Pages. The `Helm Repository` workflow publishes Pages metadata and visual assets from `main` without rebuilding the application image or creating another release. Artifact Hub applies updates after it processes a changed repository index.

## License

Cluster Sentinel is licensed under the [MIT License](LICENSE).

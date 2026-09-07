# Cluster Sentinel Helm chart

This chart deploys Cluster Sentinel through the `application` chart version `0.3.9`, aliased as `clustersentinel`. Application-specific resources stay in this chart: read-only cluster RBAC, the MCP authentication Secret, Prometheus alert rules, and the Grafana dashboard ConfigMap.

The complete dependency is bundled under `charts/application/` in both Git and the release archive. Rendering, packaging, and installation do not require `helm dependency build` or `helm dependency update`. The upstream repository URL in `Chart.yaml` records where the dependency comes from.

## Install

```bash
helm repo add cluster-sentinel https://chaser100.github.io/cluster-sentinel
helm repo update

helm upgrade --install clustersentinel cluster-sentinel/clustersentinel \
  --version 0.9.1 \
  --namespace clustersentinel \
  --create-namespace
```

## Default resources

The default values create:

- one `Deployment` and one `ClusterIP` Service on port `8080`;
- a ServiceAccount plus a ClusterRole and ClusterRoleBinding limited to read-only access for Events and Namespaces;
- a retained `clustersentinel-mcp-auth` Secret with a generated 48-character token;
- a Grafana dashboard ConfigMap labeled `grafana_dashboard: "1"`.

`ServiceMonitor` and `PrometheusRule` are disabled by default. Enable them only when the Prometheus Operator CRDs are installed:

```yaml
prometheusRule:
  enabled: true
  labels:
    release: kube-prometheus-stack

clustersentinel:
  serviceMonitor:
    enabled: true
```

## Existing MCP Secret

Set both references when the existing Secret uses a different name:

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

The Secret must contain a non-empty `token` key unless `mcpAuth.secretKey` and the nested `secretKey` value are changed together.

## External access

The dependency supports Ingress and Gateway API `HTTPRoute`. Keep `/metrics` internal. Add every external MCP hostname to `CLUSTERSENTINEL_MCP_ALLOWED_HOSTS`; requests with another `Host` value receive `403` even when the Bearer token is valid.

Example Gateway API values:

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
                type: PathPrefix
                value: /mcp
            - path:
                type: PathPrefix
                value: /api/v1/events
          backendRefs:
            - name: clustersentinel
              port: 8080

  env:
    - name: CLUSTERSENTINEL_EVENTS_MODE
      value: kubernetes
    - name: CLUSTERSENTINEL_MCP_ALLOWED_HOSTS
      value: localhost,127.0.0.1,::1,clustersentinel,clustersentinel.example.com
```

Gateway API and Prometheus Operator CRDs are not installed by this chart.

## Main values

| Value | Default | Description |
| --- | --- | --- |
| `mcpAuth.manageSecret` | `true` | Create and retain the MCP Bearer Secret |
| `mcpAuth.existingSecret` | `""` | Reuse an externally managed Secret |
| `rbac.create` | `true` | Create cluster-wide read-only event permissions |
| `grafanaDashboard.enabled` | `true` | Create the Grafana sidecar ConfigMap |
| `prometheusRule.enabled` | `false` | Create the PrometheusRule |
| `clustersentinel.image` | `chaser420/cluster-sentinel` | Container image repository |
| `clustersentinel.imageTag` | `0.9.1` | Container image tag; must match the chart version |
| `clustersentinel.serviceMonitor.enabled` | `false` | Create a ServiceMonitor |
| `clustersentinel.resources.requests` | `50m`, `128Mi` | Default CPU and memory requests |
| `clustersentinel.resources.limits` | `500m`, `512Mi` | Default CPU and memory limits |

All other workload values pass to the aliased [Universal Helm Chart](https://github.com/chaser100/u-helm-chart/tree/main/helm-charts/application).

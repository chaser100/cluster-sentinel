# Universal Helm Chart

A reusable universal Helm chart for deploying containerized applications to Kubernetes without maintaining a separate chart for every service.

## Features

- Deployments, Services, Ingress, and Gateway API HTTPRoutes
- Jobs, CronJobs, and Horizontal Pod Autoscaling
- ConfigMaps, Secrets, arbitrary volumes, and PersistentVolumeClaims
- Init containers, sidecars, lifecycle hooks, and custom manifests
- Optional Prometheus Operator ServiceMonitor
- Argo CD, Flux, and other GitOps workflows

## Requirements

- Helm 3
- Kubernetes 1.23+ for the complete built-in API set
- Gateway API CRDs when `route.enabled=true`
- Prometheus Operator CRDs when `serviceMonitor.enabled=true`

Optional CRDs are not installed by this chart.

## Install

Add the repository:

```bash
helm repo add universal https://chaser100.github.io/u-helm-chart
helm repo update
```

Create a minimal `values.yaml`:

```yaml
image: ghcr.io/example/my-app
imageTag: "1.0.0"

service:
  port: 8080

ingress:
  enabled: true
  className: nginx
  hosts:
    - host: app.example.com
      paths:
        - path: /
          pathType: Prefix
```

Install or upgrade the application:

```bash
helm upgrade --install my-app universal/application \
  --namespace my-app \
  --create-namespace \
  -f values.yaml
```

Pin a chart version in CI/CD:

```bash
helm search repo universal/application --versions

helm upgrade --install my-app universal/application \
  --version <VERSION> \
  --namespace my-app \
  --create-namespace \
  -f values.yaml
```

## Common Use Cases

### Deployment Strategy

The chart uses the Kubernetes default rollout behavior unless `deploymentStrategy` is set.

```yaml
deploymentStrategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 0
    maxSurge: 1
```

Use `type: Recreate` for workloads that cannot run old and new Pods at the same time.

### Gateway API HTTPRoute

Use the simple fields for a single path routed to the chart-managed Service:

```yaml
service:
  port: 8080

route:
  enabled: true
  gateway: public-gateway
  gatewayNamespace: gateway-system
  hostname: app.example.com
  path: /
```

For multiple matches, filters, traffic splitting, or other Gateway API options, set a complete `HTTPRoute.spec` under `route.spec`:

```yaml
fullnameOverride: my-app

service:
  port: 8080

route:
  enabled: true
  name: my-app
  spec:
    parentRefs:
      - name: public-gateway
        namespace: gateway-system
        sectionName: https
    hostnames:
      - app.example.com
    rules:
      - matches:
          - path:
              type: Exact
              value: /health
          - path:
              type: PathPrefix
              value: /api
        backendRefs:
          - name: my-app
            port: 8080
```

When `route.spec` is non-empty, it replaces the spec generated from `route.gateway`, `route.hostname`, `route.path`, and the other simple route fields. HTTPRoute metadata still comes from `route.name`, `route.labels`, and `route.annotations`.

### Horizontal Autoscaling

```yaml
replicaCount: 2

resources:
  requests:
    cpu: 200m
    memory: 256Mi

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
```

### Database Migration Job

```yaml
job:
  enabled: true
  name: database-migration
  image: ghcr.io/example/my-app
  imageTag: "1.0.0"
  command: ["/bin/sh", "-c"]
  args: ["./app migrate"]
```

### Persistent Storage

```yaml
persistentVolumeClaims:
  - name: application-data
    size: 10Gi
    storageClassName: standard
    accessModes:
      - ReadWriteOnce
    mountPath: /var/lib/application
```

### Sidecar Container

```yaml
extraContainers:
  enabled: true
  containers:
    - name: log-collector
      image: fluent/fluent-bit:latest
      volumeMounts:
        - name: application-logs
          mountPath: /var/log/application

volumes:
  - name: application-logs
    emptyDir: {}
```

## Prometheus ServiceMonitor

The ServiceMonitor is disabled by default and does not affect clusters without the Prometheus Operator.

```yaml
service:
  name: http
  port: 9113

serviceMonitor:
  enabled: true
  labels:
    release: kube-prometheus-stack
  endpoints:
    - port: http
      path: /metrics
      interval: 30s
```

## Tested Configurations

- [Basic application](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-basic.yaml)
- [Deployment strategy](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-deployment-strategy.yaml)
- [Ingress](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-ingress.yaml)
- [Plain Ingress](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-ingress-plain.yaml)
- [Gateway API HTTPRoute](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-route.yaml) and [advanced HTTPRoute](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-route-advanced.yaml)
- [Autoscaling](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-autoscaling.yaml)
- [Jobs](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-job.yaml) and [CronJobs](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-cronjob.yaml)
- [Persistent storage and full configuration](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-full.yaml)
- [Sidecars](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-sidecar.yaml)
- [ServiceMonitor](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-servicemonitor.yaml)
- [Extra Deployments](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-extra-deployments.yaml)
- [Custom manifests](https://github.com/chaser100/u-helm-chart/blob/main/helm-charts/application/tests/values-test-extra-manifests.yaml)

## Documentation

- [Configuration reference](https://helm.networkcat89.com/docs)
- [Tested examples](https://helm.networkcat89.com/examples)
- [Interactive playground](https://helm.networkcat89.com/playground)
- [Source repository](https://github.com/chaser100/u-helm-chart)

## Uninstall

```bash
helm uninstall my-app --namespace my-app
```

## License

MIT

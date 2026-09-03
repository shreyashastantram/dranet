# DRANET Helm Chart

## Installation

From a local checkout:

```sh
helm upgrade --install dranet ./deployments/helm/dranet -n kube-system
```

## Configuration

The following table lists the configurable parameters and their default values:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nameOverride` | Override the chart name | `""` |
| `fullnameOverride` | Override the full release name | `""` |
| `image.repository` | Container image repository | `registry.k8s.io/networking/dranet` |
| `image.tag` | Container image tag | `stable` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `imagePullSecrets` | List of image pull secrets | `[]` |
| `rbac.create` | Create RBAC resources | `true` |
| `podAnnotations` | Annotations to add to pods | `{}` |
| `podLabels` | Labels to add to pods | `{}` |
| `logVerbosity` | Log verbosity level | `4` |
| `metricsPort` | Port for the metrics/healthz server and readiness probe | binary default: `9177` |
| `metricsPath` | HTTP path for the readiness probe | `/healthz` |
| `tolerations` | Pod tolerations | `[{operator: Exists, effect: NoSchedule}]` |
| `resources.requests.cpu` | CPU resource request | `100m` |
| `resources.requests.memory` | Memory resource request | `50Mi` |
| `resources.limits.cpu` | CPU resource limit | `""` (not set) |
| `resources.limits.memory` | Memory resource limit | `""` (not set) |
| `args.filter` | CEL expression to filter network interface attributes | see binary default |
| `args.inventoryMinPollInterval` | Minimum interval between two consecutive inventory polls | binary default: `2s` |
| `args.inventoryMaxPollInterval` | Maximum interval between two consecutive inventory polls | binary default: `1m` |
| `args.inventoryPollBurst` | Number of inventory polls that can be run in a burst | binary default: `5` |
| `args.moveIBInterfaces` | If true, InfiniBand (IPoIB) interfaces are moved into the pod network namespace | binary default: `true` |
| `args.cloudProviderHint` | Hint for the cloud provider plugin (`GCE`, `AZURE`, `OKE`, `NONE`); auto-detected if unset | binary default: `""` |

> **Note:** All `args.*` fields are optional. When omitted, the flag is not passed to the binary and the binary's built-in default applies.

Parameters can be set at install time using `--set` or a custom values file:

```sh
helm upgrade --install dranet ./deployments/helm/dranet -n kube-system --set logVerbosity=6
helm upgrade --install dranet ./deployments/helm/dranet -n kube-system -f my-values.yaml
```

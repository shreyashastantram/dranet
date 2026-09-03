# ACK GPU + eRDMA dranet Example

End-to-end example of topology-aware GPU + eRDMA allocation using
[Dynamic Resource Allocation (DRA)](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)
on Alibaba Cloud ACK with
[ecs.gn8is-2x.8xlarge](https://www.alibabacloud.com/help/en/ecs/user-guide/gpu-accelerated-compute-optimized-and-vgpu-accelerated-instance-families)
instances (NVIDIA L20 GPUs +
[eRDMA](https://www.alibabacloud.com/help/en/ecs/user-guide/erdma)).

## Context

### Instance: ecs.gn8is-2x.8xlarge

Each node has:

| Resource | Count | Detail |
|---|---|---|
| GPU | 2 x NVIDIA L20 | 48 GB GDDR6 each |
| eRDMA | 1 x Elastic RDMA | 25 Gbps |

### eRDMA layout

| Device | rdmaDevice | PCI Address |
|---|---|---|
| pci-0000-00-0b-0 | erdma_0 | 0000:00:0b.0 |

### Alibaba Cloud attributes (alibaba.dra.net)

| Attribute | Description |
|---|---|
| `alibaba.dra.net/instanceType` | ECS instance type (e.g. `ecs.gn8is-2x.8xlarge`) |
| `alibaba.dra.net/erdma` | `true` when eRDMA devices are present on the node |

## Prerequisites

- Kubernetes 1.36+ with `DynamicResourceAllocation` enabled
- [MPI Operator v2beta1](https://github.com/kubeflow/mpi-operator) installed
- eRDMA enabled on the ECS instance (requires eRDMA controller installed in the cluster)

## Files

| File | Description |
|---|---|
| `device-class.yaml` | `DeviceClass` for eRDMA devices |
| `resource-claim-template.yaml` | `ResourceClaimTemplate` for GPU + eRDMA allocation |
| `mpi-job.yaml` | `MPIJob` that runs `nccl_tests/all_reduce_perf` across 2 workers |

## Usage

```bash
# Apply DeviceClass and ResourceClaimTemplates
kubectl apply -f device-class.yaml
kubectl apply -f resource-claim-template.yaml

# Launch the MPIJob
kubectl apply -f mpi-job.yaml

# Wait for workers then stream launcher logs
kubectl wait --for=condition=ready pod \
  -l training.kubeflow.org/job-name=nccl-test-erdma,training.kubeflow.org/job-role=worker \
  --timeout=300s
launcher=$(kubectl get pods \
  -l training.kubeflow.org/job-name=nccl-test-erdma,training.kubeflow.org/job-role=launcher \
  -o jsonpath='{.items[0].metadata.name}')
kubectl logs -f "${launcher}"
```

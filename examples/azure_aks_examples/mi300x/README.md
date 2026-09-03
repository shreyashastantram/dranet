# AKS MI300X RCCL + dranet Example

End-to-end example of RDMA NIC allocation on AKS with the
[ND MI300X-v5 size][mi300x] (AMD Instinct MI300X GPUs + Mellanox ConnectX-7
VFs). Both GPUs and NICs are allocated via DRA: GPUs via the
`k8s-gpu-dra-driver` (`gpu.amd.com`) and the eight ConnectX VFs via dranet
(`dra.net`). Scheduling decisions are driven by the `ResourceSlice` objects
each driver publishes on every node.

See [`../README.md`](../README.md) for cluster prerequisites, dranet-on-Azure
behavior (IB-only NIC discovery, `placementGroupId`), the shared apply/verify
flow, and notes common to both AKS examples.

[mi300x]: https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/gpu-accelerated/nd-mi300x-v5-series

## Node topology

### VM: ND MI300X v5 (`Standard_ND96isr_MI300X_v5`)

Each node has:

| Resource | Count | Detail |
|---|---|---|
| GPU | 8 × AMD Instinct MI300X | 192 GB HBM3, Infinity Fabric all-to-all |
| NIC | 8 × Mellanox ConnectX-7 VF | 400 Gb/s InfiniBand each |
| NUMA nodes | 2 | 4 GPU + 4 NIC per NUMA node |

Live `ResourceSlice` for one node (`resourceslice-dranet.yaml`):

| Device | pciAddress | rdmaDevice | NUMA |
|---|---|---|---|
| pci-0101-00-00-0 | 0101:00:00.0 | mlx5_0 | 0 |
| pci-0102-00-00-0 | 0102:00:00.0 | mlx5_1 | 0 |
| pci-0103-00-00-0 | 0103:00:00.0 | mlx5_2 | 0 |
| pci-0104-00-00-0 | 0104:00:00.0 | mlx5_3 | 0 |
| pci-0105-00-00-0 | 0105:00:00.0 | mlx5_4 | 1 |
| pci-0106-00-00-0 | 0106:00:00.0 | mlx5_5 | 1 |
| pci-0107-00-00-0 | 0107:00:00.0 | mlx5_6 | 1 |
| pci-0108-00-00-0 | 0108:00:00.0 | mlx5_7 | 1 |

## Files

| File | Description |
|---|---|
| `resource-claim-template.yaml` | Three `ResourceClaimTemplate` objects for the test cases |
| `mpi-job.yaml` | `MPIJob` for the 8 GPU × 8 NIC case (`maja/rccl-tests:rocm-7.0.2-gfx942`, RCCL 2.27.7 / ROCm 7.0.2) |
| `resourceslice-dranet.yaml` | Live NIC `ResourceSlice` from an MI300X node (reference) |

## ResourceClaimTemplates

| Template | NIC(s) selected | Selector |
|---|---|---|
| `4nic-same-numa` | 4 × NUMA-0 NICs (`mlx5_0`..`mlx5_3`) | `rdma == true && numaNode == 0` |
| `4nic-cross-numa` | 4 × NUMA-1 NICs (`mlx5_4`..`mlx5_7`) | `rdma == true && numaNode == 1` |
| `8nic-all` | all 8 RDMA NICs per worker | `rdma == true` |

Apply templates and MPIJob per the shared [usage pattern](../README.md#usage-pattern);
switch test cases by editing `resourceClaimTemplateName:` in `mpi-job.yaml`.

## Benchmark Results

2-node `all_reduce_perf` across MI300X nodes, NIC set controlled by the DRA
template. Each row corresponds to a different `MPIJob` + `ResourceClaimTemplate`
combination. The progression shows how topology-aware NIC selection — expressed
as a single CEL expression in the claim template — moves throughput across more
than an order of magnitude on the same hardware.

| Scenario | Template | GPUs × NICs | Avg busbw | Peak busbw |
|---|---|---|---|---|
| Cross-NUMA NIC selection | `4nic-cross-numa` | 4 × 4 | 6.10 GB/s | 6.19 GB/s |
| Same-NUMA NIC selection | `4nic-same-numa` | 4 × 4 | 42.53 GB/s | 49.28 GB/s |
| All NICs | `8nic-all` | 8 × 8 | 67.73 GB/s | 78.81 GB/s |

### Key observations

**Same-NUMA NIC selection is ~7× faster than cross-NUMA at 4 GPU × 4 NIC.**
When multiple local GPUs share a cross-NUMA path, throughput collapses far
below the per-NIC line rate; confining the claim to same-NUMA NICs keeps the
aggregate near the single-HCA ceiling.

**Scaling to all 8 NICs** — the 8 × 8 aggregate climbs past 78 GB/s, an order
of magnitude above the cross-NUMA case on the same hardware, with no change
except the claim template.

**Isolation confirmed.** Each template injects exactly the allocated
`/dev/infiniband/uverbsN` char devices into the pod:
`4nic-same-numa` → `uverbs0..uverbs3`,
`4nic-cross-numa` → `uverbs4..uverbs7`,
`8nic-all` → `uverbs0..uverbs7`. dranet's NRI plugin does this without
`privileged: true`.

## Notes

- GPU scheduling on the node is driven by the `ResourceSlice` objects the
  `k8s-gpu-dra-driver` publishes under the `gpu.amd.com` DRA driver — one
  GPU device per MI300X. dranet publishes NIC `ResourceSlice` objects under
  `dra.net`. The scheduler picks a node whose slices can satisfy every
  request in the pod's `ResourceClaim`(s); `amd.com/gpu: 8` in `mpi-job.yaml`
  pins the worker to a full-node MI300X host.

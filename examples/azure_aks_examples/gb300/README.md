# AKS GB300 InfiniBand dranet Example

End-to-end example of topologically-aware GPU + InfiniBand NIC allocation on
AKS with the [ND GB300-v6 size series][gb300] (NVIDIA GB300 GPUs + Mellanox
ConnectX IB-only VFs). Both GPUs and NICs are allocated via DRA
(`gpu.nvidia.com` + `dra.net`).

See [`../README.md`](../README.md) for cluster prerequisites, dranet-on-Azure
behavior (IB-only NIC discovery, `placementGroupId`), the shared apply/verify
flow, and notes common to both AKS examples.

Two GB300-specific capabilities are demonstrated:

1. **IB-only NIC discovery and isolation** — dranet exposes GB300 ConnectX VFs
   (which have no Ethernet netdev) as DRA devices and injects only the allocated
   `/dev/infiniband/uverbsN` into each pod.
2. **Azure placement group awareness** — a CEL selector on
   `azure.dra.net/placementGroupId` constrains a job to a single InfiniBand
   fabric.

[gb300]: https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/gpu-accelerated/nd-gb300-v6-series?tabs=sizebasic

## Node topology

### VM: ND GB300 v6

Each node has:

| Resource | Count | Detail |
|---|---|---|
| GPU | 4 × NVIDIA GB300 | 288 GB HBM3E, NVLink-18 all-to-all |
| NIC | 4 × Mellanox ConnectX | 800 Gb/s InfiniBand each |
| NUMA nodes | 2 | 2 GPU + 2 NIC per NUMA node |

NUMA topology (`nvidia-smi topo -m`):

|      | GPU0 | GPU1 | GPU2 | GPU3 | NIC0 | NIC1 | NIC2 | NIC3 |
|------|------|------|------|------|------|------|------|------|
| GPU0 | X    | NV18 | NV18 | NV18 | NODE | NODE | SYS  | SYS  |
| GPU1 | NV18 | X    | NV18 | NV18 | NODE | NODE | SYS  | SYS  |
| GPU2 | NV18 | NV18 | X    | NV18 | SYS  | SYS  | NODE | NODE |
| GPU3 | NV18 | NV18 | NV18 | X    | SYS  | SYS  | NODE | NODE |

NIC mapping: NIC0=mlx5_0 (NUMA 0), NIC1=mlx5_1 (NUMA 0), NIC2=mlx5_2 (NUMA 1), NIC3=mlx5_3 (NUMA 1)

### DRA device attributes

**GPU** (driver: `gpu.nvidia.com`):

| Device | pciBusID | NUMA | pcieRoot |
|---|---|---|---|
| gpu-0 | 0008:06:00.0 | 0 | pci0008:00 |
| gpu-1 | 0009:06:00.0 | 0 | pci0009:00 |
| gpu-2 | 0018:06:00.0 | 1 | pci0018:00 |
| gpu-3 | 0019:06:00.0 | 1 | pci0019:00 |

**NIC** (driver: `dra.net`):

| Device | pciAddress | rdmaDevice | NUMA |
|---|---|---|---|
| pci-0101-00-00-0 | 0101:00:00.0 | mlx5_0 | 0 |
| pci-0102-00-00-0 | 0102:00:00.0 | mlx5_1 | 0 |
| pci-0103-00-00-0 | 0103:00:00.0 | mlx5_2 | 1 |
| pci-0104-00-00-0 | 0104:00:00.0 | mlx5_3 | 1 |

These devices have no `ifName` attribute — IB-only status is derived at runtime
from `rdmaDevice != "" && ifName == ""`. See `resourceslice-gpu.yaml` and
`resourceslice-dranet.yaml` for the full ResourceSlice objects from a live node.

## Files

| File | Description |
|---|---|
| `resource-claim-template.yaml` | Four `ResourceClaimTemplate` objects for the test cases |
| `mpi-job.yaml` | `MPIJob` that runs `nccl_tests/all_reduce_perf` across 2 workers |
| `resourceclaim.yaml` | Live `ResourceClaim` from a scheduled pod (reference) |
| `resourceslice-gpu.yaml` | Live GPU `ResourceSlice` from a GB300 node (reference) |
| `resourceslice-dranet.yaml` | Live NIC `ResourceSlice` from a GB300 node (reference) |

## ResourceClaimTemplates

Four templates are defined. Update `mpi-job.yaml` `resourceClaimTemplateName:`
to switch between them.

### `1nic-aligned` — 1 GPU + 1 NIC, same NUMA

```yaml
- name: gpu
  exactly:
    deviceClassName: gpu.nvidia.com
    selectors:
    - cel:
        expression: 'device.attributes["resource.kubernetes.io"]["pciBusID"] == "0008:06:00.0"'
- name: nic
  exactly:
    deviceClassName: dranet.net
    selectors:
    - cel:
        expression: 'device.attributes["dra.net"]["rdmaDevice"] == "mlx5_0"'
```

GPU 0 (NUMA 0) + mlx5_0 (NUMA 0). **NODE** affinity — direct PCIe path for GDR.

### `2nic-aligned` — 1 GPU + 2 NICs, same NUMA

```yaml
- name: gpu
  exactly:
    deviceClassName: gpu.nvidia.com
    selectors:
    - cel:
        expression: 'device.attributes["resource.kubernetes.io"]["pciBusID"] == "0008:06:00.0"'
- name: nic
  exactly:
    deviceClassName: dranet.net
    count: 2
    selectors:
    - cel:
        expression: 'device.attributes["dra.net"]["rdma"] == true && device.attributes["dra.net"]["numaNode"] == 0'
```

GPU 0 (NUMA 0) + any 2 RDMA NICs from NUMA 0 (mlx5_0 + mlx5_1). DRA picks 2
distinct devices from the NUMA-0 pool.

### `1nic-unaligned` — 1 GPU + 1 NIC, cross-NUMA

```yaml
- name: gpu
  exactly:
    deviceClassName: gpu.nvidia.com
    selectors:
    - cel:
        expression: 'device.attributes["resource.kubernetes.io"]["pciBusID"] == "0008:06:00.0"'
- name: nic
  exactly:
    deviceClassName: dranet.net
    selectors:
    - cel:
        expression: 'device.attributes["dra.net"]["rdmaDevice"] == "mlx5_2"'
```

GPU 0 (NUMA 0) + mlx5_2 (NUMA 1). **SYS** affinity — cross-NUMA, no GDR path.

### `ib-same-fabric` — same placement group (IB fabric)

```yaml
- name: nic
  exactly:
    deviceClassName: dranet.net
    selectors:
    - cel:
        expression: >-
          device.attributes["azure.dra.net"]["placementGroupId"] == "<placementGroupId>"
    - cel:
        expression: >-
          device.attributes["dra.net"]["rdma"] == true
```

Constrains the scheduler to RDMA devices on nodes within the specified
placement group, preventing cross-fabric scheduling failures. Replace
`<placementGroupId>` with a value published by dranet in your cluster — see
[the shared README](../README.md#azure-placement-group-and-vm-metadata) for the
lookup command.

## Benchmark Results

2-node `all_reduce_perf`, 1 GPU per worker, `NCCL_IB_DATA_DIRECT=0` (nvidia-peermem GDR).
Transport: `NET/IBext_v11/GDRDMA`.

| Template | GPU | NIC(s) | NUMA relation | Channels | GDR | Avg busbw |
|---|---|---|---|---|---|---|
| `1nic-aligned` | gpu-0 (NUMA 0) | mlx5_0 (NUMA 0) | NODE | 8 | ✓ | **~56 GB/s** |
| `2nic-aligned` | gpu-0 (NUMA 0) | mlx5_0 + mlx5_1 (NUMA 0) | NODE | 16 | ✓ | **~112 GB/s** |
| `1nic-unaligned` | gpu-0 (NUMA 0) | mlx5_2 (NUMA 1) | SYS | 2 | ✗ | **~25 GB/s** |

### Key observations

**NUMA alignment matters ~4.5×:**
Cross-NUMA (SYS) placement degrades performance from ~56 GB/s to ~25 GB/s with
the same NIC count. Three compounding penalties:

1. **GDR disabled** — NCCL falls back from `GDRDMA` to staging through host
   memory when the NIC has no direct PCIe path to the GPU.
2. **Fewer channels** — NCCL's topology engine allocates 2 channels for
   SYS-distant NICs vs 8 for NODE-local NICs.
3. **Cross-NUMA memory traffic** — data crosses the QPI/UPI interconnect between
   NUMA domains on every transfer.

**Isolation confirmed:**

- `1nic-aligned`: pod sees only `uverbs0` + `umad0` + `rdma_cm` (mlx5_0)
- `2nic-aligned`: pod sees only `uverbs0` + `uverbs1` + `umad0` + `umad1` + `rdma_cm` (mlx5_0 + mlx5_1)
- `1nic-unaligned`: pod sees only `uverbs2` + `umad2` + `rdma_cm` (mlx5_2)

In all cases `uverbs*` devices for un-allocated NICs are absent, without
`privileged: true` — isolation is enforced by the dranet NRI plugin injecting
only the char devices that correspond to the DRA-allocated NIC(s).

**`count: 2` with a pool selector:**
The `2nic-aligned` template uses a single request with `count: 2` and a
`numaNode == 0` predicate. The DRA scheduler allocates two distinct NUMA-0
devices — `pci-0101-00-00-0` and `pci-0102-00-00-0` — and dranet injects both
`uverbs0` and `uverbs1` into the pod. The `count: N` + pool-selector pattern
is the idiomatic DRA approach for multi-device allocation from a homogeneous group.

## Placement group verification

On a cluster with 2 `Standard_ND128isr_GB300_v6` GPU nodes, each in its own
placement group:

| Node | placementGroupId | IB Devices (pciAddress) |
|---|---|---|
| aks-gpu-45139532-vmss000081 | `c6c749e8-a38b-470e-8c94-2a7d00001bf0` | 0101/0102/0103/0104:00:00.0 |
| aks-gpu-45139532-vmss000089 | `2b4b6af0-0ca2-4a02-b883-178618b5ab5e` | 0101/0102/0103/0104:00:00.0 |

Confirmed that nodes in different placement groups have different
`placementGroupId` values and that cross-placement-group IB is non-functional:

- **Intra-node `ib_write_bw`**: 449.43 Gb/s (working)
- **Cross-node `ib_write_bw`** (different placement groups): transport retry counter exceeded, error 12 (broken)
- **Cross-node NCCL `all_reduce_perf`** (different placement groups): hangs at RDMA data transfer

Using the `ib-same-fabric` template (or adding the `placementGroupId` CEL
predicate to any other template) constrains the scheduler to a single IB
fabric and avoids these failures.

---
title: "Alibaba Cloud ACK with GPU and eRDMA"
date: 2026-07-09T00:00:00Z
---

[eRDMA](https://www.alibabacloud.com/help/en/ecs/user-guide/elastic-rdma-erdma) (Elastic RDMA) is Alibaba Cloud's software-defined RDMA technology that reuses the VPC network for low-latency, high-throughput communication. On ACK GPU clusters, you can use DRANET to allocate eRDMA devices to pods via [Dynamic Resource Allocation (DRA)](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/), enabling NCCL to bypass the kernel and transfer data directly between GPU memory and the eRDMA adapter.

This guide uses [`ecs.gn8is-2x.8xlarge`](https://www.alibabacloud.com/help/en/ecs/user-guide/overview-of-instance-families) instances (2 NVIDIA L20 GPUs and 1 eRDMA adapter per node). The full source lives at [examples/alibaba_ack_examples/gpu-erdma](https://github.com/kubernetes-sigs/dranet/tree/main/examples/alibaba_ack_examples/gpu-erdma).

## Prerequisites

### Create an ACK cluster with eRDMA nodes

Create an ACK cluster (Kubernetes 1.36+) with `DynamicResourceAllocation` enabled. Add GPU node pools using eRDMA-capable instance types (e.g. `gn8is`, `ebmgn7ex`, `ebmgn7ix`). When creating the ECS instances, enable eRDMA by either:

- Selecting the **Install eRDMA software stack** option in the ECS console, or
- Using the **Alibaba Cloud Linux 3 (eRDMA software stack pre-installed)** marketplace image, which comes with the eRDMA driver and Mellanox OFED pre-installed.

See the [eRDMA overview](https://www.alibabacloud.com/help/en/ecs/user-guide/elastic-rdma-erdma) for details on enabling eRDMA on ECS instances.

### Install the MPI Operator

```sh
kubectl apply --server-side -k "https://github.com/kubeflow/mpi-operator/manifests/overlays/standalone?ref=v0.7.0"
```

### Install DRANET

Install the NVIDIA GPU device plugin and the DRANET DaemonSet:

```sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/dranet/refs/heads/main/install.yaml
kubectl get pods -l k8s-app=dranet -n kube-system
```

DRANET auto-detects Alibaba Cloud instances via the [ECS Instance Metadata Service (IMDS)](https://www.alibabacloud.com/help/en/ecs/user-guide/view-instance-metadata) and discovers eRDMA devices by scanning `/sys/class/infiniband/` for entries prefixed with `erdma`. No cloud-specific configuration is required.

## Verify discovered devices

Once DRANET is running, the eRDMA devices are published as DRA `ResourceSlice` objects. Inspect them:

```sh
kubectl get resourceslices -o yaml | grep -A5 erdma
```

Each eRDMA device exposes the following attributes:

```yaml
dra.net/pciAddress: 0000:00:0b.0
dra.net/rdma: true
dra.net/rdmaDevice: erdma_0
alibaba.dra.net/instanceType: ecs.gn8is-2x.8xlarge
alibaba.dra.net/erdma: true
```

| Attribute | Description |
|---|---|
| `dra.net/rdma` | `true` for any RDMA-capable device |
| `dra.net/rdmaDevice` | RDMA device name (e.g. `erdma_0`) |
| `dra.net/pciAddress` | PCI address of the device |
| `alibaba.dra.net/instanceType` | ECS instance type from IMDS |
| `alibaba.dra.net/erdma` | `true` when the device is an eRDMA adapter |

## Defining Resources for DRANET

First, we define a `DeviceClass` that selects only eRDMA devices on Alibaba Cloud instances. The two CEL selectors work together: the first matches any RDMA-capable device from DRANET, and the second narrows it to devices that carry the `alibaba.dra.net/erdma` attribute. This lets the same DRANET DaemonSet run on mixed clusters (Alibaba, AWS, Azure) and select only the relevant devices per node.

```yaml
# DeviceClass for Alibaba Cloud eRDMA devices.
apiVersion: resource.k8s.io/v1
kind: DeviceClass
metadata:
  name: erdma.alibaba.dra.net
spec:
  selectors:
  - cel:
      expression: >-
        device.driver == "dra.net" &&
        device.attributes["dra.net"].rdma == true
  - cel:
      expression: >-
        "alibaba.dra.net" in device.attributes &&
        device.attributes["alibaba.dra.net"]["erdma"] == true
```

Next, a `ResourceClaimTemplate` requests one eRDMA device per worker. GPUs are allocated via the traditional `nvidia.com/gpu` device plugin (2 GPUs per worker on `ecs.gn8is-2x.8xlarge`):

```yaml
# 1 eRDMA per worker.
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata:
  name: erdma
spec:
  spec:
    devices:
      requests:
      - name: erdma
        exactly:
          deviceClassName: erdma.alibaba.dra.net
          count: 1
```

## Creating the workload

The `MPIJob` runs NCCL `all_reduce_perf` across 2 workers, each with 2 GPUs and 1 eRDMA device. Workers are scheduled on different nodes via pod anti-affinity:

```yaml
apiVersion: kubeflow.org/v2beta1
kind: MPIJob
metadata:
  name: nccl-test-erdma
spec:
  slotsPerWorker: 2
  mpiReplicaSpecs:
    Launcher:
      replicas: 1
      template:
        spec:
          containers:
          - name: nccl
            image: dsw-registry.cn-hangzhou.cr.aliyuncs.com/pai/nccl-tests:12.2.2-cudnn8-devel-ubuntu22.04-nccl2.19.3-1-85f9143
            command: ["/bin/bash", "-c"]
            args:
            - |
              sleep 10
              mpirun -np 4 \
                --allow-run-as-root \
                -bind-to none \
                --mca pml ob1 \
                --mca btl tcp,self \
                --mca btl_tcp_if_include eth0 \
                --mca coll_hcoll_enable 0 \
                --mca coll_ucc_enable 0 \
                -x LD_LIBRARY_PATH \
                -x NCCL_DEBUG=INFO \
                -x NCCL_SOCKET_IFNAME=eth0 \
                -x NCCL_IB_HCA=erdma \
                -x NCCL_SHM_DISABLE=1 \
                /opt/nccl_tests/build/all_reduce_perf -b 512M -e 8G -f 2 -g 1 -c 0
    Worker:
      replicas: 2
      template:
        spec:
          affinity:
            podAntiAffinity:
              requiredDuringSchedulingIgnoredDuringExecution:
              - labelSelector:
                  matchLabels:
                    training.kubeflow.org/job-name: nccl-test-erdma
                    training.kubeflow.org/job-role: worker
                topologyKey: kubernetes.io/hostname
          automountServiceAccountToken: false
          resourceClaims:
          - name: erdma
            resourceClaimTemplateName: erdma
          containers:
          - name: nccl
            image: dsw-registry.cn-hangzhou.cr.aliyuncs.com/pai/nccl-tests:12.2.2-cudnn8-devel-ubuntu22.04-nccl2.19.3-1-85f9143
            resources:
              limits:
                nvidia.com/gpu: "2"
              claims:
              - name: erdma
            volumeMounts:
            - name: shm
              mountPath: /dev/shm
            securityContext:
              capabilities:
                add:
                - IPC_LOCK
          volumes:
          - name: shm
            emptyDir:
              medium: Memory
              sizeLimit: 8Gi
          tolerations:
          - key: "nvidia.com/gpu"
            operator: "Exists"
            effect: "NoSchedule"
```

The key eRDMA-specific NCCL environment variables are:

- `NCCL_IB_HCA=erdma` — tells NCCL to use eRDMA devices (named `erdma_*`) as the InfiniBand HCA
- `NCCL_SHM_DISABLE=1` — disables shared-memory transport, forcing all traffic (including intra-node) through eRDMA; useful for benchmarking eRDMA, but remove for production multi-GPU workloads
- `NCCL_SOCKET_IFNAME=eth0` — uses the pod network for TCP fallback and control traffic

## Running and Observing

Apply the manifests and launch the job:

```sh
kubectl apply -f device-class.yaml
kubectl apply -f resource-claim-template.yaml
kubectl apply -f mpi-job.yaml
```

Wait for the workers to become ready:

```sh
kubectl wait --for=condition=ready pod \
  -l training.kubeflow.org/job-name=nccl-test-erdma,training.kubeflow.org/job-role=worker \
  --timeout=300s
```

Verify that the eRDMA device was allocated to each worker:

```sh
kubectl get resourceclaims
NAME                        STATE                AGE
nccl-test-erdma-worker-0…   allocated,reserved   1m
nccl-test-erdma-worker-1…   allocated,reserved   1m
```

Stream the launcher logs to see the NCCL test output:

```sh
launcher=$(kubectl get pods \
  -l training.kubeflow.org/job-name=nccl-test-erdma,training.kubeflow.org/job-role=launcher \
  -o jsonpath='{.items[0].metadata.name}')
kubectl logs -f "${launcher}"
```

The NCCL `all_reduce_perf` output will show the eRDMA transport in use and the achieved bus bandwidth. To confirm NCCL is actually using eRDMA (and not falling back to TCP), grep for the transport line:

```sh
kubectl logs "${launcher}" | grep "NET/IB"
# Expected: NCCL INFO NET/IB : Using [0]erdma_0:1/RoCE [RO]; OOB eth0:<pod-ip>
# If absent, NCCL fell back to TCP — check that the image includes the eRDMA network plugin.
```

Full results are in the [example README](https://github.com/kubernetes-sigs/dranet/blob/main/examples/alibaba_ack_examples/gpu-erdma/README.md).

#!/bin/bash
set -euo pipefail

NUM_GPUS=${MFU_GPUS_PER_WORKER:-4}
NUM_HOSTS=$(awk 'NF {print $1}' /etc/mpi/hostfile | sort -u | wc -l)
NP=$((NUM_HOSTS * NUM_GPUS))
export MASTER_ADDR=$(awk 'NF {print $1; exit}' /etc/mpi/hostfile)
export MASTER_PORT=${MASTER_PORT:-29500}
RUN_LABEL=${MFU_RUN_LABEL:-4gpu}

while ! (for host in $(awk 'NF {print $1}' /etc/mpi/hostfile | sort -u); do
  getent hosts "$host" >/dev/null || exit 1
  ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$host" true 2>/dev/null || exit 1
done); do
  echo "Waiting for workers to accept SSH..."
  sleep 5
done
sleep 5

echo "Launching ${RUN_LABEL} PyTorch NCCL MFU benchmark across ${NUM_HOSTS} nodes (${NP} ranks)"
mpirun \
  --allow-run-as-root \
  --bind-to none \
  --map-by slot \
  -mca routed direct \
  -mca plm_rsh_args "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  -x PATH \
  -x LD_LIBRARY_PATH \
  -x MASTER_ADDR \
  -x MASTER_PORT \
  -x NCCL_DEBUG=WARN \
  -x NCCL_SOCKET_IFNAME=eth0 \
  -x NCCL_IB_DISABLE=0 \
  -x NCCL_IB_HCA=mlx5 \
  -x NCCL_NET_GDR_LEVEL=PHB \
  -x NCCL_DMABUF_ENABLE=1 \
  -x CUDA_DEVICE_MAX_CONNECTIONS=1 \
  -x PYTHONUNBUFFERED=1 \
  -x MFU_BATCH_SIZE \
  -x MFU_HIDDEN_SIZE \
  -x MFU_LAYERS \
  -x MFU_WARMUP_STEPS \
  -x MFU_STEPS \
  -x GPU_PEAK_TFLOPS \
  -np "${NP}" \
  python3 -u /workspace/benchmark/mfu_benchmark.py

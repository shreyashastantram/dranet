import datetime
import os
import time

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
from torch.nn.parallel import DistributedDataParallel as DDP


def env_int(name, default):
    return int(os.environ.get(name, default))


def env_float(name, default):
    return float(os.environ.get(name, default))


# Dense BF16 Tensor Core peak TFLOP/s per GPU. NVIDIA datasheets advertise the
# "with sparsity" number; dense is half (see footnote on each product page).
#   H100 SXM:  1979 sparse / 2 = 989   https://www.nvidia.com/en-us/data-center/h100/
#   H200 SXM:  1979 sparse / 2 = 989   https://www.nvidia.com/en-us/data-center/h200/
#   B200:      4500 sparse / 2 = 2250  https://www.nvidia.com/en-us/data-center/dgx-b200/
#   GB200:     5000 sparse / 2 = 2500  https://www.nvidia.com/en-us/data-center/gb200-nvl72/
#   A100 SXM:   624 sparse / 2 = 312   https://www.nvidia.com/en-us/data-center/a100/
def default_peak_tflops(device_name):
    name = device_name.lower()
    if "h100" in name:
        return 989.0
    if "h200" in name:
        return 989.0
    if "b200" in name:
        return 2250.0
    if "gb200" in name:
        return 2500.0
    if "a100" in name:
        return 312.0
    return 989.0


def first_host_from_mpi_hostfile():
    try:
        with open("/etc/mpi/hostfile", "r", encoding="utf-8") as hostfile:
            for line in hostfile:
                fields = line.split()
                if fields:
                    return fields[0]
    except FileNotFoundError:
        return "localhost"
    return "localhost"


class DenseBenchmark(nn.Module):
    def __init__(self, hidden_size, layers):
        super().__init__()
        self.layers = nn.ModuleList(
            nn.Linear(hidden_size, hidden_size, bias=False) for _ in range(layers)
        )

    def forward(self, x):
        for index, layer in enumerate(self.layers):
            x = layer(x)
            if index + 1 != len(self.layers):
                x = F.gelu(x)
        return x


def main():
    rank = env_int("OMPI_COMM_WORLD_RANK", env_int("RANK", 0))
    world_size = env_int("OMPI_COMM_WORLD_SIZE", env_int("WORLD_SIZE", 1))
    local_rank = env_int("OMPI_COMM_WORLD_LOCAL_RANK", env_int("LOCAL_RANK", 0))

    os.environ.setdefault("RANK", str(rank))
    os.environ.setdefault("WORLD_SIZE", str(world_size))
    os.environ.setdefault("LOCAL_RANK", str(local_rank))
    os.environ.setdefault("MASTER_ADDR", first_host_from_mpi_hostfile())
    os.environ.setdefault("MASTER_PORT", "29500")

    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is not available")

    device_count = torch.cuda.device_count()
    if device_count == 0:
        raise RuntimeError("No CUDA devices are visible")

    device_index = local_rank % device_count
    torch.cuda.set_device(device_index)
    device = torch.device("cuda", device_index)

    dist.init_process_group(
        backend="nccl",
        rank=rank,
        world_size=world_size,
        timeout=datetime.timedelta(minutes=30),
    )

    hidden_size = env_int("MFU_HIDDEN_SIZE", 8192)
    batch_size = env_int("MFU_BATCH_SIZE", 4096)
    layers = env_int("MFU_LAYERS", 12)
    warmup_steps = env_int("MFU_WARMUP_STEPS", 5)
    measure_steps = env_int("MFU_STEPS", 20)

    torch.manual_seed(1234 + rank)
    torch.backends.cuda.matmul.allow_tf32 = True

    model = DenseBenchmark(hidden_size, layers).to(device=device, dtype=torch.bfloat16)
    model = DDP(model, device_ids=[device_index], output_device=device_index)
    x = torch.randn(batch_size, hidden_size, device=device, dtype=torch.bfloat16)

    def train_step():
        model.zero_grad(set_to_none=True)
        y = model(x)
        loss = y.float().square().mean()
        loss.backward()
        return loss.detach()

    for _ in range(warmup_steps):
        train_step()

    torch.cuda.synchronize(device)
    dist.barrier()
    start = time.perf_counter()

    for _ in range(measure_steps):
        train_step()

    torch.cuda.synchronize(device)
    dist.barrier()
    elapsed = time.perf_counter() - start

    elapsed_tensor = torch.tensor([elapsed], device=device)
    dist.all_reduce(elapsed_tensor, op=dist.ReduceOp.MAX)
    elapsed = elapsed_tensor.item()

    step_seconds = elapsed / measure_steps
    # Linear layer training FLOPs: forward matmul + dInput + dWeight (6ND rule).
    # See PaLM paper, Appendix B: https://arxiv.org/abs/2204.02311
    flops_per_rank_step = 6.0 * batch_size * hidden_size * hidden_size * layers
    tflops_per_gpu = flops_per_rank_step / step_seconds / 1e12
    device_name = torch.cuda.get_device_name(device)
    peak_tflops = env_float("GPU_PEAK_TFLOPS", default_peak_tflops(device_name))
    mfu = 100.0 * tflops_per_gpu / peak_tflops
    global_tflops = tflops_per_gpu * world_size

    loss_tensor = train_step()
    dist.all_reduce(loss_tensor, op=dist.ReduceOp.AVG)

    if rank == 0:
        print(
            "MFU_RESULT "
            f"backend=nccl "
            f"device={device_name!r} "
            f"world_size={world_size} "
            f"local_visible_gpus={device_count} "
            f"batch_size_per_gpu={batch_size} "
            f"hidden_size={hidden_size} "
            f"layers={layers} "
            f"warmup_steps={warmup_steps} "
            f"measured_steps={measure_steps} "
            f"dtype=bf16 "
            f"avg_step_ms={step_seconds * 1000.0:.3f} "
            f"tflops_per_gpu={tflops_per_gpu:.2f} "
            f"global_tflops={global_tflops:.2f} "
            f"peak_tflops_per_gpu={peak_tflops:.1f} "
            f"mfu_percent={mfu:.2f} "
            f"final_loss={loss_tensor.item():.6f}",
            flush=True,
        )

    dist.destroy_process_group()


if __name__ == "__main__":
    main()

#!/bin/bash
# The NVIDIA libraries for EESSI 2023.06's loader on rust (2026-11-02), for mpi2g.sbatch.
# EESSI's compat-layer loader searches neither the host's /usr/lib nor ldconfig's cache. haskell
# reaches the driver through EESSI's host_injections (/opt/eessi/nvidia); julia has no
# /opt/eessi, so libcuda.so.1 doesn't load there (job 26141: "error no-libcuda"). And neither
# node gives that loader a libcudart.so, which Dragon 0.15 dlopens to list a node's GPUs (job
# 26141: "GPUs: {'julia': []}"). So, without touching the nodes:
# - nvlib/ holds links to the host driver's libraries (same path and version, 580.126.09, on
#   both nodes) and libcudart.so, put first on LD_LIBRARY_PATH by mpi2g.sbatch;
# - cudart/ is PyPI's nvidia-cuda-runtime-cu12, installed for EESSI's Python 3.11.
# Run inside a job, not on the login node:
#   srun -p cpu -w julia -N 1 -c 2 --mem=4G -t 10 bash rust-gpu-setup.sh
set -euo pipefail
B=/home/mturilli/tmp/rhapsody-mpi2-gpu
PY=/home/mturilli/tmp/rhapsody-mpi2/venv/bin/python
mkdir -p "$B/nvlib" "$B/logs" "$B/runs"
[ -e "$B/cudart" ] || "$PY" -m pip install -q --target "$B/cudart" "nvidia-cuda-runtime-cu12==12.9.*"
RT=$(ls "$B"/cudart/nvidia/cuda_runtime/lib/libcudart.so.12* | head -1)
H=/usr/lib/x86_64-linux-gnu
for l in libcuda.so.1 libnvidia-ml.so.1 libnvidia-ptxjitcompiler.so.1; do
  ln -sfn "$H/$l" "$B/nvlib/$l"
done
ln -sfn libcuda.so.1 "$B/nvlib/libcuda.so"
ln -sfn "$RT" "$B/nvlib/libcudart.so"
ln -sfn "$RT" "$B/nvlib/libcudart.so.12"
ls -l "$B/nvlib"
"$PY" -m pip list --path "$B/cudart" 2>/dev/null | grep -i nvidia
LD_LIBRARY_PATH=$B/nvlib "$PY" -c '
import ctypes
for n in ("libcuda.so.1", "libcudart.so", "libnvidia-ml.so.1"):
    try:
        ctypes.CDLL(n); print(n, "loads")
    except OSError as e:
        print(n, "FAILS:", e)
'

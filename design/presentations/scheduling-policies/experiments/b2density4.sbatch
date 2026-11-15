#!/bin/bash
# Bridges-2 density check on half a v100-32 node in GPU-shared, run while the whole-node check
# (b2density.sbatch) waits in the GPU queue: 4 GPUs, 32 pipelines (8 per GPU, E2's density),
# node mode, 48 Batch workers (96 scaled by 4/8). Then 4 minutes of learned memory (res-learn).
# The per-GPU numbers match a whole node; CPU load is on 20 of the node's 40 cores, and other
# users' jobs may hold the other half of the node.
#   sbatch -A dmr170002p -J d2s -d singleton -p GPU-shared -N 1 --gpus=v100-32:4 --ntasks-per-node=1 -c 20 -t 30 b2density4.sbatch
#SBATCH --output=/ocean/projects/dmr170002p/mturilli/sched-exp/logs/%x-%j.out
BASE=/ocean/projects/dmr170002p/mturilli/sched-exp
echo "== $(hostname) CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES SLURM_JOB_GPUS=$SLURM_JOB_GPUS"
nvidia-smi --query-gpu=index,pci.bus_id,name,memory.total --format=csv
RUNS=$(mktemp -p "$BASE/runs" density4.XXXX)
COMMON="--rep 0 --pipelines 32 --mem-scale 0.667 --node-gpu-mem 31.0 --ctx-gib 0.30 --iters-per-ms 31800 --kind-per-tool --workers-per-node 48 --shapes-from-dragon --expect-gpus 4"
echo "e2_gpu.py --config res-node --window 480 $COMMON" > "$RUNS"
echo "e2_gpu.py --config res-learn --window 240 $COMMON --unknown-per-node 4" >> "$RUNS"
RUNS=$RUNS PS_EVERY=10 RUN_TIMEOUT_S=900 MIN_LEFT_S=300 RESULTS_DIR=$BASE/results/density4 bash "$BASE/experiments/b2run.sbatch"

#!/bin/bash
# Bridges-2 density check before E2 (fifth campaign): one whole v100-32 node (8 GPUs) at E2's
# density, 64 pipelines (8 per GPU), node mode, 96 Batch workers. Does CPU limit the GPU tasks?
# Then 4 minutes of learned memory (res-learn) on the same node.
# The node sampler records load and, every 10 s, ps of our processes (PS_EVERY).
#   sbatch -A dmr170002p -J d2 -d singleton -p GPU -N 1 --gpus=v100-32:8 --ntasks-per-node=1 -c 40 -t 30 b2density.sbatch
#SBATCH --output=/ocean/projects/dmr170002p/mturilli/sched-exp/logs/%x-%j.out
BASE=/ocean/projects/dmr170002p/mturilli/sched-exp
RUNS=$(mktemp -p "$BASE/runs" density.XXXX)
echo "e2_gpu.py --config res-node --rep 0 --pipelines 64 --window 480 --mem-scale 0.667 --node-gpu-mem 31.0 --ctx-gib 0.30 --iters-per-ms 31800 --kind-per-tool --workers-per-node 96 --shapes-from-dragon --expect-gpus 8" > "$RUNS"
echo "e2_gpu.py --config res-learn --rep 0 --pipelines 64 --window 240 --mem-scale 0.667 --node-gpu-mem 31.0 --ctx-gib 0.30 --iters-per-ms 31800 --kind-per-tool --workers-per-node 96 --shapes-from-dragon --expect-gpus 8 --unknown-per-node 8" >> "$RUNS"
RUNS=$RUNS PS_EVERY=10 RUN_TIMEOUT_S=900 MIN_LEFT_S=300 RESULTS_DIR=$BASE/results/density bash "$BASE/experiments/b2run.sbatch"

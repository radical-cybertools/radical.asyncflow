#!/bin/bash
# Bridges-2: where does node-mode E2 stall? In the density run (49881976, 4 GPUs, 32 pipelines,
# 48 workers) res-node finished its first rfd3 tasks and then delivered nothing more; on one GPU
# with 8 pipelines and 16 workers (49885043) it did not stall. STALL_ARGS (default: one GPU, 8
# pipelines) are e2_gpu.py's arguments; at each time in DUMP_AT the e2_gpu.py client gets
# SIGUSR1 and writes its threads' stacks (faulthandler) to the job log.
#   sbatch -A dmr170002p -J d2s -d singleton -p GPU-shared --gpus=v100-32:1 -N 1 --ntasks-per-node=1 -c 5 -t 15 b2stall.sbatch
#SBATCH --output=/ocean/projects/dmr170002p/mturilli/sched-exp/logs/%x-%j.out
BASE=/ocean/projects/dmr170002p/mturilli/sched-exp
RUNS=$(mktemp -p "$BASE/runs" stall.XXXX)
echo "e2_gpu.py ${STALL_ARGS:---config res-node --rep 0 --pipelines 8 --window 120 --mem-scale 0.667 --node-gpu-mem 31.0 --ctx-gib 0.30 --iters-per-ms 31800 --kind-per-tool --workers-per-node 16 --shapes-from-dragon --expect-gpus 1}" > "$RUNS"
(
  for t in ${DUMP_AT:-200 260}; do
    sleep $((t - ${prev:-0})); prev=$t
    for pid in $(pgrep -u "$USER" -f "bin/python [^ ]*experiments/e2_gpu.py"); do
      echo "== t+${t}s SIGUSR1 to e2_gpu.py pid $pid"; kill -USR1 "$pid"
    done
  done
) &
RUNS=$RUNS PS_EVERY=10 RUN_TIMEOUT_S=${RUN_TIMEOUT_S:-360} MIN_LEFT_S=240 RESULTS_DIR=$BASE/results/stall bash "$BASE/experiments/b2run.sbatch"

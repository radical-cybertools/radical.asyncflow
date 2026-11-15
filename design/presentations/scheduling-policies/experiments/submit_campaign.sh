#!/bin/bash
# Submit E1-E3 on rust as two job streams. Run on the rust login node from this directory:
#   bash submit_campaign.sh [e1|e2|e3|all]
#
# Two Dragon runtimes on one node break each other (run.sbatch's dragon-cleanup kills the
# other one's processes), so every job on a node has the same name and --dependency=singleton,
# and the jobs on that node run one at a time:
#   dj: julia, partition cpu, 64 CPUs (32 Batch workers): E3 and E1
#   dh: haskell, partition gpu, 2 GPUs, 56 CPUs (28 Batch workers): E2, stock and spaced
# Together 120 CPUs and 2 GPUs, inside QOS normal's 128 CPUs and 2 GPUs per user.
# Configurations are interleaved, in a different order in each repetition.
set -e
cd "$(dirname "$0")"
export RESULTS_DIR=/home/mturilli/tmp/sched-exp/results
WHAT=${1:-all}
CPU="-J dj -d singleton -p cpu -w julia -c 64 --mem=64G"
GPU="-J dh -d singleton -p gpu -w haskell --gres=gpu:2 -c 56 --mem=96G"
REPS="1 2 3"

# SKIP_DJ / SKIP_DH: skip the first N jobs of each stream (already run or running).
SKIP_DJ=${SKIP_DJ:-0}
SKIP_DH=${SKIP_DH:-0}
N_DJ=0
N_DH=0
sub_dj() { N_DJ=$((N_DJ + 1)); [ "$N_DJ" -le "$SKIP_DJ" ] && return; AF=$1 RH=$2 sbatch $CPU -t "$3" run.sbatch "${@:4}"; }
sub_dh() { N_DH=$((N_DH + 1)); [ "$N_DH" -le "$SKIP_DH" ] && return; AF=$1 RH=$2 sbatch $GPU -t "$3" run.sbatch "${@:4}"; }

order() {  # order <rep> <items...>: the items in a fixed per-rep shuffle
  local rep=$1; shift
  python3 -c "import random, sys; x = sys.argv[2:]; random.Random(int(sys.argv[1])).shuffle(x); print(' '.join(x))" "$rep" "$@"
}

if [ "$WHAT" = e3 ] || [ "$WHAT" = all ]; then
  for rep in $REPS; do
    for combo in $(order "$rep" A B C); do
      case $combo in
        A) sub_dj af-ordering rh-fix 30 e3_overhead.py --configs none,fifo,lineage,fair --rep "$rep" ;;
        B) sub_dj af-resource rh-fix 30 e3_overhead.py --configs none,res-agg,res-node --rep "$rep" ;;
        C) sub_dj af-resource rh-merged 30 e3_overhead.py --configs none,res-node --rep "$rep" ;;
      esac
    done
  done
fi

if [ "$WHAT" = e1 ] || [ "$WHAT" = all ]; then
  MIXED="--workload mixed --bulk 1000 --bulk-min 1 --bulk-max 5 --chain 3 --step 0.5 --period 6 --nchains 12"
  DIAMONDS="--workload diamonds --diamonds 256 --dur 1.0"
  for rep in $REPS; do
    for run in $(order "$rep" none:0 fifo:0 lineage:0 fair:0 none:8 fifo:8 lineage:8 fair:8); do
      sub_dj af-ordering rh-fix 15 e1_ordering.py $MIXED \
        --config "${run%%:*}" --spaced-ms "${run##*:}" --rep "$rep"
    done
    for run in $(order "$((rep + 10))" none:0 fifo:0 lineage:0 fair:0 none:8 fifo:8 lineage:8 fair:8); do
      sub_dj af-ordering rh-fix 10 e1_ordering.py $DIAMONDS \
        --config "${run%%:*}" --spaced-ms "${run##*:}" --rep "$rep"
    done
  done
fi

if [ "$WHAT" = e2 ] || [ "$WHAT" = all ]; then
  for rep in $REPS; do
    for mode in $(order "$((rep + 20))" none res-agg res-node); do
      sub_dh af-resource rh-merged 25 e2_gpu.py --config "$mode" --rep "$rep" \
        --pipelines 24 --window 720
    done
  done
  # The same with submissions 8 ms apart, so Batch reports each task when it ends.
  for rep in $REPS; do
    for mode in $(order "$((rep + 30))" none res-agg res-node); do
      sub_dh af-resource rh-merged 25 e2_gpu.py --config "$mode" --rep "$rep" \
        --pipelines 24 --window 720 --spaced-ms 8
    done
  done
fi
squeue -u "$USER" -o "%.8i %.4j %.9P %.2t %.10M %.4C %R"

#!/bin/bash
# Fourth campaign: Batch's completion hand-off without the client worker's 50 ms wait (switch e).
# Run on the rust login node from this directory:  bash submit_campaign4.sh
#
# One stream on julia (-J d2 -d singleton), 64 CPUs, so 32 Batch workers (RHAPSODY's pool cap),
# capacity 32, as in the second campaign. Trees: asyncflow af-prio (641ee65), RHAPSODY
# rh-place-prio (14faf2f). Dragon: the branch experiment/batch-reports-priority at 0a7d87d
# (dbf5405 + switch e), built in $BRANCH. Variants: (a)+(b) against (a)+(b)+(e), interleaved in a
# fixed per-repetition shuffle. Repetition 1 gets twice the time limit.
set -e
cd "$(dirname "$0")"
export RESULTS_DIR=/home/mturilli/tmp/sched-exp/r4/results
mkdir -p "$RESULTS_DIR"
BRANCH=/home/mturilli/tmp/sched-exp/dragon-exp
CPU="-J d2 -d singleton -p cpu -w julia -c 64 --mem=64G"
MIXED="--workload mixed --bulk 1000 --bulk-min 1 --bulk-max 5 --chain 3 --step 0.5 --period 6 --nchains 12"

order() {  # order <seed> <items...>: the items in a fixed per-seed shuffle
  local seed=$1; shift
  python3 -c "import random, sys; x = sys.argv[2:]; random.Random(int(sys.argv[1])).shuffle(x); print(' '.join(x))" "$seed" "$@"
}

limit() {  # limit <rep> <minutes>: twice the minutes for repetition 1
  [ "$1" = 1 ] && echo $(( $2 * 2 )) || echo "$2"
}

run() {  # run <minutes> <script> [args]
  local minutes=$1; shift
  DRAGON_TREE=$BRANCH AF=af-prio RH=rh-place-prio sbatch $CPU -t "$minutes" run.sbatch "$@"
}

for rep in 1 2 3; do
  jobs=$(order "$((rep + 100))" e3:+a+b e3:+a+b+e e1:none+a+b e1:none+a+b+e e1:fair+a+b e1:fair+a+b+e)
  for j in $jobs; do
    kind=${j%%:*} spec=${j#*:}
    if [ "$kind" = e3 ]; then
      run "$(limit "$rep" 10)" e3_overhead.py --configs "none$spec,fair$spec,lineage$spec" --rep "$rep"
    else
      run "$(limit "$rep" 10)" e1_ordering.py $MIXED --config "$spec" --rep "$rep"
    fi
  done
done
squeue -u "$USER" -o "%.8i %.4j %.9P %.2t %.10M %.4C %R" | head -5
echo "$(squeue -h -u "$USER" | wc -l) jobs queued"

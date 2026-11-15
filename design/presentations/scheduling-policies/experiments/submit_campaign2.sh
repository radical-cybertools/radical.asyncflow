#!/bin/bash
# Second campaign: prompt reports, priorities and a capacity above the pool size.
# Run on the rust login node from this directory:  bash submit_campaign2.sh [e1a|e1b|e3|all|split]
#
# One stream on julia (-J dj -d singleton), 64 CPUs, so 32 Batch workers, as in the first campaign.
# Every run uses asyncflow experiment/policy-priority (af-prio, 641ee65) and RHAPSODY
# experiment/dragon-priority (rh-prio, 738288e); with their new options off they are the first
# campaign's code. Dragon is either the stock dragonhpc 0.14.1 wheel or, with DRAGON_TREE, the
# branch experiment/batch-reports-priority (dbf5405) built by build_dragon.sh.
# Configuration specs: see common.parse_spec. Repetition 1 gets twice the time limit.
set -e
cd "$(dirname "$0")"
export RESULTS_DIR=/home/mturilli/tmp/sched-exp/r2/results
mkdir -p "$RESULTS_DIR"
WHAT=${1:-all}
BRANCH=/home/mturilli/tmp/sched-exp/dragon-exp
CPU="-J dj -d singleton -p cpu -w julia -c 64 --mem=64G"
REPS="1 2 3"
P="+a+b+d+prio"  # the branch with every switch and the policy's priorities

order() {  # order <seed> <items...>: the items in a fixed per-seed shuffle
  local seed=$1; shift
  python3 -c "import random, sys; x = sys.argv[2:]; random.Random(int(sys.argv[1])).shuffle(x); print(' '.join(x))" "$seed" "$@"
}

limit() {  # limit <rep> <minutes>: twice the minutes for repetition 1
  [ "$1" = 1 ] && echo $(( $2 * 2 )) || echo "$2"
}

# run <stock|branch> <minutes> <script> [args]
run() {
  local dragon=$1 minutes=$2; shift 2
  if [ "$dragon" = branch ]; then
    DRAGON_TREE=$BRANCH AF=af-prio RH=rh-prio sbatch $CPU -t "$minutes" run.sbatch "$@"
  else
    DRAGON_TREE= AF=af-prio RH=rh-prio sbatch $CPU -t "$minutes" run.sbatch "$@"
  fi
}

E1A_STOCK="none fair none+each fair+each fair@128+each"
E1A_BRANCH="none+a+b fair+a+b fair@32$P fair@64$P fair@128$P fair@inf$P fifo@128$P"
E1B_STOCK="none lineage none+each lineage+each lineage@128+each"
E1B_BRANCH="none+a+b lineage+a+b lineage@32$P lineage@64$P lineage@128$P lineage@inf$P"
E3_STOCK="none,fair,lineage,none+each,fair+each,fair@128+each,lineage+each,lineage@128+each"
E3_BRANCH="none+a+b,fair+a+b,lineage+a+b,fair@32$P,fair@64$P,fair@128$P,fair@inf$P,lineage@32$P,lineage@64$P,lineage@128$P,lineage@inf$P,fifo@128$P"
MIXED="--workload mixed --bulk 1000 --bulk-min 1 --bulk-max 5 --chain 3 --step 0.5 --period 6 --nchains 12"
DIAMONDS="--workload diamonds --diamonds 256 --dur 1.0"

for rep in $REPS; do
  [ "$WHAT" = split ] && break
  if [ "$WHAT" = e3 ] || [ "$WHAT" = all ]; then
    for d in $(order "$((rep + 40))" stock branch); do
      configs=$E3_STOCK; [ "$d" = branch ] && configs=$E3_BRANCH
      run "$d" "$(limit "$rep" 30)" e3_overhead.py --configs "$configs" --rep "$rep"
    done
  fi
  if [ "$WHAT" = e1a ] || [ "$WHAT" = all ]; then
    for c in $(order "$((rep + 50))" $(printf 'stock:%s ' $E1A_STOCK) $(printf 'branch:%s ' $E1A_BRANCH)); do
      run "${c%%:*}" "$(limit "$rep" 15)" e1_ordering.py $MIXED --config "${c#*:}" --rep "$rep"
    done
  fi
  if [ "$WHAT" = e1b ] || [ "$WHAT" = all ]; then
    for c in $(order "$((rep + 60))" $(printf 'stock:%s ' $E1B_STOCK) $(printf 'branch:%s ' $E1B_BRANCH)); do
      run "${c%%:*}" "$(limit "$rep" 10)" e1_ordering.py $DIAMONDS --config "${c#*:}" --rep "$rep"
    done
  fi
done
# Added after repetition 1 of E1a, to separate (b) from (a): reports without one-task compiles,
# and one-task compiles without reports (submitted after the main matrix, 3 repetitions).
if [ "$WHAT" = split ]; then
  for rep in $REPS; do
    run branch "$(limit "$rep" 15)" e3_overhead.py --configs "none+b,fair+b,lineage+b,fair@128+b,none+a" --rep "$rep"
    for c in $(order "$((rep + 70))" none+b fair+b); do
      run branch "$(limit "$rep" 15)" e1_ordering.py $MIXED --config "$c" --rep "$rep"
    done
  done
fi
squeue -u "$USER" -o "%.8i %.4j %.9P %.2t %.10M %.4C %R" | head -5
echo "$(squeue -h -u "$USER" | wc -l) jobs queued"

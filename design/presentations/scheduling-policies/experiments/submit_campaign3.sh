#!/bin/bash
# Third campaign: one Dragon runtime over two nodes. Run on the rust login node from this
# directory, one wave at a time (squeue is checked before each GPU wave):
#   bash submit_campaign3.sh a-branch <rep>   E2 on haskell + fortran, Dragon branch (a)+(b)
#   bash submit_campaign3.sh a-stock <rep>    E2 on haskell + fortran, stock dragonhpc 0.14.1
#   bash submit_campaign3.sh b <rep>          E1a, E1b and E3 on haskell + julia
#
# One job stream (-J d2 -d singleton) for both parts, so two Dragon jobs of ours never share a
# node. Part A is a heterogeneous job: haskell (partition gpu) and fortran (hidden partition
# staging) share no partition. 28 CPUs and one GPU on each: 14 Batch workers per node.
# Part B is a plain two-node job in partition cpu: 32 CPUs per node, 16 workers per node, 32
# in all, as the one-node runs on julia (64 CPUs, 32 workers). Capacity "pool" is the total.
#
# Trees: asyncflow af-resource (6285975) for E2, af-prio (641ee65) for E1/E3; RHAPSODY
# rh-place-prio (14faf2f: placement 762c5b3 merged into experiment/dragon-priority 738288e).
# Dragon: the stock wheel, or the branch experiment/batch-reports-priority (dbf5405) built in
# $BRANCH. Repetition 1 of Part B gets twice the time limit; every fortran job stays under 25 min.
set -e
cd "$(dirname "$0")"
export RESULTS_DIR=/home/mturilli/tmp/sched-exp/r3/results
mkdir -p "$RESULTS_DIR"
WHAT=${1:?usage: submit_campaign3.sh a-branch|a-stock|b <rep>}
REP=${2:?rep}
BRANCH=/home/mturilli/tmp/sched-exp/dragon-exp
het() {  # het <minutes>: the two components, each with the time limit
  echo "-J d2 -d singleton -t $1 -p gpu -w haskell --gres=gpu:1 -c 28 --mem=64G : -t $1 -p staging -w fortran --gres=gpu:1 -c 28 --mem=64G"
}
CPU2="-J d2 -d singleton -p cpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 32 --mem=64G"

order() {  # order <seed> <items...>: the items in a fixed per-seed shuffle
  local seed=$1; shift
  python3 -c "import random, sys; x = sys.argv[2:]; random.Random(int(sys.argv[1])).shuffle(x); print(' '.join(x))" "$seed" "$@"
}

limit() {  # limit <rep> <minutes>: twice the minutes for repetition 1
  [ "$1" = 1 ] && echo $(( $2 * 2 )) || echo "$2"
}

# E2: 24 pipelines for 720 s; GPU memory x1.5 so haskell's 47 GiB binds before the 141 GiB total.
E2="--mem-scale 1.5 --gpu-mem haskell=47.0,fortran=94.5 --iters-per-ms-by-gpu Ada=79000,Blackwell=69000"
MIXED="--workload mixed --bulk 1000 --bulk-min 1 --bulk-max 5 --chain 3 --step 0.5 --period 6 --nchains 12"
DIAMONDS="--workload diamonds --diamonds 256 --dur 1.0"

case $WHAT in
  a-branch)
    for m in $(order "$((REP + 80))" none res-agg res-node); do
      DRAGON_TREE=$BRANCH AF=af-resource RH=rh-place-prio sbatch $(het 24) run2n.sbatch \
        e2_gpu.py --config "$m+a+b" --rep "$REP" $E2
    done ;;
  a-stock)
    for m in $(order "$((REP + 90))" none res-agg res-node); do
      DRAGON_TREE= AF=af-resource RH=rh-place-prio sbatch $(het 24) run2n.sbatch \
        e2_gpu.py --config "$m" --rep "$REP" $E2
    done ;;
  b)
    run() {  # run <stock|branch> <minutes> <script> [args]
      local d=$1 minutes=$2; shift 2
      local tree=; [ "$d" = branch ] && tree=$BRANCH
      DRAGON_TREE=$tree AF=af-prio RH=rh-place-prio sbatch $CPU2 -t "$minutes" run2n.sbatch "$@"
    }
    for d in $(order "$((REP + 40))" stock branch); do
      configs="none+each,fair+each,lineage+each"; [ "$d" = branch ] && configs="none+a+b,fair+a+b,lineage+a+b"
      run "$d" "$(limit "$REP" 15)" e3_overhead.py --configs "$configs" --rep "$REP"
    done
    for c in $(order "$((REP + 50))" stock:none+each stock:fair+each branch:none+a+b branch:fair+a+b); do
      run "${c%%:*}" "$(limit "$REP" 15)" e1_ordering.py $MIXED --config "${c#*:}" --rep "$REP"
    done
    for c in $(order "$((REP + 60))" stock:none+each stock:lineage+each branch:none+a+b branch:lineage+a+b); do
      run "${c%%:*}" "$(limit "$REP" 10)" e1_ordering.py $DIAMONDS --config "${c#*:}" --rep "$REP"
    done ;;
  *) echo "unknown wave $WHAT"; exit 1 ;;
esac
squeue -u "$USER" -o "%.10i %.4j %.9P %.2t %.10M %.4C %R" | head -12
echo "$(squeue -h -u "$USER" | wc -l) job components queued"

#!/bin/bash
# Fifth campaign: submissions on PSC Bridges-2. Run on a login node from this directory:
#   bash b2_submit.sh e2 <rep>         # 4 v100-32 nodes (32 GPUs, as S1's 8 Delta nodes), the 4 E2 modes shuffled, one job
#   bash b2_submit.sh raw              # 3a: raw Batch in 1-, 2- and 4-node runtimes, one 4-node job
#   bash b2_submit.sh af <nodes> <rep> # 3b: E1a, E1b, E3 on 2 or 4 RM nodes, branch (a)+(b)+(e)
#   bash b2_submit.sh i95              # 4: rhapsody#95 on RM-shared, 4 and 16 cores
#   bash b2_submit.sh e1one [workers]  # follow-ups 2 and 3: rust's E1a on one RM node, 32 (or 64) workers
# Every job is in one stream (-J d2 -d singleton) and charged to dmr170002p. JOBNAME names another
# stream: d2r for the RM sections, so they need not wait behind a held or pending GPU job. A RUNS
# file per job goes to $BASE/runs/plans; b2run.sbatch runs its lines in order, one Dragon runtime each.
set -e
cd "$(dirname "$0")"
BASE=/ocean/projects/dmr170002p/mturilli/sched-exp
PLANS=$BASE/runs/plans
mkdir -p "$PLANS"
STREAM="-A dmr170002p -J ${JOBNAME:-d2} -d singleton"

order() {  # order <seed> <items...>: the items in a fixed per-seed shuffle
  local seed=$1; shift
  python3 -c "import random, sys; x = sys.argv[2:]; random.Random(int(sys.argv[1])).shuffle(x); print(' '.join(x))" "$seed" "$@"
}

submit() {  # submit <env> <rhapsody tree> <plan file> <sbatch options...>
  local env=$1 rh=$2 plan=$3; shift 3
  squeue -a -u "$USER" -h -o "%i %j %P %T"
  sbatch --export=ALL,DRAGON_ENV=$env,AF=af-resource,RH=$rh,RUNS=$plan,RUN_TIMEOUT_S=${RUN_TIMEOUT_S:-},MIN_LEFT_S=${MIN_LEFT_S:-600},RESULTS_DIR=$BASE/results/${RES:?} \
    $STREAM "$@" b2run.sbatch
}

case $1 in
e2)
  rep=${2:?rep}
  # V100 calibration from smoke test 1 (job 49819709): 31,800 iterations/ms, a context takes 0.30 GiB
  # of the 31.73 GiB. Memory scaled by 32/48 (V100 32 GB against the S1 model's A40 48 GB).
  E2="--rep $rep --pipelines 256 --window 720 --mem-scale 0.667 --node-gpu-mem 31.0 --ctx-gib 0.30"
  E2="$E2 --iters-per-ms 31800 --kind-per-tool --workers-per-node 96 --expect-gpus 8 --pin-mod 8"
  plan=$PLANS/e2_r$rep.txt
  : > "$plan"
  for m in $(order "$((rep + 200))" none res-agg res-node res-learn); do
    extra=
    [ "$m" = res-learn ] && extra="--unknown-per-node 8"
    echo "e2_gpu.py --config $m $E2 $extra" >> "$plan"
  done
  cat "$plan"
  RES=e2 RUN_TIMEOUT_S=1500 submit stock rh-b2 "$plan" -p GPU -N 4 --gpus=v100-32:32 --ntasks-per-node=1 -c 40 -t 110
  ;;
raw)
  AB='{"client_linger_sec":0,"client_max_batch_size":1,"report_each_task":true,"client_wake_on_completion":true}'
  plan=$PLANS/raw.txt
  : > "$plan"
  for n in 1 2 4; do
    layouts=$(seq -s, 1 $n | sed 's/,3//')
    echo "env=stock nodes=$n diag_twonode.py --layouts $layouts --reps 3 --tag stock-n$n" >> "$plan"
    echo "env=branch nodes=$n diag_twonode.py --layouts $layouts --reps 3 --batch-kwargs $AB --tag branch-n$n" >> "$plan"
  done
  for n in 1 2 4; do
    echo "env=stock nodes=$n log=DEBUG diag_twonode.py --layouts $n --reps 1 --tag stock-n$n-debug" >> "$plan"
  done
  cat "$plan"
  RES=raw RUN_TIMEOUT_S=600 submit stock rh-b2 "$plan" -p RM -N 4 --ntasks-per-node=1 -c 128 -t 60
  ;;
af)
  nodes=${2:?nodes} rep=${3:?rep}
  workers=$((64 * nodes))
  # rust ran these on 32 workers; the bulk and the diamonds scale with the workers, so the pool
  # is as contended as there.
  MIXED="--workload mixed --bulk $((1000 * workers / 32)) --bulk-min 1 --bulk-max 5 --chain 3 --step 0.5 --period 6 --nchains 12"
  DIAMONDS="--workload diamonds --diamonds $((256 * workers / 32)) --dur 1.0"
  S=+a+b+e
  plan=$PLANS/af_n${nodes}_r$rep.txt
  : > "$plan"
  for j in $(order "$((rep * 10 + nodes + 300))" e3 e1a:none e1a:fair e1b:none e1b:lineage); do
    case $j in
      e3) echo "e3_overhead.py --configs none$S,fair$S,lineage$S --rep $rep" ;;
      e1a:*) echo "e1_ordering.py $MIXED --config ${j#*:}$S --rep $rep" ;;
      e1b:*) echo "e1_ordering.py $DIAMONDS --config ${j#*:}$S --rep $rep" ;;
    esac >> "$plan"
  done
  cat "$plan"
  limit=40; [ "$rep" = 1 ] && limit=60
  RES=af RUN_TIMEOUT_S=900 submit branch rh-b2 "$plan" -p RM -N "$nodes" --ntasks-per-node=1 -c 128 -t $limit
  ;;
i95)
  # One run per job: a hung 0.6.0 shutdown cannot leave processes for the next run.
  for c in 4 16; do
    for rep in 1 2; do
      for rh in rh-fix rh-0.6.0; do
        plan=$PLANS/i95_c${c}_${rh}_r$rep.txt
        echo "rh=$rh b2_issue95.py --config $rh --rep $rep" > "$plan"
        RES=i95 RUN_TIMEOUT_S=1080 MIN_LEFT_S=300 submit stock "$rh" "$plan" -p RM-shared -N 1 --ntasks-per-node=1 -c "$c" -t 25
      done
    done
  done
  ;;
e1one)
  # rust's E1a (1,000 bulk tasks, 32 workers, capacity 32) on one RM node: is 3b's 45-56 s
  # no-policy chain scale or site? No policy and Fair alternate first per rep, as in 3b.
  # A workers argument (64) keeps 1,000 bulk tasks per 32 workers: worker count against spanning nodes.
  w=${2:-32}
  MIXED="--workload mixed --bulk $((1000 * w / 32)) --bulk-min 1 --bulk-max 5 --chain 3 --step 0.5 --period 6 --nchains 12"
  plan=$PLANS/e1one.txt
  [ "$w" = 32 ] || plan=$PLANS/e1one_w$w.txt
  : > "$plan"
  for rep in 1 2 3; do
    pols="none fair"; [ "$rep" = 2 ] && pols="fair none"
    for p in $pols; do
      echo "e1_ordering.py $MIXED --config $p+a+b+e --rep $rep --workers-per-node $w" >> "$plan"
    done
  done
  cat "$plan"
  RES=e1one RUN_TIMEOUT_S=600 submit branch rh-b2 "$plan" -p RM -N 1 --ntasks-per-node=1 -c 128 -t 30
  ;;
*)
  echo "usage: $0 e2 <rep> | raw | af <nodes> <rep> | i95 | e1one [workers]"; exit 2 ;;
esac

#!/bin/bash
# Policy experiments on PSC Bridges-2 (fifth campaign): one or more Dragon runs in one job,
# one Dragon runtime per run, over all the job's nodes.
#
#   DRAGON_ENV=stock AF=af-resource RH=rh-b2 sbatch -A dmr170002p -J d2 -d singleton \
#     -p GPU -N 8 --gpus=v100-32:64 --ntasks-per-node=1 -c 40 -t 100 \
#     b2run.sbatch e2_gpu.py --config res-node --rep 1 ...
#   RUNS=<file> ... sbatch ... b2run.sbatch      # one run per line: [key=value ...] <script.py> [args]
# Leading key=value tokens of a RUNS line apply to that run only: env=<stock|branch> (the
# Dragon venv), rh=<tree> (RHAPSODY), nodes=<n> (dragon -N: the runtime's first n nodes of
# the job), log=<level> (dragon -l, e.g. DEBUG; its logs go to the run dir).
#
# As run2n.sbatch on rust, adapted to Bridges-2:
# - paths under /ocean/projects/dmr170002p/mturilli/sched-exp; trees in trees/ (git archive,
#   each with a COMMIT file);
# - DRAGON_ENV names a venv in envs/: "stock" is the dragonhpc 0.14.1 wheel, "branch" the same
#   wheel with batch.py from experiment/batch-reports-priority 0a7d87d (envs/branch/DRAGON_BATCH_COMMIT);
# - TCP transport over IPoIB (ib0);
# - a plain multi-node job: Dragon's own srun runs with srun-hetjob first on PATH, which only
#   adds --overlap, so that Dragon's step shares the CPUs with the samplers. -c must be the
#   node's cores, so that Dragon's step (which inherits it) may use all of them;
# - several runs per job (RUNS): each has its own run dir and result dir, and between runs
#   this job's /dev/shm files are removed on every node and Dragon processes counted.
# RUN_TIMEOUT_S caps each run (SIGINT, then SIGKILL 60 s later); MIN_LEFT_S skips a run that
# would start with less time left (default 600 s).
# Courtesy: one job stream (-J d2 -d singleton), so two of our Dragon jobs never share a node
# (RM-shared nodes are shared with other users). No dragon-cleanup: only our own /dev/shm
# files created since the run started are removed.
#SBATCH --output=/ocean/projects/dmr170002p/mturilli/sched-exp/logs/%x-%j.out

BASE=/ocean/projects/dmr170002p/mturilli/sched-exp
RESULTS=${RESULTS_DIR:-$BASE/results}
JOBRUN=$BASE/runs/$SLURM_JOB_ID
mkdir -p "$JOBRUN" "$RESULTS"
if [ -n "$RUNS" ]; then
  mapfile -t LINES < "$RUNS"
  cp "$RUNS" "$JOBRUN/runs.txt"
else
  LINES=("$*")
fi

use_env() {  # use_env <stock|branch> <rhapsody tree>: the Dragon venv and the trees for a run
  [ -n "$VIRTUAL_ENV" ] && deactivate
  source "$BASE/envs/$1/bin/activate"
  DV="dragonhpc $(python -c 'import importlib.metadata as m; print(m.version("dragonhpc"))') wheel"
  [ -f "$BASE/envs/$1/DRAGON_BATCH_COMMIT" ] && DV="$DV + batch.py $(cat "$BASE/envs/$1/DRAGON_BATCH_COMMIT")"
  export DRAGON_VERSION=$DV
  export ASYNCFLOW_COMMIT=$(cat "$BASE/trees/${AF:?}/COMMIT")
  export RHAPSODY_COMMIT=$(cat "$BASE/trees/$2/COMMIT")
  export PYTHONPATH=$BASE/experiments:$BASE/trees/$AF/src:$BASE/trees/$2/src
}
use_env "${DRAGON_ENV:?stock or branch}" "${RH:?}"
export PYTHONUNBUFFERED=1

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
NODELIST=$(IFS=,; echo "${NODES[*]}")
on_node() {  # on_node <host> <command...>: one task on that node, inside this job
  local h=$1; shift
  srun --overlap --nodes=1 --nodelist="$h" --ntasks=1 --cpu_bind=none "$@"
}
echo "== job $SLURM_JOB_ID on $NODELIST: partition=$SLURM_JOB_PARTITION cpus/task=$SLURM_CPUS_PER_TASK gpus=${SLURM_GPUS:-${SLURM_JOB_GPUS:-none}}"
echo "== $DRAGON_VERSION; asyncflow $AF $ASYNCFLOW_COMMIT; rhapsody $RH $RHAPSODY_COMMIT; ${#LINES[@]} run(s)"

NETWORK_PREFIX='^(ib)\w*$'
SHIM=$JOBRUN/shim
mkdir -p "$SHIM"
cp "$BASE/experiments/srun-hetjob" "$SHIM/srun"
chmod +x "$SHIM/srun"

left_s() {
  squeue -h -j "$SLURM_JOB_ID" -o %L | head -1 | awk '{d=0; t=$1; if (index(t, "-")) {split(t, a, "-"); d=a[1]; t=a[2]}
    n=split(t, p, ":"); s=0; for (i=1; i<=n; i++) s=s*60+p[i]; print d*86400+s}'
}

[ "${#LINES[@]}" -eq 0 ] || [ -z "${LINES[*]// }" ] && { echo "== nothing to run (RUNS unset and no arguments)"; exit 2; }
k=0
for line in "${LINES[@]}"; do
  [ -z "${line// }" ] && continue
  case $line in \#*) continue ;; esac
  k=$((k + 1))
  read -r -a ARGV <<< "$line"
  R_ENV=$DRAGON_ENV R_RH=$RH DRAGON_OPTS=()
  while [[ ${ARGV[0]} == *=* ]]; do
    case ${ARGV[0]} in
      env=*) R_ENV=${ARGV[0]#env=} ;;
      rh=*) R_RH=${ARGV[0]#rh=} ;;
      nodes=*) DRAGON_OPTS+=(-N "${ARGV[0]#nodes=}") ;;
      log=*) DRAGON_OPTS+=(-l "${ARGV[0]#log=}") ;;
      *) echo "== unknown token ${ARGV[0]}"; exit 2 ;;
    esac
    ARGV=("${ARGV[@]:1}")
  done
  use_env "$R_ENV" "$R_RH"
  SCRIPT=${ARGV[0]}
  RUN=$JOBRUN/$k
  mkdir -p "$RUN/out"
  export RUN_DIR=$RUN RESULTS_DIR=$RUN/out
  LEFT=$(left_s)
  if [ "${LEFT:-0}" -lt "${MIN_LEFT_S:-600}" ] 2>/dev/null; then
    echo "== run $k skipped: ${LEFT}s left in the job: $line"
    continue
  fi
  TLIMIT=$((LEFT - 150))
  [ -n "$RUN_TIMEOUT_S" ] && [ "$RUN_TIMEOUT_S" -lt "$TLIMIT" ] && TLIMIT=$RUN_TIMEOUT_S
  RUN_START=$(date +%s)
  echo "== run $k at $(date '+%F %T %Z'): $line"
  echo "== run $k: $DRAGON_VERSION; rhapsody $R_RH $RHAPSODY_COMMIT; dragon ${DRAGON_OPTS[*]}"
  squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" > "$RUN/squeue_start.txt"
  date +%z > "$RUN/tz.txt"
  SAMPLERS=()
  for h in "${NODES[@]}"; do
    PS_EVERY=${PS_EVERY:-30} on_node "$h" bash "$BASE/experiments/node_sampler.sh" "$RUN" > /dev/null 2>&1 &
    SAMPLERS+=($!)
  done
  SECONDS=0
  (cd "$RUN" && SRUN_HETJOB_LOG=$RUN/srun-hetjob.log PATH=$SHIM:$PATH timeout --signal=INT --kill-after=60 $TLIMIT \
    dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" "${DRAGON_OPTS[@]}" "$BASE/experiments/$SCRIPT" "${ARGV[@]:1}")
  RC=$?
  WALL=$SECONDS
  echo "== run $k dragon rc=$RC wall=${WALL}s"
  kill -TERM "${SAMPLERS[@]}" 2>/dev/null
  sleep 2
  squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" > "$RUN/squeue_end.txt"
  python "$BASE/experiments/merge_meta.py" "$RUN" "$RUN/out" "$SLURM_JOB_ID" "$RC" "$WALL"
  for p in "$RUN"/out/*.json; do
    [ -e "$p" ] || continue
    b=$(basename "$p")
    [ "$b" = "${SLURM_JOB_ID}_noresult.json" ] && b="${SLURM_JOB_ID}_${k}_noresult.json"
    [ -e "$RESULTS/$b" ] && b="${b%.json}_${k}.json"  # same mode twice in one job
    mv "$p" "$RESULTS/$b"
  done
  # This run's /dev/shm files on each node, and a count of Dragon processes left.
  for h in "${NODES[@]}"; do
    timeout -k 5 60 srun --overlap --nodes=1 --nodelist="$h" --ntasks=1 --cpu_bind=none bash -c "find /dev/shm -mindepth 1 -maxdepth 1 -user \$USER -newermt @$RUN_START -delete; \
      echo \"== cleanup \$(hostname): procs=\$(pgrep -u \$USER -fc '[d]ragon-') shm=\$(find /dev/shm -mindepth 1 -maxdepth 1 -user \$USER | wc -l)\""
  done
done
echo "== job done at $(date '+%F %T %Z')"

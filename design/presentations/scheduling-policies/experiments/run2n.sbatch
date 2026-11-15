#!/bin/bash
# One run of a policy experiment with one Dragon runtime over two nodes (third campaign).
#
# A heterogeneous job, for E2 (haskell in partition gpu and fortran in the hidden partition
# staging share no partition):
#   DRAGON_TREE=$BASE/dragon-exp AF=af-resource RH=rh-place-prio sbatch -J d2 -d singleton -t 24 \
#     -p gpu -w haskell --gres=gpu:1 -c 28 --mem=64G : -p staging -w fortran --gres=gpu:1 -c 28 --mem=64G \
#     run2n.sbatch e2_gpu.py --config res-node+a+b --rep 1 ...
# A two-node job, for E1 and E3 (haskell and julia in partition cpu):
#   ... sbatch -J d2 -d singleton -p cpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 32 --mem=64G \
#     run2n.sbatch e3_overhead.py ...
#
# As run.sbatch (same trees, Dragon builds, result files and node state), except:
# - every srun here uses --overlap and an explicit --ntasks: in a heterogeneous job on rust a
#   step without either starts but never runs its task (jobs 24780, 24782, 24786);
# - Dragon runs with srun-hetjob first on PATH. In a heterogeneous job it turns Dragon's
#   one-step srun over N nodes into one step over every component; in a plain job it only adds
#   --overlap;
# - node_sampler.sh runs on every node; merge_meta.py adds its files per host.
#SBATCH --output=/home/mturilli/tmp/sched-exp/logs/%x-%j.out

SCRIPT=${1:?usage: run2n.sbatch <script.py> [args]}
shift
JOB_START=$(date +%s)
BASE=/home/mturilli/tmp/sched-exp
RUN=$BASE/runs/$SLURM_JOB_ID
mkdir -p "$RUN"
export RUN_DIR=$RUN
export RESULTS_DIR=${RESULTS_DIR:-$BASE/r3/results}

NODES=() SEL=()
if [ -n "$SLURM_HET_SIZE" ]; then
  for g in $(seq 0 $((SLURM_HET_SIZE - 1))); do
    v=SLURM_JOB_NODELIST_HET_GROUP_$g
    NODES+=("${!v}") SEL+=("--het-group=$g")
  done
else
  for h in $(scontrol show hostnames "$SLURM_JOB_NODELIST"); do NODES+=("$h") SEL+=("--nodes=1 --nodelist=$h"); done
fi
on_node() {  # on_node <index> <command...>: one task on that node, inside this job
  local i=$1; shift
  srun --overlap ${SEL[$i]} --ntasks=1 --cpu_bind=none "$@"
}
NODELIST=$(IFS=,; echo "${NODES[*]}")

if [ -n "$DRAGON_TREE" ]; then
  source "$DRAGON_TREE/_env/bin/activate"
  source "$DRAGON_TREE/devtools/VARIABLES"
  source /cvmfs/software.eessi.io/versions/2023.06/init/bash >/dev/null
  module load GCCcore/13.2.0
  export LD_LIBRARY_PATH="$EBROOTGCCCORE/lib64:$LD_LIBRARY_PATH"
  export DRAGON_VERSION="dragon source tree $(cat "$DRAGON_TREE/COMMIT")"
else
  source /home/mturilli/tmp/dragon-rust/venv/bin/activate
  export DRAGON_VERSION="dragonhpc $(pip show dragonhpc 2>/dev/null | awk '/^Version/{print $2}') wheel"
fi
export ASYNCFLOW_COMMIT=$(cat "$BASE/trees/${AF:?}/COMMIT")
export RHAPSODY_COMMIT=$(cat "$BASE/trees/${RH:?}/COMMIT")
export PYTHONPATH=$BASE/experiments:$BASE/trees/$AF/src:$BASE/trees/$RH/src
export PYTHONUNBUFFERED=1
echo "== job $SLURM_JOB_ID $SCRIPT $* on $NODELIST (het size ${SLURM_HET_SIZE:-none}): batch on $(hostname)"
echo "== $DRAGON_VERSION; asyncflow $AF $ASYNCFLOW_COMMIT; rhapsody $RH $RHAPSODY_COMMIT"

squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" > "$RUN/squeue_start.txt"
date +%z > "$RUN/tz.txt"
SAMPLERS=()
for i in "${!NODES[@]}"; do
  on_node "$i" bash "$BASE/experiments/node_sampler.sh" "$RUN" > /dev/null 2>&1 &
  SAMPLERS+=($!)
done

NETWORK_PREFIX='^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'
LEFT=$(squeue -h -j "$SLURM_JOB_ID" -o %L | head -1 | awk '{d=0; t=$1; if (index(t, "-")) {split(t, a, "-"); d=a[1]; t=a[2]}
  n=split(t, p, ":"); s=0; for (i=1; i<=n; i++) s=s*60+p[i]; print d*86400+s}')
[ "${LEFT:-0}" -gt 300 ] 2>/dev/null || LEFT=1800
SHIM=$RUN/shim
mkdir -p "$SHIM"
cp "$BASE/experiments/srun-hetjob" "$SHIM/srun"
chmod +x "$SHIM/srun"
SECONDS=0
cd "$RUN" || exit 1
PATH=$SHIM:$PATH timeout --signal=INT --kill-after=60 $((LEFT - 120)) \
  dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" "$BASE/experiments/$SCRIPT" "$@"
RC=$?
echo "== dragon rc=$RC wall=${SECONDS}s"
kill -TERM "${SAMPLERS[@]}" 2>/dev/null
sleep 2
squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" > "$RUN/squeue_end.txt"
python "$BASE/experiments/merge_meta.py" "$RUN" "$RESULTS_DIR" "$SLURM_JOB_ID" "$RC" "$SECONDS"

# This job's /dev/shm files on each node (one Dragon job of ours per node: -J d2 -d singleton).
# Slurm kills the job's processes. No dragon-cleanup (see run.sbatch).
for i in "${!NODES[@]}"; do
  timeout -k 5 60 srun --overlap ${SEL[$i]} --ntasks=1 --cpu_bind=none bash -c "find /dev/shm -maxdepth 1 -user \$USER -newermt @$JOB_START -delete; \
    echo \"== cleanup \$(hostname): procs=\$(pgrep -u \$USER -fc '[d]ragon-') shm=\$(find /dev/shm -maxdepth 1 -user \$USER | wc -l)\""
done

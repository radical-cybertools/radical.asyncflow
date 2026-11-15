#!/bin/bash
# One run of a policy experiment on rust, under stock dragonhpc 0.14.1.
#
#   AF=af-ordering RH=rh-fix sbatch -J e1 -c 64 run.sbatch e1_ordering.py --config fair --rep 1 ...
#
# AF and RH name trees in $BASE/trees (exported with git archive; each has a COMMIT file):
#   af-ordering  asyncflow feature/ordering-policies  b002de3
#   af-resource  asyncflow feature/resource-policy    6285975
#   rh-fix       RHAPSODY fix/dragon-affinity-sizing  d2cbc58 (caps Batch's pool to the job's CPUs)
#   rh-merged    RHAPSODY d2cbc58 + feature/dragon-placement 762c5b3 (local merge bd777d1)
#   af-prio      asyncflow experiment/policy-priority      641ee65 (second campaign)
#   rh-prio      RHAPSODY experiment/dragon-priority       738288e (second campaign)
# Dragon is the stock dragonhpc 0.14.1 wheel, unless DRAGON_TREE names a source tree built by
# build_dragon.sh (second campaign: experiment/batch-reports-priority dbf5405).
# Resources come from the sbatch command line. QOS normal allows 128 CPUs and 2 GPUs per user
# in total, so every run holds part of a node. Batch starts allowed_cpus // 2 workers.
# Besides the result JSON the script writes, it records the node state: the jobs on the node
# at start and end, and every 5 s the load, the CPUs the node and this job use, and with GPUs
# nvidia-smi once a second. merge_meta.py adds all of it to the run's result file.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=/home/mturilli/tmp/sched-exp/logs/%x-%j.out

SCRIPT=${1:?usage: run.sbatch <script.py> [args]}
shift
JOB_START=$(date +%s)
BASE=/home/mturilli/tmp/sched-exp
RUN=$BASE/runs/$SLURM_JOB_ID
mkdir -p "$RUN"
export RUN_DIR=$RUN
export RESULTS_DIR=${RESULTS_DIR:-$BASE/results}

if [ -n "$DRAGON_TREE" ]; then
  # A Dragon source tree built by build_dragon.sh: its venv and paths. libdragon.so is built
  # with EESSI GCC 13; the compat-layer Python ships GCC 10's libstdc++.
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
NODE=$(hostname)
echo "== job $SLURM_JOB_ID $SCRIPT $* on $NODE: partition=$SLURM_JOB_PARTITION cpus=$SLURM_CPUS_ON_NODE gpus=${SLURM_JOB_GPUS:-none}"
echo "== $DRAGON_VERSION; asyncflow $AF $ASYNCFLOW_COMMIT; rhapsody $RH $RHAPSODY_COMMIT"
echo "== batch step: $(grep Cpus_allowed_list /proc/self/status)"

squeue -a -w "$NODE" -h -o "%i|%u|%P|%T|%C|%b|%M|%j" > "$RUN/squeue_start.txt"
date +%z > "$RUN/tz.txt"

# Node CPU use from /proc/stat (gawk: "load" is a builtin name), this job's from its cgroup: the rest is other jobs.
CG=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup | sed 's|/step_.*||')
sampler() {
  local prev_t prev_busy prev_tot prev_mine
  while true; do
    read -r _ u n s i w q sq st _ < /proc/stat
    tot=$((u + n + s + i + w + q + sq + st)); busy=$((tot - i - w))
    mine=$(awk '/^usage_usec/{print $2}' "$CG/cpu.stat" 2>/dev/null)
    t=$(date +%s.%N)
    if [ -n "$prev_t" ]; then
      awk -v t="$t" -v pt="$prev_t" -v b="$busy" -v pb="$prev_busy" -v to="$tot" -v pto="$prev_tot" \
          -v m="$mine" -v pm="$prev_mine" -v ncpu="$(nproc --all)" -v la="$(cut -d' ' -f1 /proc/loadavg)" \
        'BEGIN { node = (b - pb) / (to - pto) * ncpu; me = (m - pm) / ((t - pt) * 1e6);
                 printf "%.1f,%s,%.2f,%.2f\n", t, la, node, me }'
    fi
    prev_t=$t; prev_busy=$busy; prev_tot=$tot; prev_mine=$mine
    sleep 5
  done
}
sampler > "$RUN/load.csv" 2>/dev/null &
SAMPLER=$!
GPUSAMPLER=
if [ -n "$SLURM_JOB_GPUS" ] && command -v nvidia-smi > /dev/null; then
  nvidia-smi --query-gpu=index,pci.bus_id,name,memory.total --format=csv,noheader > "$RUN/gpus.txt"
  nvidia-smi --query-gpu=timestamp,index,utilization.gpu,memory.used --format=csv,noheader,nounits \
    -lms 1000 > "$RUN/gpu.csv" 2>/dev/null &
  GPUSAMPLER=$!
fi

# No HSTA fabric on rust (Proxmox VMs): tcp transport; the bridge interfaces are vmbr*.
NETWORK_PREFIX='^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'
LEFT=$(squeue -h -j "$SLURM_JOB_ID" -o %L | awk '{d=0; t=$1; if (index(t, "-")) {split(t, a, "-"); d=a[1]; t=a[2]}
  n=split(t, p, ":"); s=0; for (i=1; i<=n; i++) s=s*60+p[i]; print d*86400+s}')
[ "${LEFT:-0}" -gt 300 ] 2>/dev/null || LEFT=1800
SECONDS=0
cd "$RUN" || exit 1
timeout --signal=INT --kill-after=60 $((LEFT - 120)) \
  dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" "$BASE/experiments/$SCRIPT" "$@"
RC=$?
echo "== dragon rc=$RC wall=${SECONDS}s"
kill $SAMPLER $GPUSAMPLER 2>/dev/null
squeue -a -w "$NODE" -h -o "%i|%u|%P|%T|%C|%b|%M|%j" > "$RUN/squeue_end.txt"
python "$BASE/experiments/merge_meta.py" "$RUN" "$RESULTS_DIR" "$SLURM_JOB_ID" "$RC" "$SECONDS"

# Leave no Dragon pools behind, touching only this job's: Slurm kills the job's processes,
# and the /dev/shm files of this user created since the job started are this job's, because
# each node runs one Dragon job of ours at a time (-J <node stream> -d singleton).
# The first campaign called dragon-cleanup here instead. Inside a Slurm job that kills every
# Dragon process of this user on the node, in any job, and its /dev/shm cleanup goes over SSH,
# which fails on rust; see results/diag/dragon_cleanup_in_slurm_*.log.
srun --nodes=1 --ntasks=1 --cpu_bind=none bash -c \
  "find /dev/shm -maxdepth 1 -user \$USER -newermt @$JOB_START -delete; echo \"== cleanup \$(hostname): procs=\$(pgrep -u \$USER -fc dragon-) shm=\$(find /dev/shm -maxdepth 1 -user \$USER | wc -l)\""

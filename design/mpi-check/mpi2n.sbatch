#!/bin/bash
# MPI through Dragon on two rust nodes; the one-node check on three is design/mpi-check-three.md.
#   sbatch -J d2 -d singleton -p cpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 16 --mem=32G \
#     -t 20 mpi2n.sbatch
# Optional: BASELINES=0 skips the baselines; CASES="<name> <how> <pmi> <ranks> <layout> <reps> [fail];..."
# runs only those Dragon cases; DRAGON_LOG=DEBUG turns on Dragon's logs (in each case directory);
# DRAGON_MPI sets Slurm's MPI plugin for Dragon's own steps (rust's default is pmix, which makes
# Dragon's processes PMIx clients of Slurm's server; job 26074).
# Compiles mpi_hello.c here, runs the baselines (srun --mpi=pmix, Open MPI's mpirun), then one
# Dragon runtime per case (mpi_case.py), as run2n.sbatch starts Dragon. After each runtime the
# job's leftover steps are cancelled and, on each node, this user's processes whose working
# directory is the case's are ended, and the files this user made in /dev/shm since the case
# started are removed if no other job of this user runs on that node. No dragon-cleanup.
#SBATCH --output=/home/mturilli/tmp/mpi-check/logs/%x-%j.out
set -u
JOB_START=$(date +%s)
B=/home/mturilli/tmp/mpi-check
RUN=$B/runs/$SLURM_JOB_ID
mkdir -p "$RUN" && cd "$RUN" || exit 1
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
NODELIST=$(IFS=,; echo "${NODES[*]}")
on_node() {  # on_node <host> <command...>: one task on that node, inside this job
  local h=$1; shift
  srun --overlap --nodes=1 --nodelist="$h" --ntasks=1 -c 1 --cpu_bind=none "$@"
}
elapsed() { python3 -c "import sys, time; print(round(time.time() - float(sys.argv[1]), 2))" "$1"; }
# Open MPI from EESSI, the same build on every node (julia has no system Open MPI: job 26070),
# generic x86_64 so that no node gets code for another's CPU. Its libraries carry an RPATH;
# Dragon's dlopen("libpmix.so") needs the PMIx directory on LD_LIBRARY_PATH. The EESSI version
# must be the venv Python's (2023.06): that Python runs under 2023.06's own loader and C
# library, which neither searches the host's /usr/lib nor loads 2025.06's libpmix (it needs
# GLIBC_2.38; jobs 26075-26077 and the dlopen probes).
export EESSI_SOFTWARE_SUBDIR_OVERRIDE=x86_64/generic
set +u  # EESSI's init and Lmod read unset variables (job 26071 ended there, silently)
source "/cvmfs/software.eessi.io/versions/${EESSI_VER:-2023.06}/init/bash" >/dev/null 2>&1
module load "${OMPI_MODULE:-OpenMPI/4.1.6-GCC-13.2.0}" >/dev/null 2>&1 || { echo "module load failed"; exit 1; }
source /home/mturilli/tmp/dragon-native-pool/venv/bin/activate
set -u
[ -n "${EBROOTPMIX:-}" ] || { echo "no EBROOTPMIX after module load"; exit 1; }
export LD_LIBRARY_PATH=$EBROOTPMIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
echo "== job $SLURM_JOB_ID on $NODELIST: $(python -V 2>&1), dragonhpc $(pip show dragonhpc 2>/dev/null | awk '/^Version/{print $2}'), $(ompi_info --version 2>/dev/null | head -1), PMIx $(basename "$EBROOTPMIX")"
squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%M|%j|%N" >squeue_start.txt
for h in "${NODES[@]}"; do
  others=$(squeue -h -u "$USER" -w "$h" -o %i | grep -vx "$SLURM_JOB_ID" | xargs)
  on_node "$h" bash -c '
    others=$1
    echo "-- $(hostname): $(ls "$EBROOTPMIX/lib/libpmix.so" 2>/dev/null || echo "no libpmix.so"), $(mpirun --version 2>/dev/null | head -1)"
    # Job 26070 (cancelled, 06:03:00-06:03:46 on haskell and julia): its own files only.
    f=$(find /dev/shm -maxdepth 1 -user "$USER" -newermt "2026-11-01 06:02:59" ! -newermt "2026-11-01 06:04:30")
    if [ -n "$f" ] && [ -z "$others" ]; then rm -f -- $f; echo "   removed job 26070 file(s): $(basename -a $f | xargs)"
    elif [ -n "$f" ]; then echo "   kept job 26070 file(s), job(s) $others of $USER here: $(basename -a $f | xargs)"; fi
  ' _ "$others"
done
mpicc -O2 -Wl,-rpath,"$EBROOTOPENMPI/lib" -o mpi_hello "$B/mpi_hello.c" || { echo "mpicc failed"; exit 1; }
ldd mpi_hello | grep -E "libmpi|libpmix" | sed 's/^/   /'

summary() {  # summary <case dir>: per repetition, the sizes and hosts the ranks saw
  local r
  for r in "$1"/out/rep-*; do
    [ -d "$r" ] || continue
    echo "   $(basename "$r"): $(ls "$r" | wc -l) rank files; $(cat "$r"/rank-* 2>/dev/null |
      awk '{print "of", $4, "on", $6}' | sort | uniq -c | xargs)"
  done
}

after_case() {  # after_case <case dir> <start epoch>
  local d=$1 t0=$2 steps h others
  steps=$(squeue -h -s -j "$SLURM_JOB_ID" -o %i | grep -E "^$SLURM_JOB_ID\.[0-9]+\$" | xargs)
  if [ -n "$steps" ]; then
    echo "   cancelling leftover step(s) $steps"
    scancel $steps
    for _ in $(seq 20); do
      [ -z "$(squeue -h -s -j "$SLURM_JOB_ID" -o %i | grep -E "^$SLURM_JOB_ID\.[0-9]+\$")" ] && break
      sleep 3
    done
  fi
  for h in "${NODES[@]}"; do
    others=$(squeue -h -u "$USER" -w "$h" -o %i | grep -vx "$SLURM_JOB_ID" | xargs)
    on_node "$h" bash -c '
      d=$1 t0=$2 others=$3
      mine() { for p in $(pgrep -u "$USER"); do
        case $(readlink "/proc/$p/cwd" 2>/dev/null) in "$d" | "$d"/*) echo "$p" ;; esac; done; }
      left=$(mine | xargs)
      if [ -n "$left" ]; then kill -TERM $left 2>/dev/null; sleep 3; kill -KILL $(mine) 2>/dev/null; fi
      shm=$(find /dev/shm -maxdepth 1 -user "$USER" -newermt "@$t0" | wc -l)
      if [ -z "$others" ]; then find /dev/shm -maxdepth 1 -user "$USER" -newermt "@$t0" -delete; gone=removed
      else gone="kept (job(s) $others of $USER here)"; fi
      echo "   $(hostname): $(wc -w <<<"$left") leftover process(es) ended, $(mine | wc -l) left;" \
        "$shm new /dev/shm file(s) $gone; Dragon processes now $(pgrep -u "$USER" -fc "[d]ragon")"
    ' _ "$d" "$t0" "$others"
  done
}

if [ "${BASELINES:-1}" = 1 ]; then
echo "== baselines"
for rep in 1 2 3; do
  d=$RUN/srun-pmix-8 && mkdir -p "$d/out/rep-$rep" && t0=$(date +%s.%N)
  timeout -k 10 60 srun --overlap --mpi=pmix -N 2 --ntasks-per-node=4 -c 1 --cpu_bind=none \
    ./mpi_hello "$d/out/rep-$rep" 8
  echo "== srun --mpi=pmix, 8 ranks, rep $rep: rc=$? in $(elapsed "$t0") s"
done
summary "$RUN/srun-pmix-8"
fi

SHIM=$RUN/shim
mkdir -p "$SHIM" && cp /home/mturilli/tmp/sched-exp/experiments/srun-hetjob "$SHIM/srun" && chmod +x "$SHIM/srun"
NETWORK_PREFIX='^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'
dcase() {  # dcase <name> <how> <pmi> <ranks> <layout> <reps> [fail]
  local name=$1 d=$RUN/$1 t0 rc
  shift
  mkdir -p "$d/out" && t0=$(date +%s)
  (cd "$d" && PATH=$SHIM:$PATH env ${DRAGON_MPI:+SLURM_MPI_TYPE=$DRAGON_MPI} timeout --signal=INT --kill-after=30 240 \
    dragon -w slurm -t tcp ${DRAGON_LOG:+-l "$DRAGON_LOG"} --network-prefix "$NETWORK_PREFIX" "$B/mpi_case.py" "$RUN/mpi_hello" "$d/out" "$@" \
    >run.out 2>run.err)
  rc=$?
  echo "== $name: dragon rc=$rc in $(($(date +%s) - t0)) s"
  grep -a -E "^(RESULT|hosts)" "$d/run.out" ||
    true
  grep -a -q "^RESULT" "$d/run.out" || {
    echo "   no RESULT:"
    grep -a -h -E "Abnormal|Error|exception" "$d/run.out" "$d/run.err" | grep -v -E "UCX|pml_ucx|Network Config" | sort -u | head -4
    grep -a -h -i -E "pmix|exception|error" "$d"/*.log 2>/dev/null | grep -v -E "UCX|pml_ucx" | cut -c1-300 | sort -u | head -12
  }
  summary "$d"
  after_case "$d" "$t0"
}

if [ -n "${CASES:-}" ]; then
  IFS=';' read -ra list <<<"$CASES"
  for c in "${list[@]}"; do dcase $c; done
else
dcase pg-pmix-split pg pmix 8 split 3
dcase batch-pmix-split batch pmix 8 split 3
dcase pg-pmix-default pg pmix 8 default 1
dcase batch-pmix-default batch pmix 8 default 1
dcase batch-crayDefault-split batch default 8 split 1
dcase batch-pmix-conc8 batch pmix 4 conc8 1
dcase pg-pmix-split-fail pg pmix 8 split 1 fail
dcase batch-pmix-split-fail batch pmix 8 split 1 fail
dcase mpirun-in-batch mpirun none 8 split 1
fi

squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%M|%j|%N" >squeue_end.txt
# The baselines' Open MPI segments and anything else this job left in /dev/shm; files only.
for h in "${NODES[@]}"; do
  others=$(squeue -h -u "$USER" -w "$h" -o %i | grep -vx "$SLURM_JOB_ID" | xargs)
  on_node "$h" bash -c '
    others=$1 t0=$2
    n=$(find /dev/shm -maxdepth 1 -user "$USER" -newermt "@$t0" | wc -l)
    if [ -z "$others" ]; then find /dev/shm -maxdepth 1 -user "$USER" -newermt "@$t0" -delete; g=removed
    else g="kept (job(s) $others of $USER here)"; fi
    echo "== end $(hostname): $n /dev/shm file(s) since the job started, $g; Dragon processes $(pgrep -u "$USER" -fc "[d]ragon")"
  ' _ "$others" "$JOB_START"
done
echo "== done"

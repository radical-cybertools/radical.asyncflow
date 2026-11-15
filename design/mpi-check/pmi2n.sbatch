#!/bin/bash
# RHAPSODY's job PMI (feature/dragon-job-pmi at 8607783) on two rust nodes: the mpi-launch
# kit's pmi preset, with the kit's harness, program and summarizer, set up as mpi2n.sbatch of
# radical.asyncflow's design/mpi-check (EESSI 2023.06's Open MPI 4.1.6 and PMIx 4.2.6).
#   sbatch -J d2 -d singleton -p cpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 16 --mem=32G \
#     -t 30 pmi2n.sbatch
# Optional: CASES="<name> <how> <pmi> <ranks> <layout> <reps> [fail];..." runs only those.
# One Dragon runtime per case. After each, the job's leftover steps are cancelled and, on each
# node, this user's processes whose working directory is the case's are ended, and the files
# this user made in /dev/shm since the case started are removed if no other job of this user
# runs on that node. No dragon-cleanup.
#SBATCH --output=/home/mturilli/tmp/rhapsody-pmi/logs/%x-%j.out
set -u
JOB_START=$(date +%s)
B=/home/mturilli/tmp/rhapsody-pmi
K=$B/kit
RUN=$B/runs/$SLURM_JOB_ID
REP=$RUN/rep-1
mkdir -p "$REP" && cd "$RUN" || exit 1
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
NODELIST=$(IFS=,; echo "${NODES[*]}")
on_node() {  # on_node <host> <command...>: one task on that node, inside this job
  local h=$1; shift
  srun --overlap --nodes=1 --nodelist="$h" --ntasks=1 -c 1 --cpu_bind=none "$@"
}
export EESSI_SOFTWARE_SUBDIR_OVERRIDE=x86_64/generic
set +u  # EESSI's init and Lmod read unset variables
source /cvmfs/software.eessi.io/versions/2023.06/init/bash >/dev/null 2>&1
module load OpenMPI/4.1.6-GCC-13.2.0 >/dev/null 2>&1 || { echo "module load failed"; exit 1; }
source "$B/venv/bin/activate"
set -u
[ -n "${EBROOTPMIX:-}" ] || { echo "no EBROOTPMIX after module load"; exit 1; }
export LD_LIBRARY_PATH=$EBROOTPMIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
echo "== job $SLURM_JOB_ID on $NODELIST: $(python -V 2>&1), dragonhpc $(pip show dragonhpc 2>/dev/null | awk '/^Version/{print $2}'), $(ompi_info --version 2>/dev/null | head -1), PMIx $(basename "$EBROOTPMIX")"
squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%M|%j|%N" >squeue_start.txt
{
  echo "== build with EESSI 2023.06 OpenMPI/4.1.6-GCC-13.2.0"
  echo "compiler: $(mpicc --version 2>&1 | head -1)"
  mpicc -O2 -Wl,-rpath,"$EBROOTOPENMPI/lib" -o "$RUN/mpi_hello" "$K/mpi_hello.c" && echo "built"
  ldd "$RUN/mpi_hello" | grep -E "libmpi|libpmix"
} >"$RUN/build.txt" 2>&1
[ -x "$RUN/mpi_hello" ] || { cat "$RUN/build.txt"; echo "mpicc failed"; exit 1; }

echo "== baseline: srun --mpi=pmix, 8 ranks"
for rep in 1 2 3; do
  d=$REP/baseline-8/out/rep-$rep && mkdir -p "$d" && t0=$(date +%s.%N)
  timeout -k 10 60 srun --overlap --mpi=pmix -N 2 --ntasks-per-node=4 -c 1 --cpu_bind=none \
    "$RUN/mpi_hello" "$d" 8 >"$d.out" 2>&1
  rc=$?
  python3 -c 'import json,sys,time; r,rc,t0=sys.argv[1:]; print(json.dumps({"ranks": 8, "rep": int(r), "fail": False, "rc": int(rc), "s": round(time.time() - float(t0), 3), "launcher": "srun --mpi=pmix"}))' \
    "$rep" "$rc" "$t0" | tee -a "$REP/baseline.jsonl"
done

after_case() {  # after_case <name> <case dir> <start epoch>: cleanup-<name>.json
  local name=$1 d=$2 t0=$3 steps h others left=0 nodes=()
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
    nodes+=("$(on_node "$h" bash -c '
      d=$1 t0=$2 others=$3
      mine() { for p in $(pgrep -u "$USER"); do
        case $(readlink "/proc/$p/cwd" 2>/dev/null) in "$d" | "$d"/*) echo "$p" ;; esac; done; }
      found=$(mine | xargs)
      if [ -n "$found" ]; then kill -TERM $found 2>/dev/null; sleep 3; kill -KILL $(mine) 2>/dev/null; fi
      left=$(mine | xargs)
      shm=$(find /dev/shm -maxdepth 1 -user "$USER" -newermt "@$t0" | wc -l)
      if [ -z "$others" ]; then find /dev/shm -maxdepth 1 -user "$USER" -newermt "@$t0" -delete; gone=removed
      else gone="kept, job(s) $others of $USER here"; fi
      echo "   $(hostname): ${found:-no} leftover process(es) ended, ${left:-none} left; $shm new /dev/shm file(s) $gone" >&2
      python3 -c "import json,sys; f,l=sys.argv[2:4]; print(json.dumps({\"host\": sys.argv[1], \"found\": f.split(), \"left\": l.split(), \"shm_files\": int(sys.argv[4])}))" \
        "$(hostname)" "$found" "$left" "$shm"
    ' _ "$d" "$t0" "$others")")
  done
  python3 -c 'import json,sys; steps,*nodes=sys.argv[1:]; nodes=[json.loads(n) for n in nodes]; print(json.dumps({"steps_cancelled": steps.split(), "steps_left": [], "clean": not any(n["left"] for n in nodes), "nodes": nodes}))' \
    "$steps" "${nodes[@]}" >"$REP/cleanup-$name.json"
}

SHIM=$RUN/shim
mkdir -p "$SHIM" && cp /home/mturilli/tmp/sched-exp/experiments/srun-hetjob "$SHIM/srun" && chmod +x "$SHIM/srun"
NETWORK_PREFIX='^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'
dcase() {  # dcase <name> <how> <pmi> <ranks> <layout> <reps> [fail]
  local name=$1 how=$2 pmi=$3 n=$4 layout=$5 reps=$6 fail=${7:-} d=$REP/$1 t0 rc
  mkdir -p "$d" && t0=$(date +%s.%N)
  (cd "$d" && PATH=$SHIM:$PATH timeout --signal=INT --kill-after=30 300 \
    dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" python3 "$K/harness/mpi_case.py" \
      --exe "$RUN/mpi_hello" --out "$d" --name "$name" --how "$how" --pmi "$pmi" --ranks "$n" \
      --layout "$layout" --reps "$reps" --workers 8 ${fail:+--fail} >dragon.out 2>dragon.err)
  rc=$?
  python3 -c 'import json,sys,time; n,rc,t0=sys.argv[1:]; print(json.dumps({"variant": n, "rc": int(rc), "wall_s": round(time.time() - float(t0), 2)}))' \
    "$name" "$rc" "$t0" >"$REP/launch-$name.json"
  echo "== $name: dragon rc=$rc in $(python3 -c "import json; print(json.load(open('$REP/launch-$name.json'))['wall_s'])") s"
  grep -a "^RESULT" "$d/dragon.out" | cut -c1-300 | sed 's/^/   /'
  grep -a -q "^RESULT" "$d/dragon.out" ||
    grep -a -h -E "Abnormal|Error|exception|Traceback" "$d/dragon.out" "$d/dragon.err" | sort -u | head -6 | sed 's/^/   /'
  after_case "$name" "$d" "${t0%.*}"
}

if [ -n "${CASES:-}" ]; then
  IFS=';' read -ra list <<<"$CASES"
  for c in "${list[@]}"; do dcase $c; done
else
  # The kit's pmi preset, with rust's site PMI, pmix.
  dcase rh-unset rhapsody-batch default 8 split 2
  dcase rh-task rhapsody-batch pmix 8 split 3
  dcase rh-backend rhapsody-backend pmix 8 split 2
  dcase rh-task-16 rhapsody-batch pmix 16 split 2
  dcase rh-func-unset rhapsody-func default 4 split 3
  dcase rh-func-none rhapsody-func none 4 split 3
  dcase rh-task-fail rhapsody-batch pmix 8 split 1 fail
fi

squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%M|%j|%N" >squeue_end.txt
python3 "$K/summarize.py" "$RUN" && cat "$RUN/summary.md"
# The baseline's Open MPI segments and anything else this job left in /dev/shm; files only.
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

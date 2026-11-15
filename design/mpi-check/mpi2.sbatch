#!/bin/bash
# MPI on RHAPSODY's native backend, MPI from Python (mpi4py) and MPI with GPUs, on two rust
# nodes (2026-11-02): the mpi-launch kit's native and mpi4py presets with rust's site PMI, pmix,
# or its GPU cases, with the kit's harness, programs and summarizer, set up as pmi2n.sbatch
# (EESSI 2023.06's Open MPI 4.1.6 and PMIx 4.2.6).
#   CPU: sbatch -J d2 -d singleton -p cpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 16 \
#          --mem=32G -t 30 mpi2.sbatch
#   GPU: sbatch -J d2 -d singleton -p gpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 16 \
#          --mem=32G --gres=gpu:1 -t 30 --export=ALL,MODE=gpu mpi2.sbatch
# MODE=gpu runs the GPU cases: one rank per node, each on its node's one granted GPU, the
# reduction in host or in device memory (EESSI's Open MPI 4.1.6 is built with CUDA support),
# and the baseline with mpi_gpu in both modes.
# Optional: CASES="<name> <how> <pmi> <ranks> <layout> <reps> [flags];..." runs only those.
# One Dragon runtime per case. After each, the job's leftover steps are cancelled and, on each
# node, this user's processes whose working directory is the case's are ended, and the files
# this user made in /dev/shm since the case started are removed if no other job of this user
# runs on that node. No dragon-cleanup.
#SBATCH --output=/home/mturilli/tmp/rhapsody-mpi2/logs/%x-%j.out
set -u
JOB_START=$(date +%s)
B=/home/mturilli/tmp/rhapsody-mpi2
K=$B/kit/kits/mpi-launch
MODE=${MODE:-cpu}
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
# Dragon dlopens libpmix.so, and mpi4py's wheel libmpi.so.40; EESSI sets no LD_LIBRARY_PATH.
export LD_LIBRARY_PATH=$EBROOTOPENMPI/lib:$EBROOTPMIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
echo "== job $SLURM_JOB_ID ($MODE) on $NODELIST: $(python -V 2>&1), dragonhpc $(pip show dragonhpc 2>/dev/null | awk '/^Version/{print $2}'), mpi4py $(pip show mpi4py 2>/dev/null | awk '/^Version/{print $2}'), $(ompi_info --version 2>/dev/null | head -1), PMIx $(basename "$EBROOTPMIX")"
squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" >squeue_start.txt
if [[ $MODE == gpu ]]; then
  for h in "${NODES[@]}"; do
    on_node "$h" bash -c 'echo "$(hostname): CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}; $(nvidia-smi -L 2>&1 | tr "\n" " ")"'
  done | tee gpus.txt
fi
{
  echo "== build with EESSI 2023.06 OpenMPI/4.1.6-GCC-13.2.0"
  echo "compiler: $(mpicc --version 2>&1 | head -1)"
  mpicc -O2 -Wl,-rpath,"$EBROOTOPENMPI/lib" -o "$RUN/mpi_hello" "$K/mpi_hello.c" && echo "built mpi_hello"
  mpicc -O2 -Wl,-rpath,"$EBROOTOPENMPI/lib" -o "$RUN/mpi_gpu" "$K/mpi_gpu.c" -ldl && echo "built mpi_gpu"
  ldd "$RUN/mpi_hello" | grep -E "libmpi|libpmix"
  echo "mpi4py $(python -c 'import mpi4py; print(mpi4py.__version__, mpi4py.__file__)')"
} >"$RUN/build.txt" 2>&1
[ -x "$RUN/mpi_hello" ] && [ -x "$RUN/mpi_gpu" ] || { cat "$RUN/build.txt"; echo "mpicc failed"; exit 1; }

# The baseline: 8 ranks of mpi_hello, or in MODE=gpu 1 + 1 ranks of mpi_gpu in each mode.
baseline() {  # <ranks> <per node> <dir name> <program> [mode]
  local n=$1 per=$2 name=$3 gpu=${5:-} d t0 rc
  shift 3
  echo "== baseline: srun --mpi=pmix, $n ranks of $(basename "$1")"
  for rep in 1 2 3; do
    d=$REP/$name/out/rep-$rep && mkdir -p "$d" && t0=$(date +%s.%N)
    timeout -k 10 60 srun --overlap --mpi=pmix -N 2 --ntasks-per-node="$per" -c 1 --cpu_bind=none \
      "$1" "$d" "$n" "${@:2}" >"$d.out" 2>&1
    rc=$?
    python3 -c 'import json,sys,time; n,r,rc,t0,g,d=sys.argv[1:]; print(json.dumps({"ranks": int(n), "rep": int(r), "fail": False, "gpu": g or None, "rc": int(rc), "s": round(time.time() - float(t0), 3), "launcher": "srun --mpi=pmix", "dir": d}))' \
      "$n" "$rep" "$rc" "$t0" "$gpu" "$name/out/rep-$rep" | tee -a "$REP/baseline.jsonl"
  done
}
if [[ $MODE == gpu ]]; then
  baseline 2 1 baseline-2-gpu-host "$RUN/mpi_gpu" host
  baseline 2 1 baseline-2-gpu-device "$RUN/mpi_gpu" device
else
  baseline 8 4 baseline-8 "$RUN/mpi_hello"
fi

after_case() {  # after_case <name> <case dir> <start epoch>: cleanup-<name>.json
  local name=$1 d=$2 t0=$3 steps h others nodes=()
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
dcase() {  # dcase <name> <how> <pmi> <ranks> <layout> <reps> [flags: fail, gpu-<mode>[-all]]
  local name=$1 how=$2 pmi=$3 n=$4 layout=$5 reps=$6 d=$REP/$1 t0 rc exe=$RUN/mpi_hello f
  local -a extra=()
  for f in "${@:7}"; do
    case $f in
      fail) extra+=(--fail) ;;
      gpu-host | gpu-device) extra+=(--gpu "${f#gpu-}") exe=$RUN/mpi_gpu ;;
      gpu-host-all | gpu-device-all) f=${f#gpu-} && extra+=(--gpu "${f%-all}" --gpu-all) exe=$RUN/mpi_gpu ;;
      *) echo "== $name: unknown flag $f"; return ;;
    esac
  done
  mkdir -p "$d" && t0=$(date +%s.%N)
  (cd "$d" && PATH=$SHIM:$PATH timeout --signal=INT --kill-after=30 300 \
    dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" python3 "$K/harness/mpi_case.py" \
      --exe "$exe" --out "$d" --name "$name" --how "$how" --pmi "$pmi" --ranks "$n" \
      --layout "$layout" --reps "$reps" --workers 8 "${extra[@]}" >dragon.out 2>dragon.err)
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
elif [[ $MODE == gpu ]]; then
  # One rank per node, each on the node's one GPU in this job.
  dcase gpu-pg pg pmix 2 split 3 gpu-host
  dcase gpu-pg-dev pg pmix 2 split 3 gpu-device
  dcase gpu-pg-dev-all pg pmix 2 split 2 gpu-device-all
  dcase gpu-batch-dev batch pmix 2 split 2 gpu-device
  dcase gpu-rh rhapsody-batch pmix 2 split 2 gpu-host
  dcase gpu-rh-dev rhapsody-batch pmix 2 split 3 gpu-device
  dcase gpu-nat rhapsody-native pmix 2 split 2 gpu-host
  dcase gpu-nat-dev rhapsody-native pmix 2 split 3 gpu-device
else
  # The kit's native and mpi4py presets, with rust's site PMI, pmix.
  dcase nat-unset rhapsody-native default 8 split 1
  dcase nat-task rhapsody-native pmix 8 split 3
  dcase nat-backend rhapsody-native-backend pmix 8 split 2
  dcase nat-task-16 rhapsody-native pmix 16 split 2
  dcase nat-fail rhapsody-native pmix 8 split 1 fail
  dcase pg-mpi4py pg-mpi4py pmix 8 split 3
  dcase py-batch rhapsody-mpi4py pmix 8 split 3
  dcase py-native rhapsody-native-mpi4py pmix 8 split 3
  dcase py-batch-16 rhapsody-mpi4py pmix 16 split 2
  dcase py-native-16 rhapsody-native-mpi4py pmix 16 split 2
  dcase py-batch-unset rhapsody-mpi4py default 8 split 1
  dcase py-batch-fail rhapsody-mpi4py pmix 8 split 1 fail
  dcase py-native-fail rhapsody-native-mpi4py pmix 8 split 1 fail
fi

squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" >squeue_end.txt
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

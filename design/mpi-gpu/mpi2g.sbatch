#!/bin/bash
# MPI with GPUs on rust through Dragon (2026-11-02): mpi2.sbatch's GPU cases, rerun with the
# NVIDIA libraries EESSI's loader needs (rust-gpu-setup.sh: nvlib/ first on LD_LIBRARY_PATH).
# The mpi-launch kit's harness, mpi_gpu.c and summarizer, and the venv, are mpi2.sbatch's
# (~/tmp/rhapsody-mpi2), used as they are; runs and logs go to ~/tmp/rhapsody-mpi2-gpu.
#   Two nodes, one GPU each (1 + 1 ranks):
#     sbatch -J d2 -d singleton -p gpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 16 \
#       --mem=32G --gres=gpu:1 -t 20 mpi2g.sbatch
#   haskell's two GPUs (2 ranks on one node):
#     sbatch -J d2 -d singleton -p gpu -w haskell -N 1 --ntasks-per-node=1 -c 16 \
#       --mem=32G --gres=gpu:2 -t 20 --export=ALL,RPN=2 mpi2g.sbatch
# RPN: ranks per node, one per granted GPU (default 1). Each rank reduces rank+1 from GPU
# memory through host memory (host) or hands the device pointer to MPI_Allreduce (device; EESSI's
# Open MPI 4.1.6 is built with CUDA support). Baseline: srun --mpi=pmix in both modes.
# Optional: CASES="<name> <how> <pmi> <ranks> <layout> <reps> [flags];..." runs only those.
# One Dragon runtime per case. After each, the job's leftover steps are cancelled and, on each
# node, this user's processes whose working directory is the case's are ended, and the files
# this user made in /dev/shm since the case started are removed if no other job of this user
# runs on that node. No dragon-cleanup.
#SBATCH --output=/home/mturilli/tmp/rhapsody-mpi2-gpu/logs/%x-%j.out
set -u
JOB_START=$(date +%s)
B=/home/mturilli/tmp/rhapsody-mpi2-gpu
K=/home/mturilli/tmp/rhapsody-mpi2/kit/kits/mpi-launch
VENV=/home/mturilli/tmp/rhapsody-mpi2/venv
RPN=${RPN:-1}
RUN=$B/runs/$SLURM_JOB_ID
REP=$RUN/rep-1
mkdir -p "$REP" && cd "$RUN" || exit 1
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
NODELIST=$(IFS=,; echo "${NODES[*]}")
NN=${#NODES[@]}
N=$((NN * RPN))
on_node() {  # on_node <host> <command...>: one task on that node, inside this job
  local h=$1; shift
  srun --overlap --nodes=1 --nodelist="$h" --ntasks=1 -c 1 --cpu_bind=none "$@"
}
export EESSI_SOFTWARE_SUBDIR_OVERRIDE=x86_64/generic
set +u  # EESSI's init and Lmod read unset variables
source /cvmfs/software.eessi.io/versions/2023.06/init/bash >/dev/null 2>&1
module load OpenMPI/4.1.6-GCC-13.2.0 >/dev/null 2>&1 || { echo "module load failed"; exit 1; }
source "$VENV/bin/activate"
set -u
[ -n "${EBROOTPMIX:-}" ] || { echo "no EBROOTPMIX after module load"; exit 1; }
# nvlib: the host driver's libcuda.so.1 and libnvidia-ml.so.1 and PyPI's libcudart.so, which
# EESSI's loader doesn't find on its own; then Open MPI (mpi4py's wheel) and PMIx (Dragon).
export LD_LIBRARY_PATH=$B/nvlib:$EBROOTOPENMPI/lib:$EBROOTPMIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
echo "== job $SLURM_JOB_ID on $NODELIST, $RPN rank(s) per node: $(python -V 2>&1), dragonhpc $(pip show dragonhpc 2>/dev/null | awk '/^Version/{print $2}'), $(ompi_info --version 2>/dev/null | head -1), PMIx $(basename "$EBROOTPMIX")"
squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" >squeue_start.txt
for h in "${NODES[@]}"; do
  on_node "$h" bash -c '
    echo "$(hostname): CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}; $(nvidia-smi -L 2>&1 | tr "\n" " ")"
    python - <<EOF
import ctypes
for n in ("libcuda.so.1", "libcudart.so"):
    try:
        ctypes.CDLL(n); print("   ", n, "loads")
    except OSError as e:
        print("   ", n, "FAILS:", e)
from dragon.infrastructure.gpu_desc import find_accelerators
print("    Dragon sees:", find_accelerators())
EOF'
done | tee gpus.txt
{
  echo "== build with EESSI 2023.06 OpenMPI/4.1.6-GCC-13.2.0"
  echo "compiler: $(mpicc --version 2>&1 | head -1)"
  mpicc -O2 -Wl,-rpath,"$EBROOTOPENMPI/lib" -o "$RUN/mpi_gpu" "$K/mpi_gpu.c" -ldl && echo "built mpi_gpu"
  ldd "$RUN/mpi_gpu" | grep -E "libmpi|libpmix"
  ompi_info --parsable --all 2>/dev/null | grep -m1 mpi_built_with_cuda_support:value
  ompi_info 2>/dev/null | grep -E "MCA (pml|coll): (ucx|cuda|ob1)|MCA btl: (smcuda|tcp)" | sed 's/^ */  /'
} >"$RUN/build.txt" 2>&1
[ -x "$RUN/mpi_gpu" ] || { cat "$RUN/build.txt"; echo "mpicc failed"; exit 1; }

baseline() {  # baseline <mode>: N ranks of mpi_gpu, RPN per node, 3 runs
  local mode=$1 name=baseline-$N-gpu-$1 d t0 rc
  echo "== baseline: srun --mpi=pmix, $N ranks of mpi_gpu $mode"
  for rep in 1 2 3; do
    d=$REP/$name/out/rep-$rep && mkdir -p "$d" && t0=$(date +%s.%N)
    timeout -k 10 60 srun --overlap --mpi=pmix -N "$NN" --ntasks-per-node="$RPN" -c 1 --cpu_bind=none \
      "$RUN/mpi_gpu" "$d" "$N" "$mode" >"$d.out" 2>&1
    rc=$?
    python3 -c 'import json,sys,time; n,r,rc,t0,g,d=sys.argv[1:]; print(json.dumps({"ranks": int(n), "rep": int(r), "fail": False, "gpu": g, "rc": int(rc), "s": round(time.time() - float(t0), 3), "launcher": "srun --mpi=pmix", "dir": d}))' \
      "$N" "$rep" "$rc" "$t0" "$mode" "$name/out/rep-$rep" | tee -a "$REP/baseline.jsonl"
  done
}
baseline host
baseline device

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
dcase() {  # dcase <name> <how> <pmi> <ranks> <layout> <reps> <flag: gpu-<mode>[-all]>
  local name=$1 how=$2 pmi=$3 n=$4 layout=$5 reps=$6 f=$7 d=$REP/$1 t0 rc
  local -a extra=()
  case $f in
    gpu-host | gpu-device) extra=(--gpu "${f#gpu-}") ;;
    gpu-host-all | gpu-device-all) f=${f#gpu-} && extra=(--gpu "${f%-all}" --gpu-all) ;;
    *) echo "== $name: unknown flag $f"; return ;;
  esac
  mkdir -p "$d" && t0=$(date +%s.%N)
  (cd "$d" && PATH=$SHIM:$PATH timeout --signal=INT --kill-after=30 300 \
    dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" python3 "$K/harness/mpi_case.py" \
      --exe "$RUN/mpi_gpu" --out "$d" --name "$name" --how "$how" --pmi "$pmi" --ranks "$n" \
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
else
  # Every rank on its own GPU (gpu_affinity), or (-all) on its node's GPUs by local rank.
  dcase gpu-pg pg pmix "$N" split 3 gpu-host
  dcase gpu-pg-dev pg pmix "$N" split 3 gpu-device
  dcase gpu-pg-dev-all pg pmix "$N" split 2 gpu-device-all
  dcase gpu-batch batch pmix "$N" split 2 gpu-host
  dcase gpu-batch-dev batch pmix "$N" split 2 gpu-device
  dcase gpu-rh rhapsody-batch pmix "$N" split 2 gpu-host
  dcase gpu-rh-dev rhapsody-batch pmix "$N" split 3 gpu-device
  dcase gpu-nat rhapsody-native pmix "$N" split 2 gpu-host
  dcase gpu-nat-dev rhapsody-native pmix "$N" split 3 gpu-device
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

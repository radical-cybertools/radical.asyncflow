#!/bin/bash
# The mixed-kinds kit on two rust nodes (2026-11-02): harness/mixed.py once per variant (batch,
# native) and repetition, each in its own Dragon runtime over the job, with rust's MPI (EESSI
# 2023.06 Open MPI 4.1.6, PMIx 4.2.6, pmi=pmix) and the kit's summarizer. Set up as
# mpi-check/mpi2.sbatch, whose venv it uses (dragonhpc 0.15.0, the mpi-launch archive of
# RHAPSODY, AsyncFlow 437a10d, mpi4py).
#   CPU: sbatch -J d2 -d singleton -p cpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 16 \
#          --mem=32G -t 40 mixed2.sbatch
#   GPU: sbatch -J d2 -d singleton -p gpu -w haskell,julia -N 2 --ntasks-per-node=1 -c 16 \
#          --mem=32G --gres=gpu:1 -t 25 --export=ALL,MODE=gpu mixed2.sbatch
# MODE=gpu runs one repetition with the gpu kind in the mix (one GPU per node); CPU runs REPS (2).
# For GPUs, NVLIB goes first on LD_LIBRARY_PATH: the mpi-gpu session's folder of links to the host
# driver's libcuda.so.1 and libnvidia-ml.so.1 and PyPI's libcudart.so, which EESSI's loader
# doesn't find on its own (design/mpi-gpu/rust-gpu-setup.sh); without it Dragon 0.15 lists no
# GPUs (job 26148).
# MIX_ARGS adds harness options. VARIANTS fixes the order of the runtimes in every repetition (by
# default batch then native, reversed in even ones). After each runtime, the job's leftover steps are cancelled and,
# on each node, this user's processes whose working directory is the runtime's are ended, and
# the files this user made in /dev/shm since it started are removed if no other job of this
# user runs on that node. No dragon-cleanup.
#SBATCH --output=/home/mturilli/tmp/mixed-kinds/logs/%x-%j.out
set -u
JOB_START=$(date +%s)
B=/home/mturilli/tmp/mixed-kinds
K=$B/kit/kits/mixed-kinds
VENV=${VENV:-/home/mturilli/tmp/rhapsody-mpi2/venv}
MODE=${MODE:-cpu}
REPS=${REPS:-$([[ $MODE == gpu ]] && echo 1 || echo 2)}
RUN=$B/runs/$SLURM_JOB_ID
mkdir -p "$RUN" && cd "$RUN" || exit 1
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
source "$VENV/bin/activate"
set -u
[ -n "${EBROOTPMIX:-}" ] || { echo "no EBROOTPMIX after module load"; exit 1; }
# Dragon dlopens libpmix.so, and mpi4py's wheel libmpi.so.40; EESSI sets no LD_LIBRARY_PATH.
export LD_LIBRARY_PATH=$EBROOTOPENMPI/lib:$EBROOTPMIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
if [[ $MODE == gpu ]]; then
  export LD_LIBRARY_PATH=${NVLIB:-/home/mturilli/tmp/rhapsody-mpi2-gpu/nvlib}:$LD_LIBRARY_PATH
fi
export PYTHONPATH=$K/harness PYTHONUNBUFFERED=1
TREE_COMMITS=$(python3 -c 'import importlib.metadata as m, json
out = []
for name in ("rhapsody-py", "radical.asyncflow", "mpi4py"):
    d = m.distribution(name)
    src = json.loads(d.read_text("direct_url.json") or "{}")
    ref = src.get("vcs_info", {}).get("commit_id") or src.get("url", "")
    out.append(" ".join([name, d.version, ref]).strip())
print("; ".join(out))')
export TREE_COMMITS
echo "== job $SLURM_JOB_ID ($MODE, $REPS reps) on $NODELIST: $(python -V 2>&1), dragonhpc $(pip show dragonhpc 2>/dev/null | awk '/^Version/{print $2}'), $TREE_COMMITS, $(ompi_info --version 2>/dev/null | head -1), PMIx $(basename "$EBROOTPMIX")"
echo "kit: $(cat "$B/kit/KIT_SOURCE" 2>/dev/null)"
squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" >squeue_start.txt
if [[ $MODE == gpu ]]; then
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
fi
SRC=$B/kit/kits/mpi-launch/mpi_hello.c
{
  echo "== build with EESSI 2023.06 OpenMPI/4.1.6-GCC-13.2.0"
  echo "compiler: $(mpicc --version 2>&1 | head -1)"
  echo "source: $SRC, sha256 $(sha256sum "$SRC" | cut -d' ' -f1)"
  mpicc -O2 -Wl,-rpath,"$EBROOTOPENMPI/lib" -o "$RUN/mpi_hello" "$SRC" && echo "built mpi_hello"
  ldd "$RUN/mpi_hello" | grep -E "libmpi|libpmix"
  echo "mpi4py $(python -c 'import mpi4py; print(mpi4py.__version__, mpi4py.__file__)')"
} >"$RUN/build.txt" 2>&1
[ -x "$RUN/mpi_hello" ] || { cat "$RUN/build.txt"; echo "mpicc failed"; exit 1; }

after_case() {  # after_case <rep dir> <name> <case dir> <start epoch>: cleanup-<name>.json
  local rep=$1 name=$2 d=$3 t0=$4 steps h others nodes=()
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
    "$steps" "${nodes[@]}" >"$rep/cleanup-$name.json"
}

SHIM=$RUN/shim
mkdir -p "$SHIM" && cp /home/mturilli/tmp/sched-exp/experiments/srun-hetjob "$SHIM/srun" && chmod +x "$SHIM/srun"
NETWORK_PREFIX='^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'
read -ra extra <<<"${MIX_ARGS:-}"
for rep in $(seq "$REPS"); do
  variants=(batch native)
  if ((rep % 2 == 0)); then variants=(native batch); fi
  if [[ -n ${VARIANTS:-} ]]; then read -ra variants <<<"$VARIANTS"; fi
  for v in "${variants[@]}"; do
    R=$RUN/rep-$rep d=$RUN/rep-$rep/$v
    mkdir -p "$d" && t0=$(date +%s.%N)
    (cd "$d" && PATH=$SHIM:$PATH KIT_LAUNCH=$d timeout --signal=INT --kill-after=30 "${LIMIT:-480}" \
      dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" python3 "$K/harness/mixed.py" \
        --variant "$v" --rep "$rep" --backend "$v" --out "$d" --exe "$RUN/mpi_hello" --pmi pmix \
        --workers "${MIX_WORKERS:-8}" ${extra[@]+"${extra[@]}"} >dragon.out 2>dragon.err)
    rc=$?
    python3 -c 'import json,sys,time; n,rc,t0=sys.argv[1:]; print(json.dumps({"variant": n, "rc": int(rc), "wall_s": round(time.time() - float(t0), 2)}))' \
      "$v" "$rc" "$t0" >"$R/launch-$v.json"
    echo "== rep $rep $v: dragon rc=$rc in $(python3 -c "import json; print(json.load(open('$R/launch-$v.json'))['wall_s'])") s"
    grep -a "^PHASE" "$d/dragon.out" | cut -c1-300 | sed 's/^/   /'
    grep -a -q "^PHASE" "$d/dragon.out" ||
      grep -a -h -E "Abnormal|Error|exception|Traceback" "$d/dragon.out" "$d/dragon.err" | sort -u | head -6 | sed 's/^/   /'
    after_case "$R" "$v" "$d" "${t0%.*}"
  done
done

squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" >squeue_end.txt
python3 "$K/summarize.py" "$RUN" >/dev/null && cat "$RUN/summary.md"
# Anything else this job left in /dev/shm; files only.
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

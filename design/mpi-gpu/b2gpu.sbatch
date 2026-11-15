#!/bin/bash
# MPI with GPUs through Dragon on PSC Bridges-2 (2026-11-02): rust's mpi2g.sbatch cases on V100s,
# with the mpi-launch kit's harness, mpi_gpu.c and summarizer (copied to $B/kit) and the venv of
# b2-setup.sbatch. MPI is HPC-X 2.12's Open MPI 4.1.5 from NVHPC 22.9, built with CUDA 11.7
# (smcuda, coll/cuda, UCX with its CUDA transport); its ranks are PMIx 3.2.3 clients. Dragon's
# PMIx server loads PSC's Open MPI 5.0.8's libpmix.so (PMIx 5.0.8; pmixlib/), and lists GPUs
# through NVHPC's libcudart.so (CUDA 11.7). Dragon: Slurm launcher with srun-hetjob first on PATH
# (short node names, --overlap), TCP over IPoIB, as in the fifth campaign.
#   One node, two GPUs (2 ranks):
#     sbatch -A dmr170002p -J d2 -d singleton -p GPU-shared --gpus=v100-32:2 -N 1 \
#       --ntasks-per-node=1 -c 10 -t 20 --export=ALL,RPN=2 b2gpu.sbatch
#   Two nodes, two GPUs each (2 + 2 ranks; whole nodes, charged 8 GPUs each):
#     sbatch -A dmr170002p -J d2 -d singleton -p GPU --gpus=v100-32:16 -N 2 \
#       --ntasks-per-node=1 -c 40 -t 20 --export=ALL,RPN=2 b2gpu.sbatch
# RPN: ranks per node, each on its own GPU. host mode reduces through host memory, device mode
# hands the device pointer to MPI_Allreduce. Baseline without Dragon, 3 runs per mode: HPC-X's
# mpirun, in mpirun/rep-1/; srun --mpi=pmix (Slurm's pmix_v2 plugin, in rep-1/) only with
# SRUN_BASELINE=1, since it hangs (below).
# Optional: CASES="<name> <how> <pmi> <ranks> <layout> <reps> <flag>;..." runs only those.
# One Dragon runtime per case; after each, as on rust: leftover steps cancelled, this user's
# processes whose working directory is the case's ended, and this user's /dev/shm files made since
# the case started removed if no other job of this user runs on the node. No dragon-cleanup.
#SBATCH --output=/ocean/projects/dmr170002p/mturilli/mpi-gpu/logs/%x-%j.out
set -u
JOB_START=$(date +%s)
B=/ocean/projects/dmr170002p/mturilli/mpi-gpu
K=$B/kit
RPN=${RPN:-1}
RUN=$B/runs/$SLURM_JOB_ID
REP=$RUN/rep-1
MREP=$RUN/mpirun/rep-1
mkdir -p "$REP" "$MREP" && cd "$RUN" || exit 1
mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
NODELIST=$(IFS=,; echo "${NODES[*]}")
NN=${#NODES[@]}
N=$((NN * RPN))
on_node() {  # on_node <host> <command...>: one task on that node, inside this job
  local h=$1; shift
  srun --overlap --nodes=1 --nodelist="$h" --ntasks=1 -c 1 --cpu_bind=none "$@"
}
NV=/opt/packages/nvidia/hpc_sdk/Linux_x86_64/22.9
source "$B/venv/bin/activate"
source "$NV/comm_libs/hpcx/hpcx-2.12/hpcx-init.sh"
hpcx_load
# pmixlib first (Dragon's libpmix.so), then HPC-X's libraries, then CUDA 11.7's runtime.
export LD_LIBRARY_PATH=$B/pmixlib:$LD_LIBRARY_PATH:$NV/cuda/lib64
# PMIx 5's server offers its shmem2 data store first; HPC-X's 3.2.3 clients have no shmem2, and
# their pmix_init fails on it (dry run 50120151). Both sides take the hash store instead.
export PMIX_MCA_gds=hash
export SRUN_HETJOB_LOG=$RUN/srun-hetjob.log
export PYTHONUNBUFFERED=1
echo "== job $SLURM_JOB_ID on $NODELIST ($SLURM_JOB_PARTITION), $RPN rank(s) per node: $(python -V 2>&1), dragonhpc $(pip show dragonhpc 2>/dev/null | awk '/^Version/{print $2}'), $(ompi_info --version 2>/dev/null | head -1) (HPC-X 2.12)"
squeue -a -w "$NODELIST" -h -o "%i|%u|%P|%T|%C|%b|%M|%j|%N" >squeue_start.txt
for h in "${NODES[@]}"; do
  on_node "$h" bash -c '
    echo "$(hostname): CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}; $(nvidia-smi -L 2>&1 | tr "\n" " ")"
    echo "    interfaces: $(ip -br -4 addr 2>/dev/null | awk "{print \$1, \$3}" | tr "\n" " ")"
    python - <<EOF
import ctypes
for n in ("libcuda.so.1", "libcudart.so", "libpmix.so"):
    try:
        ctypes.CDLL(n); print("   ", n, "loads")
    except OSError as e:
        print("   ", n, "FAILS:", e)
from dragon.infrastructure.gpu_desc import find_accelerators
print("    Dragon sees:", find_accelerators())
EOF'
done | tee gpus.txt
{
  echo "== build with HPC-X 2.12 (Open MPI 4.1.5, CUDA 11.7)"
  # NVHPC's HPC-X wrappers default to nvc; the system gcc is enough for mpi_gpu.c.
  export OMPI_CC=gcc
  echo "compiler: $(mpicc --version 2>&1 | head -1)"
  mpicc -O2 -o "$RUN/mpi_gpu" "$K/mpi_gpu.c" -ldl && echo "built mpi_gpu"
  ldd "$RUN/mpi_gpu" | grep -E "libmpi|libpmix|not found"
  ompi_info --parsable --all 2>/dev/null | grep -m1 mpi_built_with_cuda_support:value
  ompi_info 2>/dev/null | grep -E "MCA (pml|coll|pmix): (ucx|cuda|ob1|pmix3x|ext)|MCA btl: (smcuda|tcp)" | sed 's/^ */  /'
} >"$RUN/build.txt" 2>&1
[ -x "$RUN/mpi_gpu" ] || { cat "$RUN/build.txt"; echo "mpicc failed"; exit 1; }

baseline() {  # baseline <srun|mpirun> <mode>: N ranks of mpi_gpu, RPN per node, 3 runs
  local how=$1 mode=$2 name=baseline-$N-gpu-$2 d t0 rc rep_dir launcher limit=60
  local -a cmd
  if [ "$how" = srun ]; then
    rep_dir=$REP launcher="srun --mpi=pmix" limit=30
    cmd=(srun --overlap --mpi=pmix -N "$NN" --ntasks-per-node="$RPN" -c 1 --cpu_bind=none)
  else
    rep_dir=$MREP launcher="mpirun (HPC-X)"
    # ORTE starts its daemons with srun; --overlap lets that step share the job's CPUs. The
    # job has one task per node, so mpirun sees one slot per node.
    cmd=(env OMPI_MCA_plm_slurm_args=--overlap mpirun -np "$N" --map-by "ppr:$RPN:node" --bind-to none
         --oversubscribe -x LD_LIBRARY_PATH -x PATH -x OPAL_PREFIX -x PMIX_MCA_gds)
  fi
  echo "== baseline: $launcher, $N ranks of mpi_gpu $mode"
  for rep in 1 2 3; do
    d=$rep_dir/$name/out/rep-$rep && mkdir -p "$d" && t0=$(date +%s.%N)
    timeout -k 10 "$limit" "${cmd[@]}" "$RUN/mpi_gpu" "$d" "$N" "$mode" >"$d.out" 2>&1
    rc=$?
    python3 -c 'import json,sys,time; n,r,rc,t0,g,l,d=sys.argv[1:]; print(json.dumps({"ranks": int(n), "rep": int(r), "fail": False, "gpu": g, "rc": int(rc), "s": round(time.time() - float(t0), 3), "launcher": l, "dir": d}))' \
      "$N" "$rep" "$rc" "$t0" "$mode" "$launcher" "$name/out/rep-$rep" | tee -a "$rep_dir/baseline.jsonl"
    # srun --mpi=pmix hung in the dry run (no rank started); one timeout is enough to know.
    if ((rc == 124 || rc == 137)); then echo "   timed out: no more runs of this launcher"; return 1; fi
  done
}
# srun --mpi=pmix only with SRUN_BASELINE=1: in the dry runs its ranks never started, and its
# steps outlived scancel and the job, holding the node in "completing" (50120151, 50120580).
if [ "${SRUN_BASELINE:-0}" = 1 ]; then baseline srun host && baseline srun device; fi
baseline mpirun host
baseline mpirun device
job_steps() { squeue -h -s -j "$SLURM_JOB_ID" -o %i | grep -E "^$SLURM_JOB_ID\.[0-9]+\$" | grep -vxF -f <(printf '%s\n' $STUCK) | xargs; }
# The dry run's hung srun steps outlived scancel; cancelled once here, then left out of the
# cases' cleanup, which would otherwise wait a minute for them after every case.
STUCK=""
STUCK=$(job_steps)
if [ -n "$STUCK" ]; then echo "== baseline steps still running, cancelled once: $STUCK"; scancel $STUCK; fi

after_case() {  # after_case <name> <case dir> <start epoch>: cleanup-<name>.json
  local name=$1 d=$2 t0=$3 steps h others nodes=()
  steps=$(job_steps)
  if [ -n "$steps" ]; then
    echo "   cancelling leftover step(s) $steps"
    scancel $steps 2>/dev/null  # a step may end between squeue and scancel
    for _ in $(seq 20); do
      [ -z "$(job_steps)" ] && break
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
      shm=$(find /dev/shm -mindepth 1 -maxdepth 1 -user "$USER" -newermt "@$t0" | wc -l)
      if [ -z "$others" ]; then find /dev/shm -mindepth 1 -maxdepth 1 -user "$USER" -newermt "@$t0" -delete; gone=removed
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
mkdir -p "$SHIM" && cp /ocean/projects/dmr170002p/mturilli/sched-exp/experiments/srun-hetjob "$SHIM/srun" && chmod +x "$SHIM/srun"
# RM nodes name their InfiniBand port ib0; H100 nodes have eight, b2-ib0 to b2-ib7
# (job 50120751). One port, so that every node gives Dragon one address.
NETWORK_PREFIX='^(b2-)?ib0$'
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
python3 "$K/summarize.py" "$RUN/mpirun" && cat "$RUN/mpirun/summary.md"
# The baselines' Open MPI segments and anything else this job left in /dev/shm; files only.
for h in "${NODES[@]}"; do
  others=$(squeue -h -u "$USER" -w "$h" -o %i | grep -vx "$SLURM_JOB_ID" | xargs)
  on_node "$h" bash -c '
    others=$1 t0=$2
    n=$(find /dev/shm -mindepth 1 -maxdepth 1 -user "$USER" -newermt "@$t0" | wc -l)
    if [ -z "$others" ]; then find /dev/shm -mindepth 1 -maxdepth 1 -user "$USER" -newermt "@$t0" -delete; g=removed
    else g="kept (job(s) $others of $USER here)"; fi
    echo "== end $(hostname): $n /dev/shm file(s) since the job started, $g; Dragon processes $(pgrep -u "$USER" -fc "[d]ragon")"
  ' _ "$others" "$JOB_START"
done
echo "== done"

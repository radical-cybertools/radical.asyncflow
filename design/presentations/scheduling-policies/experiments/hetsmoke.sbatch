#!/bin/bash
# Third campaign smoke test: can one Dragon runtime span the components of a Slurm
# heterogeneous job (haskell in partition gpu, fortran in the hidden partition staging)?
#
#   sbatch -J d2 -d singleton -t 20 --output=$BASE/logs/%x-smoke-%j.out \
#     -p gpu -w haskell --gres=gpu:1 -c 28 --mem=64G : -p staging -w fortran --gres=gpu:1 -c 28 --mem=64G \
#     hetsmoke.sbatch
#
# 1. the hetjob's environment and what each component sees (CPUs, GPU, NICs, /dev/shm);
# 2. which srun forms Slurm accepts for one step over both components;
# 3. Dragon as run.sbatch starts it (-w slurm), its srun given only --overlap: which nodes it sees;
# 4. the same with srun-hetjob first on PATH, which turns Dragon's srun into one step over
#    every component (--het-group=0-N, one task per node).
# Everything runs inside the allocation, through srun: nothing over SSH.
# Every srun here uses --overlap and an explicit -n: in this heterogeneous job a step without
# either starts on the node but never runs its task, and srun then ignores SIGTERM (jobs 24780,
# 24782, 24786), so every timeout here also kills (-k).
BASE=/home/mturilli/tmp/sched-exp
JOB_START=$(date +%s)
RUN=$BASE/runs/$SLURM_JOB_ID
mkdir -p "$RUN"
export RUN_DIR=$RUN
DRAGON_TREE=${DRAGON_TREE:-$BASE/dragon-exp}
NETWORK_PREFIX='^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'
LAST=$((SLURM_HET_SIZE - 1))

echo "== 1. environment on $(hostname)"
env | grep -E '^SLURM_(JOB_ID|JOB_NODELIST|JOB_NUM_NODES|NNODES|NODELIST|HET_SIZE|JOB_PARTITION|CPUS_ON_NODE|JOB_GPUS|GPUS_ON_NODE|JOB_CPUS_PER_NODE)' | sort
for g in $(seq 0 $LAST); do
  echo "-- component $g"
  timeout -k 5 60 srun --overlap --het-group=$g -n1 bash -c 'echo host=$(hostname) nproc=$(nproc) CVD=$CUDA_VISIBLE_DEVICES SLURM_JOB_GPUS=$SLURM_JOB_GPUS;
    lscpu | grep -E "Model name"; nvidia-smi --query-gpu=index,pci.bus_id,name,memory.total,driver_version --format=csv,noheader;
    ip -br -4 addr | grep -v "^lo "; df -h /dev/shm | tail -1; uptime'
done

echo "== 2. srun forms"
# Not run again: "srun --overlap --het-group=0 hostname" (no -n) and srun without --overlap
# hang: the step starts, its task never does, and srun ignores timeout's TERM (24780, 24786).
for form in "--overlap --het-group=0 -n1 hostname" "--overlap --het-group=0 -n1 -l bash -c hostname" \
            "--overlap --het-group=0-$LAST -n1 -l hostname" "--overlap --het-group=0-$LAST --nodes=1 --ntasks=1 -l hostname" \
            "--overlap --nodes=$((LAST + 1)) --ntasks=$((LAST + 1)) -l hostname"; do
  echo "-- srun $form"
  timeout -k 5 20 srun $form 2>&1 | sed 's/^/   /'
  echo "   rc=${PIPESTATUS[0]}"
done

source "$DRAGON_TREE/_env/bin/activate"
source "$DRAGON_TREE/devtools/VARIABLES"
source /cvmfs/software.eessi.io/versions/2023.06/init/bash >/dev/null
module load GCCcore/13.2.0
export LD_LIBRARY_PATH="$EBROOTGCCCORE/lib64:$LD_LIBRARY_PATH"
export PYTHONPATH=$BASE/experiments
export PYTHONUNBUFFERED=1
echo "== Dragon tree $(cat "$DRAGON_TREE/COMMIT")"
cd "$RUN" || exit 1

echo "== 3. dragon -w slurm, as run.sbatch starts it (its srun given only --overlap)"
SHIM=$RUN/shim
mkdir -p "$SHIM"
cp "$BASE/experiments/srun-hetjob" "$SHIM/srun"
chmod +x "$SHIM/srun"
SECONDS=0
PATH=$SHIM:$PATH SRUN_HETJOB_MODE=overlap-only timeout --signal=INT --kill-after=30 180 \
  dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" "$BASE/experiments/smoke_twonode.py" --gpus \
  > "$RUN/attempt3.log" 2>&1
echo "   rc=$? wall=${SECONDS}s"; tail -25 "$RUN/attempt3.log" | sed 's/^/   /'
# Leftovers of attempt 3, this job's only: srun processes in this batch step's cgroup
# (scancel cannot cancel a step of a heterogeneous job here), then its /dev/shm files.
squeue -h -s -j "$SLURM_JOB_ID" -o "%i %j" | sed 's/^/   step: /'
for p in $(cat "/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)/cgroup.procs"); do
  [ "$p" != $$ ] && [ "$p" != "$BASHPID" ] && grep -qa '^srun' "/proc/$p/cmdline" 2>/dev/null && kill -KILL "$p"
done
sleep 5
for g in $(seq 0 $LAST); do
  timeout -k 5 60 srun --overlap --het-group=$g -n1 bash -c "find /dev/shm -maxdepth 1 -user \$USER -newermt @$JOB_START -delete" 2>/dev/null
done

echo "== 4. dragon -w slurm with srun-hetjob first on PATH"
SECONDS=0
PATH=$SHIM:$PATH timeout --signal=INT --kill-after=30 420 \
  dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" "$BASE/experiments/smoke_twonode.py" --gpus \
  --batch-kwargs '{"report_each_task": true, "client_max_batch_size": 1, "client_linger_sec": 0.0}' \
  > "$RUN/attempt4.log" 2>&1
echo "   rc=$? wall=${SECONDS}s"; tail -40 "$RUN/attempt4.log" | sed 's/^/   /'
grep -h "srun-hetjob" "$RUN"/attempt*.log | head -8 | sed 's/^/   /'

for g in $(seq 0 $LAST); do
  timeout -k 5 60 srun --overlap --het-group=$g -n1 bash -c "find /dev/shm -maxdepth 1 -user \$USER -newermt @$JOB_START -delete; \
    echo \"== cleanup \$(hostname): procs=\$(pgrep -u \$USER -fc '[d]ragon-') shm=\$(find /dev/shm -maxdepth 1 -user \$USER | wc -l)\""
done

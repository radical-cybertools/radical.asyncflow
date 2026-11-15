#!/bin/bash
# Run classes of Dragon's test/multi-node/test_batch.py on a source-built Dragon tree.
#   sbatch -J dh -d singleton -p cpu -w haskell -c 16 dragon_tests.sbatch <tree> <TestClass> [...]
# With STOCK_WHEEL=1 the tree's test files run on the stock dragonhpc 0.14.1 wheel instead.
# Cleans up only this job's /dev/shm files (no dragon-cleanup; see run.sbatch).
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=32G
#SBATCH --time=00:20:00
#SBATCH --output=/home/mturilli/tmp/sched-exp/logs/%x-tests-%j.out

T=${1:?usage: dragon_tests.sbatch <dragon tree> <TestClass> [...]}
shift
JOB_START=$(date +%s)
if [ -n "$STOCK_WHEEL" ]; then
  # The stock dragonhpc 0.14.1 wheel, running the tree's test files (a baseline).
  source /home/mturilli/tmp/dragon-rust/venv/bin/activate
  echo "== Dragon: stock wheel $(pip show dragonhpc | awk '/^Version/{print $2}')"
else
  source "$T/_env/bin/activate"
  source "$T/devtools/VARIABLES"
  source /cvmfs/software.eessi.io/versions/2023.06/init/bash >/dev/null
  module load GCCcore/13.2.0
  export LD_LIBRARY_PATH="$EBROOTGCCCORE/lib64:$LD_LIBRARY_PATH"
fi
export PYTHONUNBUFFERED=1
NETWORK_PREFIX='^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'
# Batch() in the tests sizes its pool from the node's 512 CPUs and would put 256 workers on
# this slice (design/dragon-rust-findings.md). Run a copy of the test file with Node.num_cpus
# capped to the CPUs Dragon may use, in the test (client) process where Batch() sizes the pool.
RUN=/home/mturilli/tmp/sched-exp/runs/$SLURM_JOB_ID
mkdir -p "$RUN"
cp -r "$T/test/multi-node/batch_utils" "$RUN/"
{
  echo "from dragon.native.machine import Node as _CapNode  # pool capped to the job's CPUs"
  echo "_cap_num_cpus = _CapNode.num_cpus"
  echo "_CapNode.num_cpus = property(lambda n: min(_cap_num_cpus.fget(n), len(n.cpus) or _cap_num_cpus.fget(n)))"
  cat "$T/test/multi-node/test_batch.py"
} > "$RUN/test_batch_capped.py"
cd "$RUN" || exit 1
echo "== job $SLURM_JOB_ID on $SLURM_JOB_NODELIST cpus=$SLURM_CPUS_ON_NODE; tests from $(cat "$T/COMMIT"), pool capped; $*"
SECONDS=0
timeout --signal=INT --kill-after=30 900 \
  dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" test_batch_capped.py -v "$@"
echo "== dragon rc=$? wall=${SECONDS}s"
srun --nodes=1 --ntasks=1 --cpu_bind=none bash -c \
  "find /dev/shm -maxdepth 1 -user \$USER -newermt @$JOB_START -delete; echo \"== cleanup \$(hostname): shm=\$(find /dev/shm -maxdepth 1 -user \$USER | wc -l)\""

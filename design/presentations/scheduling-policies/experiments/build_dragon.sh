#!/bin/bash
# Build a Dragon source tree on rust (BUILDING.md, TCP transport only), into its own venv.
#   sbatch build_dragon.sh /home/mturilli/tmp/sched-exp/dragon-exp
# The tree is a copy of a Dragon worktree without .git, with its commit in COMMIT. The recipe is
# the one in design/dragon-rust-findings.md: EESSI's GCCcore 13.2.0 targets the compat layer's
# glibc 2.37, which the compat-layer Python 3.11.4 runs on (the system gcc would build against
# glibc 2.41). At run time, GCC 13's libstdc++ must come before the compat layer's GCC 10 one.
# rhapsody-py and radical.asyncflow are installed without the dragon extra, so dragonhpc stays
# the source tree; the experiments put their branch trees first on PYTHONPATH anyway.
#SBATCH --job-name=dragon-build
#SBATCH --partition=cpu
#SBATCH --nodelist=haskell
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=00:45:00
#SBATCH --output=/home/mturilli/tmp/sched-exp/logs/%x-%j.out

TREE=${1:?usage: sbatch build_dragon.sh <dragon source tree>}
source /cvmfs/software.eessi.io/versions/2023.06/init/bash >/dev/null
module load GCCcore/13.2.0
PYBIN=$TREE/../pybin
mkdir -p "$PYBIN"
ln -sf /cvmfs/software.eessi.io/versions/2023.06/compat/linux/x86_64/usr/bin/python3.11 "$PYBIN/python3"
export PATH="$PYBIN:$PATH"
cd "$TREE" || exit 1
echo "== $(gcc --version | head -1); $(python3 --version) at $(command -v python3); tree $(cat COMMIT)"
export DRAGON_BASE_DIR=$PWD/src
SECONDS=0
. devtools/clean_build base
echo "== build rc=$? in ${SECONDS}s"
export LD_LIBRARY_PATH="$EBROOTGCCCORE/lib64:$LD_LIBRARY_PATH"
_env/bin/pip install -q "rhapsody-py==0.6.0" "radical.asyncflow==0.6.0" && echo "== rhapsody-py, radical.asyncflow installed"
_env/bin/python -c "
import inspect, dragon.workflows.batch.batch as b
print('== batch from', b.__file__)
print('== Batch takes', [p for p in ('client_linger_sec', 'client_max_batch_size', 'report_each_task', 'cap_launch_budget') if p in inspect.signature(b.Batch).parameters])
print('== options takes priority:', 'priority' in inspect.signature(b.Batch.options).parameters)
"

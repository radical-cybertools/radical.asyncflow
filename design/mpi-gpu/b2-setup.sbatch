#!/bin/bash
# The environment for MPI with GPUs through Dragon on PSC Bridges-2 (2026-11-02), for b2gpu.sbatch:
# - venv/: the uv-installed CPython 3.11 of the fifth campaign (sched-exp/tools/python) with
#   dragonhpc 0.15.0, the mpi-launch kit's RHAPSODY archive (job PMI of 757465d, native backend
#   with the group PMI) and AsyncFlow 437a10d, as the kit's KIT_PIP (no mpi4py: no case uses it);
# - pmixlib/: libpmix.so alone, from PSC's Open MPI 5.0.8 (PMIx 5.0.8, absolute RPATH), for
#   Dragon's PMIx server, which dlopens that name. HPC-X's ompi/lib has a libpmix.so too, PMIx
#   3.2.3, which the ranks use as clients; pmixlib/ goes first on LD_LIBRARY_PATH so that Dragon
#   takes 5.0.8, and holds no libpmix.so.2, so that the ranks still take HPC-X's.
# Then checks that the venv's Python loads that libpmix.so and lists Dragon's PMI backends.
#   sbatch -A dmr170002p -p RM-shared -N 1 -n 1 -c 2 -t 30 b2-setup.sbatch
#SBATCH --output=/ocean/projects/dmr170002p/mturilli/mpi-gpu/logs/%x-%j.out
set -euo pipefail
B=/ocean/projects/dmr170002p/mturilli/mpi-gpu
PY=/ocean/projects/dmr170002p/mturilli/sched-exp/tools/python/cpython-3.11-linux-x86_64-gnu/bin/python3.11
echo "== $(hostname): PyPI $(curl -s -o /dev/null -w '%{http_code}' --max-time 20 https://pypi.org/simple/pip/), GitHub $(curl -s -o /dev/null -w '%{http_code}' --max-time 20 https://github.com)"
[ -e "$B/venv" ] && { echo "$B/venv exists"; exit 1; }
"$PY" -m venv "$B/venv"
source "$B/venv/bin/activate"
python -m pip install -q -U pip
python -m pip install -q "dragonhpc[batch,telemetry]==0.15.0" \
  "$B/kit/vendor/rhapsody_py-0.6.0.tar.gz" \
  "git+https://github.com/radical-cybertools/radical.asyncflow@437a10dbd1100af8374cd9817c8247dbfc8bccc3"
python -m pip freeze >"$B/venv/pip-freeze.txt"
grep -iE "^(dragonhpc|rhapsody|radical)" "$B/venv/pip-freeze.txt"
mkdir -p "$B/pmixlib"
ln -sfn /opt/packages/openmpi/5.0.8/gnu-13.3.1/lib/libpmix.so "$B/pmixlib/libpmix.so"
LD_LIBRARY_PATH=$B/pmixlib python - <<'EOF'
import ctypes
ctypes.CDLL("libpmix.so")
print("libpmix.so loads")
from dragon.infrastructure.facts import PMIBackend
print("Dragon PMI backends:", [p.value for p in PMIBackend])
import inspect
import rhapsody.backends.execution.dragon_native as n
print("pmi in DragonNativeExecutionBackend:", "pmi" in inspect.signature(n.DragonNativeExecutionBackend.__init__).parameters)
EOF

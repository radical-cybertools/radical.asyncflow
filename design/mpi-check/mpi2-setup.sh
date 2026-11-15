#!/bin/bash
# The venv for the MPI checks of 2026-11-02 on rust (MPI on the native backend, mpi4py, GPUs):
# EESSI 2023.06's Python 3.11, as pmi2n-setup.sh, with dragonhpc 0.15.0, the mpi-launch kit's
# RHAPSODY archive (the job PMI of 757465d with the native backend's group PMI), AsyncFlow
# 437a10d and mpi4py's PyPI wheel. Then checks that the wheel finds EESSI's Open MPI 4.1.6, and
# whether that Open MPI is CUDA-aware. Run inside a job, not on the login node:
#   srun -p cpu -w julia -N 1 -c 2 --mem=4G -t 20 bash setup.sh
set -euo pipefail
B=/home/mturilli/tmp/rhapsody-mpi2
PY=/cvmfs/software.eessi.io/versions/2023.06/compat/linux/x86_64/usr/bin/python3.11
[ -e "$B/venv" ] && { echo "$B/venv exists"; exit 1; }
"$PY" -m venv "$B/venv"
source "$B/venv/bin/activate"
python -m pip install -q -U pip
python -m pip install -q "dragonhpc[batch,telemetry]==0.15.0" \
  "$B/kit/kits/mpi-launch/vendor/rhapsody-native-mpi-pmi-757465d/rhapsody_py-0.6.0.tar.gz" \
  "git+https://github.com/radical-cybertools/radical.asyncflow@437a10dbd1100af8374cd9817c8247dbfc8bccc3" \
  mpi4py
python -m pip freeze >"$B/venv/pip-freeze.txt"
grep -iE "^(dragonhpc|rhapsody|radical|mpi4py)" "$B/venv/pip-freeze.txt"
python - <<'EOF'
import inspect
import rhapsody.backends.execution.dragon_native as n
print("pmi in DragonNativeExecutionBackend:", "pmi" in inspect.signature(n.DragonNativeExecutionBackend.__init__).parameters)
EOF

export EESSI_SOFTWARE_SUBDIR_OVERRIDE=x86_64/generic
set +u
source /cvmfs/software.eessi.io/versions/2023.06/init/bash >/dev/null 2>&1
module load OpenMPI/4.1.6-GCC-13.2.0 >/dev/null 2>&1
set -u
# EESSI's modules rely on RPATH and set no LD_LIBRARY_PATH; the wheel dlopens libmpi.so.40.
export LD_LIBRARY_PATH=$EBROOTOPENMPI/lib:$EBROOTPMIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
echo "== Open MPI CUDA support: $(ompi_info --parsable --all 2>/dev/null | grep -m1 mpi_built_with_cuda_support:value || echo unknown)"
echo "== mpi4py, one process (singleton MPI_Init):"
"$B/venv/bin/python" -c 'from mpi4py import MPI; print(MPI.COMM_WORLD.Get_size(), MPI.Get_library_version().splitlines()[0])'

#!/bin/bash
# The venv for the RHAPSODY job-PMI check on rust: EESSI 2023.06's Python 3.11.4, as
# venv-s0150, with dragonhpc 0.15.0, the kit's RHAPSODY archive (PMI branch 8607783 plus the
# native backend of 88599b7) and AsyncFlow 437a10d. Run inside a job, not on the login node:
#   srun -p cpu -w julia -N 1 -c 2 --mem=4G -t 20 bash setup.sh
set -euo pipefail
B=/home/mturilli/tmp/rhapsody-pmi
PY=/cvmfs/software.eessi.io/versions/2023.06/compat/linux/x86_64/usr/bin/python3.11
[ -e "$B/venv" ] && { echo "$B/venv exists"; exit 1; }
"$PY" -m venv "$B/venv"
source "$B/venv/bin/activate"
python -m pip install -q -U pip
python -m pip install -q "dragonhpc[batch,telemetry]==0.15.0" \
  "$B/kit/vendor/rhapsody-88599b7-pmi-8607783/rhapsody_py-0.6.0.tar.gz" \
  "git+https://github.com/radical-cybertools/radical.asyncflow@437a10dbd1100af8374cd9817c8247dbfc8bccc3"
python -m pip freeze >"$B/venv/pip-freeze.txt"
python - <<'EOF'
import inspect
import rhapsody.backends.execution.dragon as d
import rhapsody.backends.execution.dragon_native as n
print("pmi in DragonExecutionBackend:", "pmi" in inspect.signature(d.DragonExecutionBackend.__init__).parameters,
      "| native:", n.DragonNativeExecutionBackend.__name__)
EOF
grep -iE "^(dragonhpc|rhapsody|radical)" "$B/venv/pip-freeze.txt"

"""Run mpi_hello as one Batch job of <n> ranks; pmi is pmix, cray, none, or default (not
passed, as RHAPSODY does today)."""
import os
import sys
import time

from dragon.infrastructure.facts import PMIBackend
from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch

exe, outdir, n, pmi = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
os.makedirs(outdir, exist_ok=True)
b = Batch(num_nodes=1, workers_per_node=8)
kw = {} if pmi == "default" else {"pmi": None if pmi == "none" else PMIBackend(pmi)}
t0 = time.time()
task = b.job([(n, ProcessTemplate(exe, args=(outdir, str(n)), cwd=outdir))], timeout=60, **kw)
try:
    res = task.get(timeout=90)
    out = f"exit_codes={res}"
except Exception as e:  # noqa: BLE001 - report whatever Batch raises
    out = f"raised {type(e).__name__}: {str(e)[:300]!r}"
print(f"RESULT batch pmi={pmi} n={n} {out} s={time.time() - t0:.2f}", flush=True)
b.close()
b.join()

"""Run mpi_hello's <n> ranks under Open MPI's own mpirun, started as one Batch process task:
Dragon sees a single process and no PMI is involved."""
import os
import sys
import time

from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch

exe, outdir, n, _ = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
os.makedirs(outdir, exist_ok=True)
b = Batch(num_nodes=1, workers_per_node=8)
t0 = time.time()
task = b.options(timeout=60).process(
    ProcessTemplate("/usr/bin/mpirun", args=("--oversubscribe", "-np", str(n), exe, outdir, str(n)), cwd=outdir))
try:
    out = f"exit_code={task.get(timeout=90)}"
except Exception as e:  # noqa: BLE001 - report whatever Batch raises
    out = f"raised {type(e).__name__}: {str(e)[:300]!r}"
print(f"RESULT batch-mpirun n={n} {out} s={time.time() - t0:.2f}", flush=True)
b.close()
b.join()

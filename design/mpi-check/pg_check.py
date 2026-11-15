"""Run mpi_hello as one ProcessGroup of <n> ranks with the given PMI (pmix, cray or none)."""
import os
import sys
import time

from dragon.infrastructure.facts import PMIBackend
from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup

exe, outdir, n, pmi = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
os.makedirs(outdir, exist_ok=True)
t0 = time.time()
grp = ProcessGroup(restart=False, ignore_error_on_exit=True,
                   pmi=None if pmi == "none" else PMIBackend(pmi), walltime=60)
grp.add_process(nproc=n, template=ProcessTemplate(target=exe, args=(outdir, str(n)), cwd=outdir))
grp.init()
grp.start()
grp.join(timeout=90)
codes = sorted(c for _, c in grp.inactive_puids)
grp.close()
print(f"RESULT pg pmi={pmi} n={n} exit_codes={codes} s={time.time() - t0:.2f}", flush=True)

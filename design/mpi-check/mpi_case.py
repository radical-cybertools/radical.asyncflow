"""One MPI case inside a Dragon runtime, repeated <reps> times; prints one RESULT line per repetition.

  mpi_case.py <exe> <outdir> <how> <pmi> <ranks> <layout> <reps> [fail]

how     pg (ProcessGroup), batch (Batch.job), mpirun (Open MPI's mpirun as one Batch process task)
pmi     pmix, cray, none, or default (not passed: Batch's own default, as RHAPSODY does)
layout  split (half the ranks on each of the first two nodes, by host name), first (all on the
        first node, by host name), on-<host> (all on that node), default (one
        template, placed by Dragon), or conc<K> (Batch: K jobs of <ranks> at once, default layout)
fail    rank 1 exits right after MPI_Init; the group or job has a 30 s limit
"""
import os
import sys
import time

from dragon.infrastructure.facts import PMIBackend
from dragon.infrastructure.policy import Policy
from dragon.native.machine import Node, System
from dragon.native.process import ProcessTemplate

exe, outdir, how, pmi, n, layout, reps = sys.argv[1:8]
n, reps = int(n), int(reps)
fail = sys.argv[8:9] == ["fail"]
limit = 30 if fail else 90
hosts = [Node(h).hostname for h in System().nodes]
print(f"hosts {hosts}", flush=True)


def templates(out):
    args = (out, str(n)) + (("fail",) if fail else ())
    if how == "mpirun":
        cmd = (f"SLURM_OVERLAP=1 exec mpirun --host {hosts[0]}:{n // 2},{hosts[1]}:{n // 2} --map-by ppr:{n // 2}:node"
               f" -np {n} {' '.join([exe, *args])}")
        return [(1, ProcessTemplate("/bin/bash", args=("-c", cmd), cwd=out))]
    if layout == "first" or layout.startswith("on-"):
        host = hosts[0] if layout == "first" else layout[3:]
        return [(n, ProcessTemplate(exe, args=args, cwd=out,
                                    policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=host)))]
    if layout == "split":
        return [(n // 2, ProcessTemplate(exe, args=args, cwd=out,
                                         policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=h)))
                for h in hosts[:2]]
    return [(n, ProcessTemplate(exe, args=args, cwd=out))]


def outdir_for(rep, k=None):
    out = f"{outdir}/rep-{rep}" + (f"-job{k}" if k is not None else "")
    os.makedirs(out, exist_ok=True)
    return out


def pg_once(out):
    from dragon.native.process_group import ProcessGroup

    grp = ProcessGroup(restart=False, ignore_error_on_exit=True, walltime=limit,
                       pmi=None if pmi in ("none", "default") else PMIBackend(pmi))
    for k, t in templates(out):
        grp.add_process(nproc=k, template=t)
    grp.init()
    grp.start()
    grp.join(timeout=limit + 30)
    codes = sorted(c for _, c in grp.inactive_puids)
    grp.close()
    return codes


batch = None
if how in ("batch", "mpirun"):
    from dragon.workflows.batch import Batch

    batch = Batch(workers_per_node=8)
    kw = {} if pmi == "default" or how == "mpirun" else {"pmi": None if pmi == "none" else PMIBackend(pmi)}
    opts = batch.options(timeout=limit, **kw)

for rep in range(1, reps + 1):
    t0 = time.time()
    try:
        if how == "pg":
            out = f"exit_codes={pg_once(outdir_for(rep))}"
        elif layout.startswith("conc"):
            tasks = [opts.job(templates(outdir_for(rep, k))) for k in range(int(layout[4:]))]
            res = [t.get(timeout=limit + 30) for t in tasks]
            out = f"jobs={len(res)} all_zero={all(c == 0 for r in res for c in r)} first={res[0]}"
        else:
            tmpl = templates(outdir_for(rep))
            task = opts.process(tmpl[0][1]) if how == "mpirun" else opts.job(tmpl)
            out = f"exit_codes={task.get(timeout=limit + 30)}"
    except Exception as e:  # noqa: BLE001 - report whatever Dragon raises
        out = f"raised {type(e).__name__}: {str(e)[:300]!r}"
    print(f"RESULT {how} pmi={pmi} n={n} layout={layout}{' fail' if fail else ''} rep={rep} {out}"
          f" s={time.time() - t0:.2f}", flush=True)

if batch is not None:
    batch.close()
    batch.join()

"""Diagnostic (third campaign): what limits Batch's throughput over two nodes?

Raw Batch, no AsyncFlow or RHAPSODY, inside one two-node Dragon runtime, each node's pool
sized to the CPUs Dragon may use there. For Batch over one node (``num_nodes=1``) and over both,
in alternating order, ``--reps`` times:
- a warm-up of one trivial task per worker;
- throughput: ``--tasks`` trivial function tasks submitted in a tight loop, then all awaited;
- latency: a chain of ``--chain`` trivial tasks, each submitted when the previous one is done.
``--batch-kwargs`` go to every ``Batch()`` (the Dragon branch's (a)+(b), for example).
Each task returns its host. Results: one line per measurement, and ``$RUN_DIR/diag_twonode.json``.
``--layouts`` (Bridges-2): the Batch node counts to measure, e.g. ``1,2,4`` in a four-node runtime,
forward in odd repetitions and reversed in even ones; ``--tag`` names the result file
``diag_twonode_<tag>_<job>.json`` and copies it to ``$RESULTS_DIR``.
"""

import argparse
import json
import multiprocessing as mp
import os
import socket
import time
from collections import Counter

import dragon  # noqa: F401
from common import dragon_nodes, log
from dragon.native.machine import Node
from dragon.workflows.batch import Batch


def host(*_):
    return socket.gethostname()


def measure(n, kwargs, tasks, chain):
    t = time.time()
    batch = Batch(num_nodes=n, **kwargs)
    start_s = time.time() - t
    workers = batch.num_workers * n
    for f in [batch.function(host) for _ in range(workers)]:
        f.get(block=True)
    t = time.perf_counter()
    fs = [batch.function(host) for _ in range(tasks)]
    hosts = [f.get(block=True) for f in fs]
    tput = tasks / (time.perf_counter() - t)
    steps, chain_hosts = [], []
    for _ in range(chain):
        t = time.perf_counter()
        chain_hosts.append(batch.function(host).get(block=True))
        steps.append((time.perf_counter() - t) * 1e3)
    batch.join()
    batch.destroy()
    steps.sort()
    return {"num_nodes": n, "workers": workers, "batch_start_s": start_s, "tasks_per_s": tput,
            "hosts": dict(Counter(hosts)), "chain_p50_ms": steps[len(steps) // 2],
            "chain_hosts": dict(Counter(chain_hosts))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-kwargs", default="{}")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--tasks", type=int, default=2000)
    ap.add_argument("--chain", type=int, default=100)
    ap.add_argument("--layouts", default="1,2")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()
    layouts = [int(x) for x in args.layouts.split(",")]
    mp.set_start_method("dragon")
    nodes = dragon_nodes()
    num_cpus = Node.num_cpus
    Node.num_cpus = property(lambda n: min(num_cpus.fget(n), len(n.cpus) or num_cpus.fget(n)))
    kwargs = json.loads(args.batch_kwargs)
    log(f"nodes {nodes}; client on {socket.gethostname()}; batch_kwargs {kwargs}")
    out = {"nodes": nodes, "client_host": socket.gethostname(), "batch_kwargs": kwargs, "runs": [],
           "slurm_job_id": os.environ.get("SLURM_JOB_ID"), "dragon": os.environ.get("DRAGON_VERSION"),
           "tag": args.tag, "layouts": layouts}
    for rep in range(args.reps):
        for n in (layouts if rep % 2 == 0 else layouts[::-1]):
            r = measure(n, kwargs, args.tasks, args.chain)
            r["rep"] = rep + 1
            out["runs"].append(r)
            log(f"rep {rep + 1}, Batch over {n} node(s), {r['workers']} workers: {r['tasks_per_s']:.0f} tasks/s "
                f"{r['hosts']}; dependent step p50 {r['chain_p50_ms']:.1f} ms {r['chain_hosts']}")
    name = f"diag_twonode_{args.tag}_{out['slurm_job_id']}.json" if args.tag else "diag_twonode.json"
    dirs = [os.environ.get("RUN_DIR", ".")] + ([os.environ["RESULTS_DIR"]] if args.tag and "RESULTS_DIR" in os.environ else [])
    for d in dirs:
        with open(os.path.join(d, name), "w") as f:
            json.dump(out, f, indent=1)


if __name__ == "__main__":
    main()

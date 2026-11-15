"""Smoke test: one Dragon runtime over two nodes (third campaign).

Raw Batch, no RHAPSODY, with each node's pool sized to the CPUs Dragon may use there. It
prints Dragon's nodes and Batch's pool, then where tasks run:
- 64 function tasks (pool workers): host, CUDA_VISIBLE_DEVICES, CPUs allowed;
- with ``--gpus``, 8 process tasks without a placement, each running ``gpu_task.py`` for 1 s:
  host and GPU bus id;
- with ``--gpus``, one process per node placed there with GPU 0 (``Policy(HOST_NAME)``),
  running ``gpu_task.py --calibrate``: the GPU model, its kernel rate and the memory free
  after one CUDA context.
Records go to ``$RUN_DIR/smoke``; a summary goes to stdout.
"""

import argparse
import collections
import glob
import json
import multiprocessing as mp
import os
import socket
import sys
import time

import dragon  # noqa: F401
from common import dragon_nodes, log
from dragon.infrastructure.policy import Policy
from dragon.native.machine import Node
from dragon.native.process import ProcessTemplate
from dragon.workflows.batch import Batch


def where(i):
    return i, socket.gethostname(), os.environ.get("CUDA_VISIBLE_DEVICES"), len(os.sched_getaffinity(0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-kwargs", default="{}")
    ap.add_argument("--gpus", action="store_true")
    args = ap.parse_args()
    mp.set_start_method("dragon")
    nodes = dragon_nodes()
    log(f"Dragon nodes (host, num_cpus, allowed cpus, gpus): {nodes}")
    num_cpus = Node.num_cpus
    Node.num_cpus = property(lambda n: min(num_cpus.fget(n), len(n.cpus) or num_cpus.fget(n)))
    t = time.time()
    batch = Batch(**json.loads(args.batch_kwargs))
    log(f"Batch: {batch.num_workers} workers in {time.time() - t:.1f} s; kwargs {args.batch_kwargs}")
    out = os.path.join(os.environ.get("RUN_DIR", "."), "smoke")
    os.makedirs(out, exist_ok=True)

    t = time.time()
    res = [f.get(block=True) for f in [batch.function(where, i) for i in range(64)]]
    log(f"64 functions in {time.time() - t:.2f} s: per host {dict(collections.Counter(r[1] for r in res))}; "
        f"CUDA_VISIBLE_DEVICES {dict(collections.Counter((r[1], r[2]) for r in res))}; "
        f"allowed CPUs per worker {sorted(set(r[3] for r in res))}")

    if args.gpus:
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gpu_task.py")
        t = time.time()
        jobs = [
            batch.process(ProcessTemplate(
                target=sys.executable,
                args=[script, "--mem-gib", "0.2", "--secs", "1", "--gpu-frac", "0.1", "--period", "1",
                      "--iters-per-ms", "79000", "--tag", f"u{i}", "--out", os.path.join(out, f"u{i}.json")]))
            for i in range(8)
        ]
        for j in jobs:
            j.get(block=True)
        recs = [json.load(open(p)) for p in sorted(glob.glob(os.path.join(out, "u*.json")))]
        log(f"8 unplaced processes in {time.time() - t:.1f} s: "
            f"{[(r['tag'], r['host'], r.get('bus'), r.get('cvd'), r['status']) for r in recs]}")
        t = time.time()
        placed = []
        for host, _, _, gpus in nodes:
            pol = Policy(placement=Policy.Placement.HOST_NAME, host_name=host, gpu_affinity=list(gpus[:1]))
            path = os.path.join(out, f"calib_{host}.json")
            placed.append(batch.process(ProcessTemplate(
                target="/bin/bash", args=["-c", f"{sys.executable} {script} --calibrate > {path}"], policy=pol)))
        for j in placed:
            j.get(block=True)
        for host, *_ in nodes:
            path = os.path.join(out, f"calib_{host}.json")
            log(f"placed on {host}: {open(path).read().strip() if os.path.exists(path) else 'no output'}")
        log(f"placed processes in {time.time() - t:.1f} s")
    batch.join()
    batch.destroy()
    log("done")


if __name__ == "__main__":
    main()

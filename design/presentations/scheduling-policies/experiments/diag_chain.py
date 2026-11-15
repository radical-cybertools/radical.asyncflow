"""Diagnostic C (third campaign): what a dependency edge costs inside Dragon's Batch.

Raw Batch, no AsyncFlow or RHAPSODY, on one node, the pool sized to the job's CPUs. A chain
of ``--length`` trivial tasks: each takes the previous result and returns its value + 1,
with ``time.time()`` at the start and end of its body (all workers are on one node, so one
clock). Variants, each with a fresh Batch, ``--chains`` chains each:

- ``seq``: task i+1 is submitted after the client has task i's result (Dragon's client round
  trip);
- ``ahead1``: all tasks submitted up front in a tight loop, task i+1 taking task i (the Batch
  Task) as its argument. Batch's client compiles submissions that arrive within its linger
  (5 ms) together, up to 256, so the chain is one compile; the largest gap between
  submissions is recorded as evidence;
- ``aheadN``: the same, with every task compiled alone: on stock 0.14.1 by setting
  ``_drain_client_task_batch.__defaults__ = (0.0, 1)``, on the branch with
  ``client_max_batch_size=1, client_linger_sec=0``. The edges then cross compiles.

``--batch-kwargs`` go to every Batch (the branch's ``report_each_task``). ``--payload`` adds a
bytes payload of that many bytes passed along the chain. Results: ``$RESULTS_DIR``.
"""

import argparse
import json
import multiprocessing as mp
import os
import statistics
import time

import dragon  # noqa: F401
from common import log, pct
from dragon.native.machine import Node
from dragon.workflows.batch import Batch
from dragon.workflows.batch import batch as batch_mod


def step(prev):
    t0 = time.time()
    value = 0 if prev is None else prev[0] + 1
    payload = None if prev is None else prev[3]
    return (value, t0, time.time(), payload)


def first(payload):
    t0 = time.time()
    return (0, t0, time.time(), payload)


def run_chain(batch, variant, length, payload):
    sub, got = [], []
    t_chain = time.time()
    if variant == "seq":
        prev = None
        res = []
        for i in range(length):
            sub.append(time.time())
            t = batch.function(first, payload) if i == 0 else batch.function(step, prev)
            prev = t.get(block=True)
            got.append(time.time())
            res.append(prev)
    else:
        tasks = []
        for i in range(length):
            sub.append(time.time())
            tasks.append(batch.function(first, payload) if i == 0 else batch.function(step, tasks[-1]))
        res = []
        for t in tasks:
            res.append(t.get(block=True))
            got.append(time.time())
    total = got[-1] - t_chain
    gaps = [(res[i + 1][1] - res[i][2]) * 1e3 for i in range(length - 1)]
    sub_gaps = [(b - a) * 1e3 for a, b in zip(sub, sub[1:])]
    return {
        "final_value": res[-1][0],
        "total_s": total,
        "gap_ms": {"p50": pct(gaps, 50), "p95": pct(gaps, 95), "max": max(gaps), "mean": statistics.mean(gaps)},
        "gaps_ms": gaps,
        "submit_to_first_start_ms": (res[0][1] - sub[0]) * 1e3,
        "submit_loop_ms": (sub[-1] - sub[0]) * 1e3,
        "max_submit_gap_ms": max(sub_gaps),
        "last_end_to_client_ms": (got[-1] - res[-1][2]) * 1e3,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-kwargs", default="{}")
    ap.add_argument("--chains", type=int, default=5)
    ap.add_argument("--length", type=int, default=100)
    ap.add_argument("--payloads", default="0,1048576")
    ap.add_argument("--rep", type=int, default=1)
    args = ap.parse_args()
    mp.set_start_method("dragon")
    num_cpus = Node.num_cpus
    Node.num_cpus = property(lambda n: min(num_cpus.fget(n), len(n.cpus) or num_cpus.fget(n)))
    base = json.loads(args.batch_kwargs)
    branch = "report_each_task" in base
    defaults = batch_mod._drain_client_task_batch.__defaults__
    out = {"rep": args.rep, "job": os.environ.get("SLURM_JOB_ID"), "slurm_job_id": os.environ.get("SLURM_JOB_ID"), "dragon": os.environ.get("DRAGON_VERSION"),
           "batch_kwargs": base, "drain_defaults": list(defaults), "runs": []}
    variants = ["seq", "ahead1", "aheadN"]
    order = variants if args.rep % 2 else variants[::-1]
    for payload_n in [int(x) for x in args.payloads.split(",")]:
        payload = b"x" * payload_n if payload_n else None
        for variant in order:
            kw = dict(base)
            if variant == "aheadN":
                if branch:
                    kw.update(client_max_batch_size=1, client_linger_sec=0.0)
                else:
                    batch_mod._drain_client_task_batch.__defaults__ = (0.0, 1)
            batch = Batch(**kw)
            for _ in range(batch.num_workers):  # warm the pool
                batch.function(first, None).get(block=True)
            for c in range(args.chains):
                r = run_chain(batch, variant, args.length, payload)
                r.update(variant=variant, payload_bytes=payload_n, chain=c + 1, workers=batch.num_workers,
                         batch_kwargs=kw)
                out["runs"].append(r)
                g = r["gap_ms"]
                log(f"{variant} payload {payload_n}: chain {c + 1}: total {r['total_s'] * 1e3:.0f} ms, gap p50 "
                    f"{g['p50']:.2f} p95 {g['p95']:.2f} max {g['max']:.2f} ms, submit->start1 "
                    f"{r['submit_to_first_start_ms']:.1f} ms, submit loop {r['submit_loop_ms']:.1f} ms "
                    f"(max gap {r['max_submit_gap_ms']:.2f}), final {r['final_value']}")
            batch.join()
            batch.destroy()
            batch_mod._drain_client_task_batch.__defaults__ = defaults
    path = os.path.join(os.environ.get("RESULTS_DIR", "."),
                        f"chain_{'branch' if branch else 'stock'}_r{args.rep}_{out['job']}.json")
    with open(path, "w") as f:
        json.dump(out, f)
    log(f"wrote {path}")


if __name__ == "__main__":
    main()

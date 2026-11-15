"""Diagnostic: raw Dragon Batch, no RHAPSODY. When do poll() and get() report a task?

Case A submits 6 sleep functions of 1..6 s in a tight loop (Batch coalesces them into one
compiled batch); case B submits them 50 ms apart (beyond Batch's 5 ms linger). For each task
it logs when its function ended, when poll() returned its tuid, and when get() returned.

Options (second campaign; without them the script is what the first campaign ran):
``--patch-defaults`` applies the no-change patch (client grouping defaults (0.0, 1)), and
``--batch-kwargs '{...}'`` passes options to ``Batch()`` (the Dragon branch's switches).
"""

import argparse
import json
import multiprocessing as mp
import time

import dragon  # noqa: F401
from common import log
from dragon.native.machine import Node
from dragon.workflows.batch import Batch


def sleeper(d):
    time.sleep(d)
    return time.time()


def run(batch, gap):
    t0 = time.time()
    tasks = {}
    for d in range(1, 7):
        t = batch.function(sleeper, d)
        tasks[t.uid] = (d, t)
        if gap:
            time.sleep(gap)
    seen = {}
    while len(seen) < len(tasks):
        tuid = batch.poll(timeout=10)
        if tuid is None:
            break
        polled = time.time()
        d, t = tasks[tuid]
        ended = t.get(block=True)
        seen[d] = (ended - t0, polled - t0, time.time() - t0)
    for d in sorted(seen):
        e, p, g = seen[d]
        log(f"gap {gap * 1e3:.0f} ms, sleep {d}: ended +{e:.2f} s, poll() +{p:.2f} s, get() +{g:.2f} s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--patch-defaults", action="store_true")
    ap.add_argument("--batch-kwargs", default="{}")
    args = ap.parse_args()
    mp.set_start_method("dragon")
    if args.patch_defaults:
        from dragon.workflows.batch import batch as batch_module

        batch_module._drain_client_task_batch.__defaults__ = (0.0, 1)
    num_cpus = Node.num_cpus
    Node.num_cpus = property(lambda n: min(num_cpus.fget(n), len(n.cpus) or num_cpus.fget(n)))
    batch = Batch(**json.loads(args.batch_kwargs))
    log(f"{batch.num_workers} workers; patch_defaults={args.patch_defaults} batch_kwargs={args.batch_kwargs}")
    w = [batch.function(sleeper, 0) for _ in range(batch.num_workers)]
    for t in w:
        t.get(block=True)
    while batch.poll(timeout=0.5) is not None:
        pass
    run(batch, 0)
    run(batch, 0.05)
    batch.join()
    batch.destroy()


if __name__ == "__main__":
    main()

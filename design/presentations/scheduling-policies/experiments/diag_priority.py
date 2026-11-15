"""Diagnostic: does Dragon's Batch start tasks in the order of a client priority?

Raw Batch, no RHAPSODY, with the pool sized to the job's CPUs (W workers). Every task sleeps
2 s and returns when it started.

- Case A: 2W low-priority tasks (0), then 1 high-priority task (10), in one tight loop, so
  they compile together and the high one is last in Batch's own (HEFT) order. With priority
  respected, it starts in the first wave.
- Case B: 2W high-priority tasks (10) in one tight loop, then, 0.5 s later, 1 low-priority
  task (0) on its own. Batch's own order lets a task submitted alone overtake a backlog
  (it is first in its compile batch); with priority respected, it starts last.

``--batch-kwargs '{...}'`` passes options to ``Batch()``. Priorities are passed through
``Batch.options(priority=...)`` when this Dragon accepts them, and left out otherwise (stock
0.14.1), which gives the reference order.
"""

import argparse
import inspect
import json
import multiprocessing as mp
import time

import dragon  # noqa: F401
from common import log
from dragon.native.machine import Node
from dragon.workflows.batch import Batch


def sleeper(d):
    t = time.time()
    time.sleep(d)
    return t


def submit(batch, priority, takes_priority, d=2.0):
    opts = batch.options(priority=priority) if takes_priority else batch.options()
    return opts.function(sleeper, d)


def report(name, tasks, probe, workers):
    starts = sorted((t.get(block=True), label) for label, t in tasks)
    t0 = starts[0][0]
    rank = next(i for i, (_, label) in enumerate(starts) if label == probe)
    start = next(s for s, label in starts if label == probe)
    wave = int((start - t0) // 2.0) + 1
    log(f"{name}: {len(starts)} tasks on {workers} workers; '{probe}' started {rank + 1}th of {len(starts)}, "
        f"at +{start - t0:.2f} s (wave {wave})")
    return {"rank": rank + 1, "of": len(starts), "start_s": start - t0, "wave": wave}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-kwargs", default="{}")
    args = ap.parse_args()
    mp.set_start_method("dragon")
    num_cpus = Node.num_cpus
    Node.num_cpus = property(lambda n: min(num_cpus.fget(n), len(n.cpus) or num_cpus.fget(n)))
    batch = Batch(**json.loads(args.batch_kwargs))
    w = batch.num_workers
    takes = "priority" in inspect.signature(batch.options).parameters
    log(f"{w} workers; batch_kwargs={args.batch_kwargs}; options take priority: {takes}")
    for t in [batch.function(sleeper, 0) for _ in range(w)]:
        t.get(block=True)

    tasks = [(f"low{i}", submit(batch, 0, takes)) for i in range(2 * w)]
    tasks.append(("high", submit(batch, 10, takes)))
    report("case A", tasks, "high", w)

    tasks = [(f"high{i}", submit(batch, 10, takes)) for i in range(2 * w)]
    time.sleep(0.5)
    tasks.append(("low", submit(batch, 0, takes)))
    report("case B", tasks, "low", w)
    batch.join()
    batch.destroy()


if __name__ == "__main__":
    main()

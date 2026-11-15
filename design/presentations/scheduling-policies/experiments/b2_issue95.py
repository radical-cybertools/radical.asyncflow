"""rhapsody#95 at a second site (Bridges-2, fifth campaign): a partial-node job on Dragon.

As ``af3.py`` and ``raw_batch.py`` on rust (design/dragon-rust-findings.md), in one script and
one Dragon runtime, timed from the script's own clock:
1. ``DragonExecutionBackend()`` and its Batch pool (workers, managers);
2. one trivial function task through AsyncFlow, submitted alone;
3. issue #95's 3-task workflow (task2 and task3 depend on task1, task3 on task2);
4. ``flow.shutdown()``, with a ``--shutdown-timeout`` after which the run is recorded as hung.

Run with RHAPSODY 0.6.0 (no pool cap) and with the pool cap (``d2cbc58``), on stock dragonhpc
0.14.1. One result file per run in ``$RESULTS_DIR``.
"""

import argparse
import asyncio
import multiprocessing as mp
import os
import time

import dragon  # noqa: F401
from common import dragon_nodes, log, run_record, write_result
from radical.asyncflow import WorkflowEngine
from rhapsody.backends import DragonExecutionBackend


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="a label: rh-0.6.0 or rh-fix")
    ap.add_argument("--rep", type=int, default=1)
    ap.add_argument("--task-timeout", type=float, default=400.0)
    ap.add_argument("--shutdown-timeout", type=float, default=300.0)
    args = ap.parse_args()
    mp.set_start_method("dragon")
    nodes = dragon_nodes()
    log(f"Dragon nodes (host, num_cpus, allowed cpus, gpus): {nodes}; affinity {len(os.sched_getaffinity(0))}")
    times, status = {}, "ok"

    t = time.time()
    backend = await DragonExecutionBackend()
    times["backend_s"] = time.time() - t
    workers, managers = backend.batch.num_workers, backend.batch.num_managers
    log(f"backend in {times['backend_s']:.1f} s: {workers} workers, {managers} managers")
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def trivial():
        return 1

    @flow.function_task
    async def task1(*args):
        return sum(range(1000))

    @flow.function_task
    async def task2(*args):
        return [x for x in range(1000) if x % 2 == 0]

    @flow.function_task
    async def task3(*args):
        sum_data, even_numbers = args
        return {"total_sum": sum_data, "even_count": len(even_numbers)}

    try:
        t = time.time()
        await asyncio.wait_for(trivial(), args.task_timeout)
        times["trivial_s"] = time.time() - t
        log(f"one trivial task in {times['trivial_s']:.2f} s")
        t = time.time()
        stamps = {}
        t1 = task1()
        t1.add_done_callback(lambda f: stamps.setdefault("task1_s", time.time() - t))
        t2 = task2(t1)
        t2.add_done_callback(lambda f: stamps.setdefault("task2_s", time.time() - t))
        result = await asyncio.wait_for(task3(t1, t2), args.task_timeout)
        times["workflow_s"] = time.time() - t
        times.update(stamps)
        log(f"workflow in {times['workflow_s']:.2f} s: {result}")
    except asyncio.TimeoutError:
        status = "task timeout"
        log(f"timed out after {args.task_timeout:.0f} s: {times}")

    t = time.time()
    try:
        await asyncio.wait_for(flow.shutdown(), args.shutdown_timeout)
        times["shutdown_s"] = time.time() - t
        log(f"shutdown in {times['shutdown_s']:.1f} s")
    except asyncio.TimeoutError:
        status = "shutdown timeout" if status == "ok" else status + ", shutdown timeout"
        times["shutdown_s"] = None
        log(f"shutdown did not finish in {args.shutdown_timeout:.0f} s")
    write_result(run_record("issue95", args.config, args.rep, workers=workers, managers=managers,
                            dragon_nodes=nodes, status=status, **times))


if __name__ == "__main__":
    asyncio.run(main())

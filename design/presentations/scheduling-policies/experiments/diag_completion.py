"""Diagnostic: when does asyncflow see a Dragon task finish, compared with when it ended?

Registers 6 independent sleep tasks of 1..6 s at once, as function tasks and then as
executable tasks, with no policy, and logs for each: its end in the worker or process and
its completion as asyncflow sees it.
"""

import asyncio
import multiprocessing as mp
import time

import dragon  # noqa: F401
from common import log, make_backend
from radical.asyncflow import WorkflowEngine


async def main():
    mp.set_start_method("dragon")
    backend, workers = await make_backend()
    flow = await WorkflowEngine.create(backend=backend)
    log(f"{workers} workers")

    @flow.function_task
    async def fn(d):
        time.sleep(d)
        return time.time()

    @flow.executable_task
    async def ex(d):
        return f"/bin/bash -c 'sleep {d}; date +%s.%N'"

    await asyncio.gather(*[fn(0) for _ in range(workers)])
    for kind, task in (("function", fn), ("executable", ex)):
        t0 = time.time()
        seen = {}
        futs = []
        for d in range(1, 7):
            f = task(d)
            f.add_done_callback(lambda _, d=d: seen.setdefault(d, time.time()))
            futs.append(f)
        res = await asyncio.gather(*futs)
        for d, r in zip(range(1, 7), res):
            end = float(r) if kind == "function" else float(str(r).strip().split()[-1])
            log(f"{kind} sleep {d}: ended +{end - t0:.2f} s, asyncflow saw it +{seen[d] - t0:.2f} s")
    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

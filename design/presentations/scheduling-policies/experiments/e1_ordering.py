"""E1: ordering policies on real Dragon. One workload and one policy per run.

``--workload mixed`` (ROSE-like): ``--bulk`` independent tasks of uniform(``--bulk-min``,
``--bulk-max``) seconds in a ``bulk`` workflow scope, and every ``--period`` seconds a
chain of ``--chain`` tasks of ``--step`` seconds in its own ``interactive:<i>`` scope.
Measured: chain latency (registration of the first step to completion of the last) and bulk
makespan.

``--workload diamonds``: ``--diamonds`` concurrent diamond workflows t1 -> (t2, t3) -> t4 of
``--dur`` seconds per task. Measured: mean workflow completion, first workflow done,
makespan, and workflows in progress at peak (t1 done, t4 not yet: their intermediate
results are live), as in the simulated benchmark.

Tasks sleep for their duration in a Batch worker and return the worker's start and end
times, and its host (third campaign: Batch spreads the tasks over two nodes). Durations are drawn from ``--seed`` + rep, so every policy sees the same tasks.
Policy capacity is the Batch pool size; FairPolicy has no weights, as in the simulation.
``--config`` is a policy name, or a spec such as ``fair@128+prio+a+b`` (see ``common.parse_spec``)
that also sets the capacity, the policy's priorities and the Dragon options.

``--spaced-ms`` > 5 submits each task at least that long after the previous one, so that
Dragon's Batch reports each task when it ends instead of when the batch it was coalesced
into ends (see ``common.make_backend``). Without it, the run is stock RHAPSODY + Dragon.

``--workers-per-node`` (Bridges-2) starts that many Batch workers per node instead of half the
node's CPUs, e.g. rust's 32 on a 128-core RM node.
"""

import argparse
import asyncio
import multiprocessing as mp
import os
import random
import socket
import time

import dragon  # noqa: F401
from common import (
    dragon_nodes,
    log,
    make_backend_for,
    make_policy,
    parse_spec,
    set_workers_per_node,
    pct,
    run_record,
    spec_record,
    summary,
    write_result,
)
from radical.asyncflow import WorkflowEngine


def sleeper(flow):
    @flow.function_task
    async def work(d, *prev):
        t = time.time()
        time.sleep(d)
        return (t, time.time(), socket.gethostname())

    return work


async def mixed(flow, args, rng):
    work = sleeper(flow)
    durations = [rng.uniform(args.bulk_min, args.bulk_max) for _ in range(args.bulk)]
    epoch0, t0 = time.time(), time.perf_counter()

    async def bulk_all():
        async with flow.workflow_scope("bulk"):
            futs = [work(d) for d in durations]
        res = await asyncio.gather(*futs)
        return time.perf_counter(), res

    async def chain(i):
        t = time.perf_counter()
        async with flow.workflow_scope(f"interactive:{i}"):
            steps = [work(args.step)]
            for _ in range(args.chain - 1):
                steps.append(work(args.step, steps[-1]))
        res = await asyncio.gather(*steps)
        return t - t0, time.perf_counter() - t, res

    bulk_task = asyncio.create_task(bulk_all())
    chains = []
    for i in range(args.nchains):
        await asyncio.sleep(t0 + (i + 1) * args.period - time.perf_counter())
        chains.append(asyncio.create_task(chain(i)))
    chains = await asyncio.gather(*chains)
    bulk_end, bulk_res = await bulk_task
    lat = [c[1] for c in chains]
    return {
        "params": {k: getattr(args, k) for k in ("bulk", "bulk_min", "bulk_max", "chain", "step", "period", "nchains")},
        "chain_latency_s": summary(lat),
        "chain_p50_s": pct(lat, 50),
        "chain_p95_s": pct(lat, 95),
        "bulk_makespan_s": bulk_end - t0,
        "chains": [{"start_s": c[0], "latency_s": c[1], "worker_times": [(r[0] - epoch0, r[1] - epoch0) for r in c[2]],
                    "hosts": [r[2] for r in c[2]]} for c in chains],
        "bulk_worker_times": [(r[0] - epoch0, r[1] - epoch0) for r in bulk_res],
        "bulk_hosts": [r[2] for r in bulk_res],
        "bulk_durations": durations,
    }


async def diamonds(flow, args, rng):
    work = sleeper(flow)
    events = []  # (time, +1 when t1 done / -1 when t4 done)
    epoch0, t0 = time.time(), time.perf_counter()

    async def wf(i):
        a = work(args.dur)
        a.add_done_callback(lambda _: events.append((time.perf_counter() - t0, 1)))
        b, c = work(args.dur, a), work(args.dur, a)
        d = work(args.dur, b, c)
        res = await asyncio.gather(a, b, c, d)
        events.append((time.perf_counter() - t0, -1))
        return time.perf_counter() - t0, res

    out = await asyncio.gather(*[wf(i) for i in range(args.diamonds)])
    wct = [o[0] for o in out]
    live = peak = 0
    for _, delta in sorted(events):
        live += delta
        peak = max(peak, live)
    return {
        "params": {"diamonds": args.diamonds, "dur": args.dur},
        "makespan_s": max(wct),
        "mean_wct_s": sum(wct) / len(wct),
        "p95_wct_s": pct(wct, 95),
        "first_s": min(wct),
        "peak_live_wf": peak,
        "wct_s": wct,
        "events": sorted(events),
        "worker_times": [[(r[0] - epoch0, r[1] - epoch0) for r in o[1]] for o in out],
        "hosts": [[r[2] for r in o[1]] for o in out],
    }


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", choices=("mixed", "diamonds"), required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--rep", type=int, required=True)
    ap.add_argument("--seed", type=int, default=1000)
    ap.add_argument("--bulk", type=int, default=1600)
    ap.add_argument("--bulk-min", type=float, default=1.0)
    ap.add_argument("--bulk-max", type=float, default=5.0)
    ap.add_argument("--chain", type=int, default=3)
    ap.add_argument("--step", type=float, default=0.5)
    ap.add_argument("--period", type=float, default=10.0)
    ap.add_argument("--nchains", type=int, default=12)
    ap.add_argument("--diamonds", type=int, default=256)
    ap.add_argument("--dur", type=float, default=2.0)
    ap.add_argument("--spaced-ms", type=float, default=0.0)
    ap.add_argument("--run-timeout", type=float, default=600.0)
    ap.add_argument("--workers-per-node", type=int, default=0)
    args = ap.parse_args()
    mp.set_start_method("dragon")
    host = dragon_nodes()[0][0]
    spec = parse_spec(args.config)
    backend_kwargs = None
    if args.workers_per_node:
        set_workers_per_node(args.workers_per_node)
        backend_kwargs = {"size_pool_to_affinity": False}
    backend, workers, capacity = await make_backend_for(spec, args.spaced_ms, backend_kwargs)
    policy = make_policy(spec["policy"], capacity, set_priority="prio" in spec["flags"])
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    configured = spec_record(spec, workers, capacity, backend)
    log(f"{args.workload}/{args.config} rep {args.rep}: {workers} workers on {host}, capacity {capacity}")

    work = sleeper(flow)
    t = time.perf_counter()
    await asyncio.gather(*[work(0.0) for _ in range(workers)])
    warmup = time.perf_counter() - t

    rng = random.Random(args.seed + args.rep)
    variant = f"spaced{args.spaced_ms:g}ms" if args.spaced_ms else "stock"
    try:
        res = await asyncio.wait_for((mixed if args.workload == "mixed" else diamonds)(flow, args, rng),
                                     timeout=args.run_timeout)
    except Exception as e:  # noqa: BLE001 - record the failure; a broken Batch can hang exit
        log(f"FAILED {e!r}")
        write_result(run_record(f"e1-{args.workload}-{variant}", args.config, args.rep, submission=variant,
                                failed=True, error=repr(e)[:1000], **configured))
        os._exit(3)
    t = time.perf_counter()
    await flow.shutdown()
    rec = run_record(f"e1-{args.workload}-{variant}", args.config, args.rep, workers=workers, dragon_host=host,
                     submission=variant, **configured,
                     warmup_s=warmup, shutdown_s=time.perf_counter() - t, **res)
    keys = ("chain_p50_s", "chain_p95_s", "bulk_makespan_s", "mean_wct_s", "first_s", "makespan_s", "peak_live_wf")
    log(", ".join(f"{k}={rec[k]:.3f}" for k in keys if k in rec))
    write_result(rec)


if __name__ == "__main__":
    asyncio.run(main())

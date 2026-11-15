"""E3: what the policies cost on real Dragon.

For each configuration in ``--configs`` (in a shuffled order), with a fresh
DragonExecutionBackend and WorkflowEngine inside the same Dragon runtime:

- warm-up: one trivial task per Batch worker, so no worker is cold;
- latency: a chain of ``--chain`` trivial function tasks, each taking the previous one's
  result, registered at once; the per-task latency is the time between successive
  completions (dependency release, policy, RHAPSODY, Dragon, completion delivery);
- throughput: ``--tasks`` independent trivial function tasks registered at once; tasks per
  second from the first registration to the last completion.

Each task returns its worker's host (third campaign, two nodes): the result records how many
tasks ran on each node, and for the chain which steps changed node.

Policies with a capacity get the Batch pool size. ResourcePolicy gets one node of
``pool size`` CPUs and every task asks for one CPU, so it also keeps at most pool-size tasks
in flight; in node mode it writes ``task["placement"]``, which RHAPSODY honours only on the
placement tree (rh-merged), where a placed function runs as its own Dragon process.

Each entry of ``--configs`` is a policy name or a spec such as ``fair@128+prio+a+b`` (see
``common.parse_spec``), which also sets the capacity, the policy's priorities and the Dragon
options of that configuration's backend.

One result file per configuration. A configuration that fails or exceeds
``--config-timeout`` gets a result file with ``failed`` and the error, and the run stops
there: after a failure Dragon's Batch may not shut down, so the script exits at once.
"""

import argparse
import asyncio
import multiprocessing as mp
import os
import random
import socket
import time
from collections import Counter

import dragon  # noqa: F401
from common import (
    dragon_nodes,
    log,
    make_backend_for,
    make_policy,
    parse_spec,
    run_record,
    spec_record,
    summary,
    write_result,
)
from radical.asyncflow import WorkflowEngine


async def run_config(name, args):
    host = dragon_nodes()[0][0]
    spec = parse_spec(name)
    backend, workers, capacity = await make_backend_for(spec)
    policy = make_policy(spec["policy"], capacity, host=host, set_priority="prio" in spec["flags"])
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    configured = spec_record(spec, workers, capacity, backend)
    log(f"{name}: {workers} Batch workers, policy {type(policy).__name__}, capacity {capacity}")

    @flow.function_task
    async def trivial(*prev):
        return socket.gethostname()

    t = time.perf_counter()
    await asyncio.gather(*[trivial() for _ in range(workers)])
    warmup = time.perf_counter() - t

    done = []
    t0 = time.perf_counter()
    x = trivial()
    x.add_done_callback(lambda _: done.append(time.perf_counter()))
    chain = [x]
    for _ in range(args.chain - 1):
        x = trivial(x)
        x.add_done_callback(lambda _: done.append(time.perf_counter()))
        chain.append(x)
    await x
    chain_s = time.perf_counter() - t0
    chain_hosts = [f.result() for f in chain]
    steps = [b - a for a, b in zip([t0] + done[:-1], done)]

    t0 = time.perf_counter()
    tput_hosts = await asyncio.gather(*[trivial() for _ in range(args.tasks)])
    tput_s = time.perf_counter() - t0

    t = time.perf_counter()
    await flow.shutdown()
    shutdown = time.perf_counter() - t
    rec = run_record(
        "e3",
        name,
        args.rep,
        workers=workers,
        dragon_host=host,
        **configured,
        params={"chain": args.chain, "tasks": args.tasks},
        warmup_s=warmup,
        chain_s=chain_s,
        per_task_latency_ms=chain_s / args.chain * 1e3,
        step_ms=summary([s * 1e3 for s in steps]),
        steps_ms=[s * 1e3 for s in steps],
        throughput_s=tput_s,
        tasks_per_s=args.tasks / tput_s,
        shutdown_s=shutdown,
        chain_hosts=chain_hosts,
        throughput_hosts=dict(Counter(tput_hosts)),
    )
    log(
        f"{name}: chain {rec['per_task_latency_ms']:.1f} ms/task, "
        f"{rec['tasks_per_s']:.0f} tasks/s, shutdown {shutdown:.1f} s"
    )
    write_result(rec)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--configs", required=True, help="comma-separated: none,fifo,...")
    ap.add_argument("--rep", type=int, required=True)
    ap.add_argument("--chain", type=int, default=100)
    ap.add_argument("--tasks", type=int, default=2000)
    ap.add_argument("--config-timeout", type=float, default=600.0)
    args = ap.parse_args()
    mp.set_start_method("dragon")
    configs = args.configs.split(",")
    random.Random(args.rep).shuffle(configs)
    log(f"nodes {dragon_nodes()}; order {configs}")
    for name in configs:
        try:
            await asyncio.wait_for(run_config(name, args), timeout=args.config_timeout)
        except Exception as e:  # noqa: BLE001 - record the failure; a broken Batch can hang exit
            log(f"{name}: FAILED {e!r}")
            write_result(run_record("e3", name, args.rep, failed=True, error=repr(e)[:1000], spec=parse_spec(name)))
            os._exit(3)


if __name__ == "__main__":
    asyncio.run(main())

"""02-policy_timeline.py — what a ready-queue policy decided, joined per task.

A small AI-steered ensemble, in the shape of the DeepDriveSim MiniApp: each replica
simulates twice in a row, one AI step trains a model on the first round of every
replica, and each replica then simulates again with the model. The AI step declares a
higher priority, so it starts before the second rounds that wait with it. The policy
is a ``FifoPolicy`` with 4 slots. Where ``FairPolicy`` and ``ResourcePolicy`` are
installed, ``--policy fair`` shares the 4 slots across the replicas and the AI step, and
``--policy resource`` admits tasks onto one 4-core node by their declared cores.

With telemetry on, the engine traces the policy. After the run, the events are read
back from the checkpoint file and joined by uid into one row per task (ready,
dispatched, started, ended, the time held in the policy, the node and the charge),
and into the policy's core occupancy over time.

Usage
-----
    python 02-policy_timeline.py
    python 02-policy_timeline.py --replicas 6 --out results/
    python 02-policy_timeline.py --policy resource   # where ResourcePolicy exists
    python 02-policy_timeline.py --observe   # RHAPSODY with Concurrent observe_peaks

``--observe`` asks the Concurrent backend for each task's worker-side start and end
times and peak memory (``observe_peaks=True``, not in RHAPSODY 0.7.0); the timeline
then shows them as ``exec_start``/``exec_end``.
"""

import argparse
import asyncio
import importlib
import socket
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from rhapsody.backends import ConcurrentExecutionBackend

from radical.asyncflow import FifoPolicy, WorkflowEngine
from radical.asyncflow.policy_telemetry import load_events, occupancy, timeline

CORES = 4


async def simulate(seconds, *_):
    time.sleep(seconds)  # busy in its worker process, like a real step
    return seconds


async def train(seconds, *_):
    time.sleep(seconds)  # busy in its worker process, like a real step
    return "model"


# FairPolicy and ResourcePolicy come with pull requests of their own
_POLICIES = {"fifo": "FifoPolicy", "fair": "FairPolicy", "resource": "ResourcePolicy"}


def available_policies() -> list:
    installed = importlib.import_module("radical.asyncflow.policies")
    return [name for name, cls in _POLICIES.items() if hasattr(installed, cls)]


def make_policy(name: str):
    policies = importlib.import_module("radical.asyncflow.policies")
    if name == "fifo":
        return FifoPolicy(CORES)
    if name == "fair":
        return policies.FairPolicy(CORES)
    node = policies.NodeShape(cpus=CORES, name=socket.gethostname())
    return policies.ResourcePolicy(policies.Pool([node], mode="node"))


async def run(replicas: int, out: Path, observe: bool, policy: str) -> Path:
    before = set(out.glob("*.jsonl"))  # a reused --out keeps earlier runs' files
    kwargs = {"observe_peaks": True} if observe else {}
    backend = await ConcurrentExecutionBackend(
        ProcessPoolExecutor(max_workers=CORES), **kwargs
    )
    flow = await WorkflowEngine.create(backend=backend, policy=make_policy(policy))
    telemetry = await flow.start_telemetry(checkpoint_path=str(out))

    sim = flow.function_task(simulate)
    trainer = flow.function_task(train)
    one_core = {"resources": {"cpus": 1}}
    ai_first = {"resources": {"cpus": 2}, "priority": 10}

    firsts, seconds, steered = [], [], []
    for i in range(replicas):
        async with flow.workflow_scope(f"replica-{i}"):
            firsts.append(sim(0.4, task_description=one_core))
            seconds.append(sim(0.4, firsts[i], task_description=one_core))
    async with flow.workflow_scope("ai"):
        model = trainer(0.8, *firsts, task_description=ai_first)
    for i in range(replicas):
        async with flow.workflow_scope(f"replica-{i}"):
            steered.append(sim(0.4, seconds[i], model, task_description=one_core))

    await asyncio.gather(*steered)
    await telemetry.stop()
    await flow.shutdown()
    (path,) = set(out.glob("*.jsonl")) - before
    return path


def report(path: Path) -> None:
    events = load_events(str(path))
    rows = timeline(events)
    t0 = min(r["created"] for r in rows if "created" in r)

    def rel(row, column):
        value = row.get(column)
        return f"{value - t0:6.2f}" if value is not None else "     -"

    print(f"events: {len(events)} in {path}\n")
    head = (
        f"{'uid':<12} {'workflow':<10} {'name':<9} {'ready':>6} {'disp':>6} "
        f"{'start':>6} {'x_beg':>6} {'x_end':>6} {'ended':>6} {'held':>5} "
        f"{'cores':>5} {'prio':>4} {'state':<5}"
    )
    print(head)
    print("-" * len(head))
    for r in rows:
        if r.get("dispatched") is None:
            continue  # blocks and services do not go through the policy
        cores = (r.get("charge") or {}).get("cpus")
        cores = "-" if cores is None else cores
        prio = (r.get("key") or {}).get("priority", "")
        print(
            f"{r['uid']:<12} {r.get('workflow_id', ''):<10} "
            f"{r.get('executable', ''):<9} {rel(r, 'ready')} {rel(r, 'dispatched')} "
            f"{rel(r, 'started')} {rel(r, 'exec_start')} {rel(r, 'exec_end')} "
            f"{rel(r, 'ended')} {r.get('held_seconds', 0):5.2f} "
            f"{cores:>5} {prio:>4} {r.get('state', ''):<5}"
        )

    print("\noccupancy (step function; cores with ResourcePolicy):")
    for p in occupancy(events):
        used = p.get("pool.cpus", p["inflight"])
        bar = "#" * int(used or 0)
        print(
            f"  t={p['t'] - t0:6.2f}  used {used:>4}/{CORES} {bar:<{CORES}}  "
            f"inflight {p['inflight']}  waiting {p['waiting']}"
        )

    signals = [e for e in events if e["event_type"].startswith("asyncflow.Policy")]
    signals += [e for e in events if e["event_type"] == "asyncflow.TaskStarving"]
    print("\npolicy signals:")
    for e in signals:
        detail = e.get("layout") or e.get("reason") or e.get("blocked_seconds")
        print(f"  {e['event_type']:<26} {e.get('task_id') or '':<12} {detail}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--replicas", type=int, default=6)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--observe", action="store_true")
    parser.add_argument("--policy", choices=available_policies(), default="fifo")
    args = parser.parse_args()
    out = args.out or Path(tempfile.mkdtemp(prefix="policy-timeline-"))
    out.mkdir(parents=True, exist_ok=True)
    report(asyncio.run(run(args.replicas, out, args.observe, args.policy)))


if __name__ == "__main__":
    main()

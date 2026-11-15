"""Helpers shared by the rust policy experiments (E1-E3): backend, policies, run records.

Every experiment script runs under ``dragon`` (see ``run.sbatch``), builds a fresh
``DragonExecutionBackend`` and ``WorkflowEngine`` per configuration, and writes one JSON
file per run into ``$RESULTS_DIR``. ``run.sbatch`` later merges the node state it sampled
(load, co-running jobs) into the same file.
"""

import asyncio
import json
import os
import socket
import statistics
import sys
import time

T0 = time.monotonic()


def log(msg):
    print(f"[t+{time.monotonic() - T0:7.1f}s] {msg}", flush=True)


def pct(xs, p):
    """Nearest-rank percentile, as in the simulated benchmark."""
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def summary(xs):
    if not xs:
        return {}
    return {
        "n": len(xs),
        "mean": statistics.mean(xs),
        "p50": pct(xs, 50),
        "p95": pct(xs, 95),
        "min": min(xs),
        "max": max(xs),
    }


def dragon_nodes():
    """``[(hostname, num_cpus, len(cpus), gpus)]`` in Dragon's node order."""
    from dragon.native.machine import Node, System

    return [
        (n.hostname, n.num_cpus, len(n.cpus), list(n.gpus or []))
        for n in (Node(h) for h in System().nodes)
    ]


async def make_backend(spaced_ms=0.0, compile_each_task=False, batch_kwargs=None, backend_kwargs=None):
    """A DragonExecutionBackend and its Batch pool size (all nodes).

    Dragon 0.14.1's Batch coalesces submissions that arrive within 5 ms of each other (up
    to 256) into one compiled batch, and ``poll()`` reports the tasks of a compiled batch
    only when all of them are done (see ``diag_batch.py``). With ``spaced_ms`` > 5, every
    task is submitted at least that long after the previous one, so each task is its own
    compiled batch and is reported when it ends. Dragon itself is unchanged.

    ``compile_each_task`` (RHAPSODY ``experiment/dragon-priority``) sets Batch's client grouping
    to one task per compile, with no Dragon change. ``batch_kwargs`` go to ``Batch()``; the
    options of Dragon ``experiment/batch-reports-priority`` travel this way. Neither is passed
    when unset, so the first campaign's trees work unchanged. ``backend_kwargs`` (Bridges-2)
    go to the backend itself, e.g. ``observe_peaks`` or ``size_pool_to_affinity``.
    """
    from rhapsody.backends import DragonExecutionBackend

    cls = DragonExecutionBackend
    if spaced_ms:
        gap = spaced_ms / 1e3

        class SpacedDragonBackend(DragonExecutionBackend):
            _last = 0.0

            async def submit_tasks(self, tasks):
                for task in tasks:
                    wait = self._last + gap - time.monotonic()
                    if wait > 0:
                        await asyncio.sleep(wait)
                    await DragonExecutionBackend.submit_tasks(self, [task])
                    self._last = time.monotonic()

        cls = SpacedDragonBackend
    kwargs = {}
    if compile_each_task:
        kwargs["compile_each_task"] = True
    if batch_kwargs:
        kwargs["batch_kwargs"] = dict(batch_kwargs)
    kwargs.update(backend_kwargs or {})
    backend = await cls(**kwargs)
    return backend, total_workers(backend.batch)


def set_workers_per_node(n):
    """Make ``Batch()`` start ``n`` workers per node (Bridges-2): Batch starts
    ``Node.num_cpus // 2`` per node, so ``Node.num_cpus`` reports ``2 * n`` from here on. Pass
    the backend ``size_pool_to_affinity=False`` with it, or RHAPSODY's pool cap undoes it."""
    from dragon.native.machine import Node

    Node.num_cpus = property(lambda _node: 2 * n)


def total_workers(batch):
    """Workers in all of Batch's pools. ``Batch.num_workers`` is one node's pool
    (``workers_per_node_in_pool``); Batch starts one pool per node (third campaign: two nodes)."""
    return batch.num_workers * max(1, len(getattr(batch, "_node_objs", None) or [None]))


# Configuration specs (second campaign): "<policy>[@<capacity>][+flag...]", e.g. "fair@128+prio+a+b".
# capacity: "pool" (default: the Batch pool size), an int, or "inf".
# flags: prio  the policy writes desc["priority"] (asyncflow experiment/policy-priority)
#        each  the no-change patch: Batch compiles every task on its own (RHAPSODY option)
#        a     Batch(client_linger_sec=0, client_max_batch_size=1)   (Dragon branch)
#        b     Batch(report_each_task=True)                           (Dragon branch)
#        d     Batch(cap_launch_budget=True)                          (Dragon branch)
#        e     Batch(client_wake_on_completion=True)    (Dragon branch 0a7d87d, fourth campaign)
# A bare policy name ("fair") is what the first campaign ran.
SPEC_FLAGS = ("prio", "each", "a", "b", "d", "e")
UNBOUNDED = 10**9


def parse_spec(spec):
    head, *flags = spec.split("+")
    policy, _, capacity = head.partition("@")
    unknown = set(flags) - set(SPEC_FLAGS)
    if unknown:
        raise ValueError(f"unknown flags {sorted(unknown)} in {spec!r}")
    return {"spec": spec, "policy": policy, "capacity": capacity or "pool", "flags": sorted(flags)}


def resolve_capacity(capacity, workers):
    if capacity == "pool":
        return workers
    if capacity == "inf":
        return UNBOUNDED
    return int(capacity)


def spec_batch_kwargs(spec):
    flags, kw = spec["flags"], {}
    if "a" in flags:
        kw.update(client_linger_sec=0.0, client_max_batch_size=1)
    if "b" in flags:
        kw["report_each_task"] = True
    if "d" in flags:
        kw["cap_launch_budget"] = True
    if "e" in flags:
        kw["client_wake_on_completion"] = True
    return kw


async def make_backend_for(spec, spaced_ms=0.0, backend_kwargs=None):
    """Backend, pool size and resolved capacity for a parsed spec."""
    backend, workers = await make_backend(spaced_ms, "each" in spec["flags"], spec_batch_kwargs(spec),
                                          backend_kwargs)
    return backend, workers, resolve_capacity(spec["capacity"], workers)


def spec_record(spec, workers, capacity, backend):
    """What a result file records about its configuration."""
    batch = backend.batch
    return {
        "spec": spec,
        "capacity_resolved": capacity,
        "batch_kwargs": spec_batch_kwargs(spec),
        "launch_budget_per_node": getattr(batch, "_workers_per_node_in_pool", None),
    }


def make_policy(name, capacity, host=None, gpus=0, gpu_mem=None, set_priority=False, shapes=None,
                memory=None, unknown_per_node=None):
    """The policies compared. ``capacity`` is the Batch pool size, so a policy keeps
    exactly as many tasks in flight as Dragon has workers.

    ``shapes`` (third campaign, two nodes): ``[(host, cpus, gpus, gpu_mem)]``, one NodeShape
    per Dragon node for ResourcePolicy, in place of the single node ``host``/``gpus``/``gpu_mem``.
    ``memory`` (Bridges-2): a MemoryModel, so ResourcePolicy learns memory from observed peaks;
    ``unknown_per_node``: the Pool's limit of tasks with unknown memory per node.
    """
    if name == "none":
        return None
    if name in ("fifo", "lineage", "fair"):
        from radical.asyncflow.policies import FairPolicy, FifoPolicy, LineagePolicy

        cls = {"fifo": FifoPolicy, "lineage": LineagePolicy, "fair": FairPolicy}[name]
        return cls(capacity, set_priority=True) if set_priority else cls(capacity)
    if name in ("res-agg", "res-node"):
        from radical.asyncflow.resources import NodeShape, Pool, ResourcePolicy

        if shapes:
            nodes = [NodeShape(cpus=c, gpus=g, gpu_mem=m, name=h) for h, c, g, m in shapes]
        else:
            nodes = [NodeShape(cpus=capacity, gpus=gpus, gpu_mem=gpu_mem, name=host)]
        mode = "aggregate" if name == "res-agg" else "node"
        pool_kw = {} if unknown_per_node is None else {"unknown_per_node": unknown_per_node}
        if memory is not None:
            return ResourcePolicy(Pool(nodes, mode=mode, **pool_kw), memory=memory)
        return ResourcePolicy(Pool(nodes, mode=mode, **pool_kw))
    raise ValueError(name)


def tree_versions():
    import radical.asyncflow as af
    import rhapsody

    return {
        "asyncflow_commit": os.environ.get("ASYNCFLOW_COMMIT"),
        "asyncflow_path": os.path.dirname(af.__file__),
        "rhapsody_commit": os.environ.get("RHAPSODY_COMMIT"),
        "rhapsody_path": os.path.dirname(rhapsody.__file__),
        "dragon": os.environ.get("DRAGON_VERSION"),
        "python": sys.version.split()[0],
    }


def run_record(experiment, config, rep, **extra):
    return {
        "experiment": experiment,
        "config": config,
        "rep": rep,
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "slurm_partition": os.environ.get("SLURM_JOB_PARTITION"),
        "slurm_nodelist": os.environ.get("SLURM_JOB_NODELIST"),
        "slurm_cpus_on_node": os.environ.get("SLURM_CPUS_ON_NODE"),
        "slurm_gpus": os.environ.get("SLURM_JOB_GPUS") or os.environ.get("SLURM_GPUS_ON_NODE"),
        "client_host": socket.gethostname(),
        "client_affinity": len(os.sched_getaffinity(0)),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "loadavg_start": open("/proc/loadavg").read().split()[:3],
        **tree_versions(),
        **extra,
    }


def write_result(rec):
    out_dir = os.environ.get("RESULTS_DIR", ".")
    os.makedirs(out_dir, exist_ok=True)
    config = rec["config"].replace("@", "-c").replace("+", "-")
    name = f"{rec['experiment']}_{config}_r{rec['rep']}_{rec['slurm_job_id']}.json"
    rec["loadavg_end"] = open("/proc/loadavg").read().split()[:3]
    rec["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    path = os.path.join(out_dir, name)
    with open(path, "w") as f:
        json.dump(rec, f, indent=1)
    log(f"wrote {path}")
    return path

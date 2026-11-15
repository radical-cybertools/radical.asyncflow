"""E2: packing GPUs by memory, with synthetic CUDA tasks on real Dragon.

``--pipelines`` IMPRESS-like pipelines run for ``--window`` seconds. Each one loops over
its GPU tools, one task at a time, as IMPRESS's adaptive chain does:
rfd3 -> mpnn (again with p = 0.70) -> boltz (p = 0.32) or rfd3; boltz -> rfd3.
The CPU-only steps (packmin, fastrelax, filter_shape) are left out.

Every task is ``gpu_task.py``, run as an executable task (a Dragon process). It holds its
GPU memory for its whole run and computes on the GPU for a quarter of it. Durations,
memory and declarations come from the S1 IMPRESS model (``impress_model.py`` on
``prototype/policy-s1``): durations halved, GB converted to GiB, each task's true footprint
(CUDA context included) drawn from the model and never above what it declares.

Modes (``--config``):
- ``none``: no policy. Pipeline p pins GPU p mod 2 through CUDA_VISIBLE_DEVICES, as
  IMPRESS pins pipeline N to GPU (N-1) mod 4.
- ``res-agg``: ResourcePolicy, aggregate mode: a task starts when its declared share and
  memory fit the free totals of both GPUs. The pipeline still pins the GPU.
- ``res-node``: ResourcePolicy, node mode: the policy picks the GPU, and RHAPSODY's
  placement branch runs the task pinned to it (Dragon sets CUDA_VISIBLE_DEVICES).

``--spaced-ms`` > 5 submits each task at least that long after the previous one, so Dragon's
Batch reports each task when it ends instead of when its coalesced batch ends (see
``common.make_backend``); without it, the run is stock RHAPSODY + Dragon.

Two nodes (third campaign), one GPU each, of different memory (haskell's RTX 6000 Ada,
fortran's RTX PRO 6000 Blackwell), when Dragon spans both:
- ``none`` and ``res-agg`` pin nothing: Dragon's Batch chooses the node, and the task uses
  that node's GPU. ``res-agg`` admits against the summed GPU slices and memory.
- ``res-node`` gets one NodeShape per node, with its own GPU memory (``--gpu-mem``).
- ``--mem-scale`` multiplies every task's true and declared GPU memory, so that one node's
  GPU memory binds before the totals do.
- ``--iters-per-ms-by-gpu`` gives ``gpu_task.py`` the kernel rate of each GPU model, so a task
  computes for the same time on either.
- ``--config`` takes the second campaign's flags, e.g. ``res-node+a+b`` (see
  ``common.parse_spec``).

Bridges-2 (fifth campaign): 8 nodes of 8 V100s, one Dragon runtime. Options, all off by default:
- ``--pin-mod N``: in ``none`` and ``res-agg`` over several nodes, pipeline p pins GPU p mod N
  on whatever node Dragon picks (IMPRESS's pinning, with N GPUs per node).
- ``res-learn``: node mode with a ``MemoryModel`` and no declared memory. The backend reports
  each task's peaks (``observe_peaks``, RHAPSODY's launcher with NVML), and the policy learns
  each tool's GPU memory from them; ``--unknown-per-node`` tasks of an unlearned tool per node.
- ``--kind-per-tool``: one AsyncFlow task function per tool (``rfd3``, ``mpnn``, ``boltz``)
  instead of one ``gpu``, so ResourcePolicy queues and learns them as separate kinds.
- ``--workers-per-node``: Batch workers per node (Batch starts cores // 2: 20 on a V100 node,
  fewer than its 64 pipelines). RHAPSODY's pool cap is turned off with it.
- ``--node-gpu-mem``, ``--ctx-gib``, ``--iters-per-ms``: the V100's usable memory, CUDA context
  and kernel rate, measured in the smoke test.
- ``--shapes-from-dragon``: one NodeShape per Dragon node even on one node, with its GPU count;
  ``--expect-gpus N`` stops the run unless every Dragon node lists N GPUs.

A CUDA out-of-memory error fails the task and ends its pipeline, as in IMPRESS. Each task
writes its own record (GPU bus id, start, end, status) to ``$RUN_DIR/tasks``; this script
collects them into the run's result file.
"""

import argparse
import asyncio
import faulthandler
import glob
import json
import math
import multiprocessing as mp
import os
import random
import signal
import sys
import time
import traceback

import dragon  # noqa: F401
from common import (
    dragon_nodes,
    log,
    make_backend,
    make_policy,
    parse_spec,
    set_workers_per_node,
    run_record,
    spec_batch_kwargs,
    write_result,
)
from radical.asyncflow import WorkflowEngine

GB = 1e9 / 2**30  # GiB per GB
CTX_GIB = 0.42  # one CUDA context and the kernel on an RTX 6000 Ada, measured (cuMemGetInfo)
GPU_MEM_GIB = 47.0  # per GPU, under the 47.37 GiB the driver reports
GPU_SHARE = 0.125  # the S1 model's share per GPU tool: at most 8 tasks per GPU
ITERS_PER_MS = 79000  # gpu_task.py kernel rate on an idle RTX 6000 Ada, measured
DURATION_SCALE = 0.5
DURATION_SIGMA = 0.25
# name: median s (k = 1), GPU fraction, true peak GB (mean, sd), declared GB
TOOLS = {
    "rfd3": (74.3, 0.25, (5.0, 0.4), 6.0),
    "mpnn": (11.1, 0.25, (1.5, 0.2), 2.0),
    "boltz": (47.8, 0.25, (4.0, 0.8), 6.0),
}
P_MPNN_AGAIN = 1 - 117 / 394
P_BOLTZ = 37 / 117  # boltz per rfd3 in the model's chain (the CPU gates multiplied)
NGPUS = 2


def next_tool(tool, rng):
    if tool == "rfd3":
        return "mpnn"
    if tool == "mpnn":
        if rng.random() < P_MPNN_AGAIN:
            return "mpnn"
        return "boltz" if rng.random() < P_BOLTZ else "rfd3"
    return "rfd3"


def sample(tool, rng, mem_scale=1.0):
    median, frac, (mean, sd), declared = TOOLS[tool]
    secs = median * DURATION_SCALE * math.exp(rng.gauss(0, DURATION_SIGMA))
    true = min(max(rng.gauss(mean, sd), 0.5), declared) * GB * mem_scale
    return secs, frac, true, declared * GB * mem_scale


def parse_by_host(text):
    """``"haskell=47.0,fortran=94.5"`` -> ``{"haskell": 47.0, "fortran": 94.5}``."""
    return {h.strip(): float(v) for h, v in (item.split("=") for item in filter(None, text.split(",")))}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="none, res-agg or res-node, with optional +flags")
    ap.add_argument("--rep", type=int, required=True)
    ap.add_argument("--pipelines", type=int, default=24)
    ap.add_argument("--window", type=float, default=720.0)
    ap.add_argument("--seed", type=int, default=2000)
    ap.add_argument("--spaced-ms", type=float, default=0.0)
    ap.add_argument("--mem-scale", type=float, default=1.0)
    ap.add_argument("--gpu-mem", default="", help="per host GiB, e.g. haskell=47.0,fortran=94.5")
    ap.add_argument("--iters-per-ms-by-gpu", default="")
    ap.add_argument("--pin-mod", type=int, default=0)
    ap.add_argument("--kind-per-tool", action="store_true")
    ap.add_argument("--workers-per-node", type=int, default=0)
    ap.add_argument("--node-gpu-mem", type=float, default=GPU_MEM_GIB)
    ap.add_argument("--ctx-gib", type=float, default=CTX_GIB)
    ap.add_argument("--iters-per-ms", type=int, default=ITERS_PER_MS)
    ap.add_argument("--shapes-from-dragon", action="store_true")
    ap.add_argument("--expect-gpus", type=int, default=0)
    ap.add_argument("--unknown-per-node", type=int, default=None)
    args = ap.parse_args()
    spec = parse_spec(args.config)
    mode = spec["policy"]
    learn = mode == "res-learn"
    if mode not in ("none", "res-agg", "res-node", "res-learn"):
        raise ValueError(f"unknown E2 mode {mode!r}")
    faulthandler.register(signal.SIGUSR1, all_threads=True)  # stack dump of a stalled run
    mp.set_start_method("dragon")
    nodes = dragon_nodes()
    host = nodes[0][0]
    multi = len(nodes) > 1
    if args.expect_gpus and any(len(g) != args.expect_gpus for *_, g in nodes):
        raise RuntimeError(f"expected {args.expect_gpus} GPUs on every Dragon node: {nodes}")
    backend_kwargs = {}
    if args.workers_per_node:
        set_workers_per_node(args.workers_per_node)
        backend_kwargs["size_pool_to_affinity"] = False
    if learn:
        backend_kwargs.update(observe_peaks=True, peak_source="launcher")
    backend, workers = await make_backend(args.spaced_ms, "each" in spec["flags"], spec_batch_kwargs(spec),
                                          backend_kwargs)
    gpu_mem = parse_by_host(args.gpu_mem)
    shapes = None
    ngpus = NGPUS
    if multi or args.shapes_from_dragon:
        ngpus = sum(len(g) for *_, g in nodes)
        shapes = [(h, workers // len(nodes), len(g), gpu_mem.get(h, args.node_gpu_mem)) for h, _, _, g in nodes]
    memory = None
    if learn:
        from radical.asyncflow.resources import MemoryModel

        memory = MemoryModel()
    policy = make_policy("res-node" if learn else mode, workers, host=host, gpus=NGPUS, gpu_mem=GPU_MEM_GIB,
                         shapes=shapes, memory=memory, unknown_per_node=args.unknown_per_node)
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    log(f"e2/{args.config} rep {args.rep}: {workers} workers, Dragon nodes {nodes}, shapes {shapes}")
    if workers < args.pipelines:
        log(f"WARNING: {workers} Batch workers < {args.pipelines} pipelines: the pool caps concurrency")

    task_dir = os.path.join(os.environ.get("RUN_DIR", "."), "tasks")
    os.makedirs(task_dir, exist_ok=True)
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gpu_task.py")

    @flow.executable_task
    async def gpu(cmd):
        return cmd

    def tool_task(name):
        async def run(cmd):
            return cmd

        run.__name__ = run.__qualname__ = name
        return flow.executable_task(run)

    by_tool = {t: tool_task(t) for t in TOOLS} if args.kind_per_tool else {}

    client = []  # per task: tag, pipeline, tool, gpu pinned by the app, registered, finished, ok
    lost = []
    epoch0 = time.time()
    t_stop = epoch0 + args.window

    async def pipeline(p):
        rng = random.Random(args.seed * 1000 + args.rep * 100 + p)
        tool, k = "rfd3", 0
        while time.time() < t_stop:
            secs, frac, true, declared = sample(tool, rng, args.mem_scale)
            tag = f"p{p:02d}-{k:03d}-{tool}"
            cmd = (
                f"{sys.executable} {script} --mem-gib {max(0.1, true - args.ctx_gib):.3f} --secs {secs:.2f}"
                f" --gpu-frac {frac} --iters-per-ms {args.iters_per_ms} --tag {tag}"
                f" --out {os.path.join(task_dir, tag + '.json')}"
            )
            if args.iters_per_ms_by_gpu:
                cmd += f" --iters-per-ms-by-gpu {args.iters_per_ms_by_gpu}"
            pinned = None
            if mode in ("none", "res-agg") and not multi and not args.shapes_from_dragon:
                pinned = p % NGPUS
                cmd = f"env CUDA_VISIBLE_DEVICES={pinned} {cmd}"
            elif mode in ("none", "res-agg") and args.pin_mod:
                pinned = p % args.pin_mod
                cmd = f"env CUDA_VISIBLE_DEVICES={pinned} {cmd}"
            desc = {"resources": {"cpus": 1, "gpus": GPU_SHARE, "gpu_mem": round(declared, 3)}}
            if learn:
                del desc["resources"]["gpu_mem"]
            row = {"tag": tag, "pipeline": p, "tool": tool, "pinned": pinned, "true_gib": true,
                   "declared_gib": declared, "secs": secs, "t_reg": time.time() - epoch0}
            client.append(row)
            fut = by_tool.get(tool, gpu)(cmd, task_description=desc)
            try:
                await fut
                row["ok"] = True
            except Exception as e:  # noqa: BLE001 - any failure ends the pipeline
                row["ok"] = False
                row["error"] = repr(e)[:300]
                row["traceback"] = "".join(traceback.format_exception(e))[-3000:]
                if getattr(e, "__cause__", None) is not None or getattr(e, "__context__", None) is not None:
                    row["cause"] = repr(e.__cause__ or e.__context__)[:500]
                lost.append(p)
                return
            finally:
                row["t_done"] = time.time() - epoch0
            tool, k = next_tool(tool, rng), k + 1

    await asyncio.gather(*[pipeline(p) for p in range(args.pipelines)])
    drained = time.time() - epoch0
    await flow.shutdown()

    records = []
    for path in sorted(glob.glob(os.path.join(task_dir, "*.json"))):
        r = json.load(open(path))
        for key in ("t_start", "t_ctx", "t_alloc", "t_end"):
            if key in r:
                r[key] -= epoch0
        records.append(r)
    done = [r for r in records if r["status"] == "done" and r["t_end"] <= args.window]
    ooms = [r for r in records if r["status"] == "oom"]
    hours = args.window / 3600
    base = f"e2-{len(nodes)}n" if multi else "e2"
    learned = None
    if memory is not None:
        learned = {f"{kind}|{dim}": {"count": n, "max": peak, "estimate": memory.estimate(kind, dim)}
                   for (kind, dim), (n, peak) in memory._obs.items()}
    rec = run_record(
        f"{base}-spaced{args.spaced_ms:g}ms" if args.spaced_ms else base,
        args.config,
        args.rep,
        submission=(f"spaced{args.spaced_ms:g}ms" if args.spaced_ms
                    else "+".join(spec["flags"]) if spec["flags"] else "stock"),
        workers=workers,
        dragon_nodes=nodes,
        params={
            "pipelines": args.pipelines, "window_s": args.window, "seed": args.seed,
            "ctx_gib": args.ctx_gib, "gpu_mem_gib": args.node_gpu_mem, "gpu_share": GPU_SHARE,
            "iters_per_ms": args.iters_per_ms, "pin_mod": args.pin_mod, "kind_per_tool": args.kind_per_tool,
            "workers_per_node": args.workers_per_node, "unknown_per_node": args.unknown_per_node, "duration_scale": DURATION_SCALE, "tools": TOOLS,
            "mem_scale": args.mem_scale, "gpu_mem_by_host": gpu_mem, "shapes": shapes,
            "iters_per_ms_by_gpu": args.iters_per_ms_by_gpu, "ngpus": ngpus,
        },
        spec=spec,
        batch_kwargs=spec_batch_kwargs(spec),
        epoch0=epoch0,
        drained_s=drained,
        cuda_ooms=len(ooms),
        pipelines_lost=len(set(lost)),
        tasks_done_in_window=len(done),
        tasks_per_hour=len(done) / hours,
        gpu_hours_per_task=(ngpus * hours / len(done)) if done else None,
        learned_memory=learned,
        tasks_registered=len(client),
        task_records=len(records),
        client=client,
        tasks=records,
    )
    log(f"{args.config}: {len(ooms)} OOMs, {len(set(lost))} pipelines lost, "
        f"{len(done)} tasks done in {args.window:.0f} s, drained at {drained:.0f} s")
    write_result(rec)


if __name__ == "__main__":
    asyncio.run(main())

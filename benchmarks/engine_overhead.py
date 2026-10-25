"""Per-task cost and memory of the engine, to compare two versions of radical.asyncflow.

    python benchmarks/engine_overhead.py measure
    python benchmarks/engine_overhead.py compare --base PYTHON --head PYTHON

`measure` prints, as JSON, the metrics of the radical.asyncflow that the interpreter
imports. `compare` runs `measure` in fresh processes of two interpreters, for example
the venvs of a pull request's base and head, interleaved (base, head, head, base, ...),
and prints a Markdown table of the medians across processes.

Method, as in the reviews of PRs #109, #111 and #112: each workload calls N tasks of
the Noop backend and lets the loop run until every one has registered (call+register),
then awaits them all (end-to-end). A process repeats this ROUNDS times after a warm-up
round and keeps its best round. The collector is off while a round is timed, as in
timeit, so that a collection does not land on whichever workload happens to trigger
it; its cost is measured on its own, over MEMORY_N tasks with the collector on. Memory
is what tracemalloc still traces per component after MEMORY_N executable tasks.
"""

import argparse
import asyncio
import gc
import json
import logging
import os
import statistics
import subprocess
import tempfile
import time
import tracemalloc

# Changes beyond this fraction are set in bold in the table
THRESHOLD = 0.05


async def _measure(n, rounds, memory_n):
    from radical.asyncflow import WorkflowEngine

    engine = await WorkflowEngine.create(dry_run=True)

    @engine.function_task
    async def t(x=None):
        return x

    @engine.executable_task
    async def e(x=None):
        return "echo hi"

    workloads = {
        "function, no args": lambda dep: [t() for _ in range(n)],
        "function, 2 plain args": lambda dep: [t(1, x=2) for _ in range(n)],
        "function, 1 future arg": lambda dep: [t(dep) for _ in range(n)],
        "executable, no args": lambda dep: [e() for _ in range(n)],
    }
    results = {}
    for label, make in workloads.items():
        register, total = [], []
        for i in range(rounds + 1):
            dep = t()  # a finished dependency, for the workload that takes one
            await dep
            gc.collect()
            gc.disable()
            t0 = time.perf_counter()
            futs = make(dep)
            await asyncio.sleep(0)  # every registration task runs once
            while not all(hasattr(f, "task") for f in futs):
                await asyncio.sleep(0)
            t1 = time.perf_counter()
            await asyncio.gather(*futs)
            t2 = time.perf_counter()
            gc.enable()
            engine._clear_internal_records()
            if i:  # round 0 warms up
                register.append((t1 - t0) / n * 1e6)
                total.append((t2 - t0) / n * 1e6)
        results[f"{label}: call+register (µs)"] = min(register)
        results[f"{label}: end-to-end (µs)"] = min(total)

    # The collector's share: function tasks with a future argument and executable
    # tasks, alternating, with the collector on.
    pauses = []

    def on_gc(phase, info):
        pauses.append(time.perf_counter() if phase == "start" else -time.perf_counter())

    dep = t()
    await dep
    gc.collect()
    before = [s["collections"] for s in gc.get_stats()]
    gc.callbacks.append(on_gc)
    t0 = time.perf_counter()
    futs = [t(dep) if k % 2 else e() for k in range(memory_n)]
    await asyncio.gather(*futs)
    t1 = time.perf_counter()
    gc.callbacks.remove(on_gc)
    after = [s["collections"] for s in gc.get_stats()]
    results["mixed, collector on: end-to-end (µs)"] = (t1 - t0) / memory_n * 1e6
    results["mixed, collector on: in collections (µs)"] = -sum(pauses) / memory_n * 1e6
    results["mixed, collector on: collections"] = sum(
        a - b for a, b in zip(after, before)
    )
    del futs
    engine._clear_internal_records()

    gc.collect()
    tracemalloc.start()
    futs = [e() for _ in range(memory_n)]
    await asyncio.gather(*futs)
    gc.collect()
    retained, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    results["memory retained per component (B)"] = retained / memory_n

    await engine.shutdown()
    return results


def measure(args):
    logging.disable(logging.CRITICAL)
    with tempfile.TemporaryDirectory() as work_dir:
        os.chdir(work_dir)  # where the engine makes its work directory
        results = asyncio.run(_measure(args.n, args.rounds, args.memory_n))
    print(json.dumps(results))


def compare(args):
    pythons = {"base": args.base, "head": args.head}
    command = [
        os.path.abspath(__file__),
        "measure",
        f"--n={args.n}",
        f"--rounds={args.rounds}",
        f"--memory-n={args.memory_n}",
    ]
    samples = {"base": [], "head": []}
    for run in range(args.runs):
        for side in ("base", "head") if run % 2 == 0 else ("head", "base"):
            out = subprocess.run(
                [pythons[side], "-I", *command],
                capture_output=True,
                text=True,
                check=True,
            )
            samples[side].append(json.loads(out.stdout))

    print(f"### Engine overhead: {args.head_label} against {args.base_label}\n")
    print("| | base | head | change |\n| --- | ---: | ---: | ---: |")
    for metric in samples["base"][0]:
        base = [s[metric] for s in samples["base"]]
        head = [s[metric] for s in samples["head"]]
        change = statistics.median(head) / statistics.median(base) - 1
        shown = f"{change:+.1%}"
        if change > THRESHOLD:
            shown = f"**{shown}**"
        print(f"| {metric} | {_spread(base)} | {_spread(head)} | {shown} |")
    print(
        f"\nMedian (min–max) of {args.runs} processes per side, interleaved; each "
        f"process keeps the best of {args.rounds} rounds of {args.n:,} tasks per "
        f"workload, and measures memory over {args.memory_n:,} executable tasks. "
        f"Changes above {THRESHOLD:.0%} are in bold. Informational: this job does not "
        "fail the build."
    )


def _spread(values):
    digits = 0 if max(values) >= 100 else 2
    return (
        f"{statistics.median(values):.{digits}f} "
        f"({min(values):.{digits}f}–{max(values):.{digits}f})"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("measure", "compare"):
        p = sub.add_parser(name)
        p.add_argument("--n", type=int, default=10_000, help="tasks per round")
        p.add_argument("--rounds", type=int, default=6, help="rounds per process")
        p.add_argument(
            "--memory-n", type=int, default=20_000, help="tasks measured for memory"
        )
        if name == "compare":
            p.add_argument("--base", required=True, help="interpreter of the base")
            p.add_argument("--head", required=True, help="interpreter of the head")
            p.add_argument("--runs", type=int, default=5, help="processes per side")
            p.add_argument("--base-label", default="base")
            p.add_argument("--head-label", default="head")
    args = parser.parse_args()
    {"measure": measure, "compare": compare}[args.command](args)


if __name__ == "__main__":
    main()

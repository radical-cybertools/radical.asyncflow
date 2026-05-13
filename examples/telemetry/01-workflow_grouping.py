"""01-workflow_grouping.py — Parallel ML training campaign with workflow telemetry.

Demonstrates ``workflow_scope()``: six independent training pipelines run
concurrently, each with four sequential stages:

    load_data → preprocess → train → evaluate

Every task inside a ``workflow_scope()`` automatically carries
``asyncflow.workflow_id`` in every telemetry event, enabling per-pipeline
Gantt charts, stage breakdowns, and cross-pipeline comparisons — with zero
changes to the task functions themselves.

Usage
-----
    python 01-workflow_grouping.py
    python 01-workflow_grouping.py --out results/
    python plot_workflow_gantt.py results/
"""

import argparse
import asyncio
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from rhapsody.backends import ConcurrentExecutionBackend
from rhapsody.telemetry import define_event
from rhapsody.telemetry.events import make_event

from radical.asyncflow import WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)

# ── Custom event ──────────────────────────────────────────────────────────────

ExperimentResult = define_event(
    "experiment.Result",
    experiment_id=str,
    dataset_size=int,
    learning_rate=float,
    loss=float,
    accuracy=float,
    elapsed_s=float,
)

# ── Experiment matrix: (dataset_size, learning_rate) ─────────────────────────

EXPERIMENTS = [
    (100, 0.010),
    (200, 0.005),
    (150, 0.020),
    (80, 0.050),
    (250, 0.001),
    (120, 0.010),
]

STAGES = ["load", "preprocess", "train", "evaluate"]


# ── Main ──────────────────────────────────────────────────────────────────────


async def main(out_dir: str = "telemetry-output") -> None:
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    init_default_logger(logging.INFO)

    backend = await ConcurrentExecutionBackend(ProcessPoolExecutor(max_workers=4))
    flow = await WorkflowEngine.create(backend)
    telemetry = await flow.start_telemetry(
        resource_poll_interval=0.5,
        checkpoint_path=out_dir,
    )

    # ── Task definitions ──────────────────────────────────────────────────────
    # Task functions are plain async coroutines — no telemetry imports needed.

    @flow.function_task
    async def load_data(experiment_id: str, size: int) -> dict:
        await asyncio.sleep(0.05 + size * 0.0008)
        return {"experiment_id": experiment_id, "size": size, "data": list(range(size))}

    @flow.function_task
    async def preprocess(raw: dict) -> dict:
        await asyncio.sleep(0.08 + raw["size"] * 0.0006)
        return {**raw, "features": [x / raw["size"] for x in raw["data"]]}

    @flow.function_task
    async def train(processed: dict, lr: float) -> dict:
        await asyncio.sleep(0.20 + processed["size"] * 0.0010)
        loss = 0.5 / (1.0 + processed["size"] * lr * 10)
        return {**processed, "loss": loss, "lr": lr}

    @flow.function_task
    async def evaluate(model: dict) -> dict:
        await asyncio.sleep(0.04 + model["size"] * 0.0003)
        accuracy = min(0.99, 0.75 + (1.0 - model["loss"]) * 0.24)
        return {**model, "accuracy": accuracy}

    # ── Per-experiment runner ─────────────────────────────────────────────────

    async def run_experiment(i: int, size: int, lr: float) -> dict:
        t0 = time.time()
        exp_id = f"exp-{i}"

        async with flow.workflow_scope(exp_id) as wid:
            # asyncflow.workflow_id propagates to all four tasks automatically.
            raw = load_data(exp_id, size)
            prep = preprocess(raw)
            model = train(prep, lr)
            result = await evaluate(model)

        elapsed = time.time() - t0

        # Emit application-level result from the orchestration layer.
        telemetry.emit(
            make_event(
                ExperimentResult,
                session_id=telemetry.session_id,
                backend="pipeline",
                task_id=wid,
                experiment_id=exp_id,
                dataset_size=size,
                learning_rate=lr,
                loss=result["loss"],
                accuracy=result["accuracy"],
                elapsed_s=elapsed,
            )
        )
        print(
            f"  {exp_id}  size={size:3d}  lr={lr:.3f}  "
            f"loss={result['loss']:.4f}  acc={result['accuracy']:.4f}  "
            f"({elapsed * 1000:.0f} ms)"
        )
        return result

    # ── Run all experiments concurrently ──────────────────────────────────────

    print(f"Running {len(EXPERIMENTS)} experiments concurrently …\n")
    t_start = time.time()
    await asyncio.gather(
        *(run_experiment(i, size, lr) for i, (size, lr) in enumerate(EXPERIMENTS))
    )
    total = time.time() - t_start

    summary = telemetry.summary()
    print(
        f"\n  {len(EXPERIMENTS)} experiments · "
        f"{summary['tasks']['completed']} tasks completed in {total * 1000:.0f} ms"
    )

    await flow.shutdown()
    await telemetry.stop()

    jsonl = next(Path(out_dir).glob("*.jsonl"), None)
    print(f"\nCheckpoint : {jsonl}")
    print(f"Plot       : python plot_workflow_gantt.py {out_dir}/")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Parallel ML training campaign with workflow_scope() telemetry."
    )
    ap.add_argument(
        "--out",
        default="telemetry-output",
        metavar="DIR",
        help="Output directory for JSONL checkpoint (default: telemetry-output/)",
    )
    args = ap.parse_args()
    asyncio.run(main(out_dir=args.out))

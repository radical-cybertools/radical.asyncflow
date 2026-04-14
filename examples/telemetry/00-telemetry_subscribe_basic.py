"""11-telemetry-subscribe-basic.py.

A 5-stage ETL workflow demonstrating AsyncFlow telemetry:

    ingest → validate → transform → aggregate → report
                  ↘                ↗
                  validate_schema

Emits standard RHAPSODY lifecycle events (TaskCreated, asyncflow.TaskResolved,
TaskSubmitted, TaskStarted, TaskCompleted) plus a custom application event
(etl.StageTimer) that records wall-clock duration for each stage.

Checkpoint file written to telemetry-output/ for use with plot_workflow.py.
"""

import asyncio
import time
from concurrent.futures import ProcessPoolExecutor

from rhapsody.backends import ConcurrentExecutionBackend
from rhapsody.telemetry import define_event
from rhapsody.telemetry.events import make_event

from radical.asyncflow import WorkflowEngine

# ── Custom application event ──────────────────────────────────────────────────
StageTimer = define_event(
    "etl.StageTimer",
    stage=str,
    duration_ms=float,
)

# ── Workflow task functions ───────────────────────────────────────────────────


async def main():
    backend = await ConcurrentExecutionBackend(ProcessPoolExecutor(max_workers=4))
    flow = await WorkflowEngine.create(backend)
    telemetry = await flow.start_telemetry(
        resource_poll_interval=0.5,
        checkpoint_path="telemetry-output",
    )

    # ── Stage definitions ──────────────────────────────────────────────────────

    @flow.function_task
    async def ingest(source: str) -> dict:
        """Simulate reading raw data from a source."""
        await asyncio.sleep(0.3)
        return {"source": source, "rows": list(range(200)), "status": "raw"}

    @flow.function_task
    async def validate(data: dict) -> dict:
        """Check row count and basic integrity."""
        await asyncio.sleep(0.2)
        return {**data, "valid_rows": [r for r in data["rows"] if r % 3 != 0]}

    @flow.function_task
    async def validate_schema(data: dict) -> dict:
        """Parallel schema check — runs alongside validate."""
        await asyncio.sleep(0.15)
        return {**data, "schema_ok": True}

    @flow.function_task
    async def transform(validated: dict, schema: dict) -> dict:
        """Apply transformations once both validation gates pass."""
        await asyncio.sleep(0.25)
        rows = validated["valid_rows"]
        return {
            **validated,
            "transformed": [r * 2 for r in rows],
            "schema_ok": schema["schema_ok"],
        }

    @flow.function_task
    async def aggregate(transformed: dict) -> dict:
        """Compute summary statistics."""
        await asyncio.sleep(0.1)
        vals = transformed["transformed"]
        return {
            **transformed,
            "count": len(vals),
            "total": sum(vals),
            "mean": sum(vals) / len(vals) if vals else 0,
        }

    @flow.function_task
    async def report(aggregated: dict) -> dict:
        """Format final output record."""
        await asyncio.sleep(0.05)
        return {
            "source": aggregated["source"],
            "count": aggregated["count"],
            "total": aggregated["total"],
            "mean": aggregated["mean"],
            "schema_ok": aggregated["schema_ok"],
        }

    # ── Run N independent workflow instances in parallel ───────────────────────

    async def run_workflow(wf_id: int) -> dict:
        t_wf = time.time()

        t = time.time()
        raw = ingest(f"source_{wf_id}")
        telemetry.emit(
            make_event(
                StageTimer,
                session_id=telemetry.session_id,
                backend="etl",
                stage="ingest",
                duration_ms=(time.time() - t) * 1000,
            )
        )

        t = time.time()
        val = validate(raw)
        schema = validate_schema(raw)
        telemetry.emit(
            make_event(
                StageTimer,
                session_id=telemetry.session_id,
                backend="etl",
                stage="validate",
                duration_ms=(time.time() - t) * 1000,
            )
        )

        t = time.time()
        xform = transform(val, schema)
        telemetry.emit(
            make_event(
                StageTimer,
                session_id=telemetry.session_id,
                backend="etl",
                stage="transform",
                duration_ms=(time.time() - t) * 1000,
            )
        )

        t = time.time()
        agg = aggregate(xform)
        telemetry.emit(
            make_event(
                StageTimer,
                session_id=telemetry.session_id,
                backend="etl",
                stage="aggregate",
                duration_ms=(time.time() - t) * 1000,
            )
        )

        t = time.time()
        result = await report(agg)
        telemetry.emit(
            make_event(
                StageTimer,
                session_id=telemetry.session_id,
                backend="etl",
                stage="report",
                duration_ms=(time.time() - t) * 1000,
            )
        )

        telemetry.emit(
            make_event(
                StageTimer,
                session_id=telemetry.session_id,
                backend="etl",
                stage="workflow_total",
                duration_ms=(time.time() - t_wf) * 1000,
            )
        )
        return result

    N = 20
    print(f"Running {N} ETL workflow instances …")
    t0 = time.time()
    results = await asyncio.gather(*(run_workflow(i) for i in range(N)))
    elapsed = time.time() - t0

    print(f"Completed {N} workflows in {elapsed * 1000:.0f} ms")

    summary = telemetry.summary()
    print(f"Tasks — {summary['tasks']}")
    if summary.get("duration"):
        d = summary["duration"]
        print(
            f"Mean task time: {d['mean_seconds'] * 1000:.1f} ms  "
            f"Max: {d['max_seconds'] * 1000:.1f} ms"
        )

    await flow.shutdown()
    await telemetry.stop()


if __name__ == "__main__":
    asyncio.run(main())

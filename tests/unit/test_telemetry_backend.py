"""Tests that telemetry names the backend that ran each task."""

import asyncio

import pytest

pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")

from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine


async def _run_on_two_backends():
    """Run tasks on two named backends.

    Returns the finished spans, the telemetry events, and the name of the backend that
    ran each task, read from the backends' own task registries.
    """
    compute = await LocalExecutionBackend(name="compute")
    ai = await LocalExecutionBackend(name="ai")
    # "compute" comes first, so it is the default backend
    engine = await WorkflowEngine.create(backend=[compute, ai])
    exporter = InMemorySpanExporter()
    telemetry = await engine.start_telemetry(
        span_processors=[SimpleSpanProcessor(exporter)]
    )
    events = []
    telemetry.subscribe(events.append)

    @engine.function_task(backend="compute")
    async def simulate(x):
        return x

    @engine.function_task(backend="ai")
    async def infer(x):
        return x

    @engine.function_task
    async def report(x):
        return x

    @engine.function_task(backend="ai")
    async def broken():
        raise ValueError("expected failure")

    # infer depends on simulate, so it resolves after a task on the other backend
    results = await asyncio.gather(
        report(infer(simulate(1))), infer(2), broken(), return_exceptions=True
    )
    assert results[:2] == [1, 2]
    assert isinstance(results[2], ValueError)

    ran_on = {uid: b.name for b in (compute, ai) for uid in b.tasks}
    await telemetry.stop()
    await engine.shutdown()
    return exporter.get_finished_spans(), events, ran_on


@pytest.mark.asyncio
async def test_task_span_backend_is_the_backend_that_ran_the_task():
    """Each task span's backend is the backend that ran it, not the default."""
    spans, _, ran_on = await _run_on_two_backends()

    assert sorted(ran_on.values()) == ["ai", "ai", "ai", "compute", "compute"]
    task_spans = {s.attributes["task_id"]: s for s in spans if s.name == "task"}
    assert task_spans.keys() == ran_on.keys()
    for uid, span in task_spans.items():
        assert span.attributes["backend"] == ran_on[uid], uid


@pytest.mark.asyncio
async def test_task_events_carry_the_backend_that_ran_the_task():
    """Every lifecycle event of a task, which feeds the task metrics, names its
    backend."""
    _, events, ran_on = await _run_on_two_backends()

    task_events = [e for e in events if e.task_id in ran_on]
    assert {e.event_type for e in task_events} >= {
        "TaskCreated",
        "asyncflow.TaskResolved",
        "TaskQueued",
        "TaskSubmitted",
        "TaskCompleted",
        "TaskFailed",
    }
    for event in task_events:
        assert event.backend == ran_on[event.task_id], (event.event_type, event.task_id)

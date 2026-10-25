"""Tests that the span current at call time becomes the parent of the task span."""

import pytest

pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")

from opentelemetry import context as otel_context
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from radical.asyncflow import WorkflowEngine


async def _make_engine():
    """Return an engine, its telemetry manager and an exporter of its spans."""
    engine = await WorkflowEngine.create(dry_run=True)
    exporter = InMemorySpanExporter()
    telemetry = await engine.start_telemetry(
        span_processors=[SimpleSpanProcessor(exporter)]
    )
    return engine, telemetry, exporter


def _task_span(exporter, uid):
    spans = [
        s
        for s in exporter.get_finished_spans()
        if s.name == "task" and s.attributes.get("task_id") == uid
    ]
    assert len(spans) == 1
    return spans[0]


@pytest.mark.asyncio
async def test_current_span_is_task_span_parent():
    """A span current when the task is called is the task span's parent."""
    engine, telemetry, exporter = await _make_engine()
    app_tracer = TracerProvider().get_tracer("application")

    @engine.function_task
    async def t():
        return 1

    # A new trace, separate from the telemetry session's own trace
    with app_tracer.start_as_current_span(
        "app.tool_call", context=otel_context.Context()
    ) as parent:
        fut = t()
    await fut
    await telemetry.stop()
    await engine.shutdown()

    span = _task_span(exporter, fut.uid)
    parent_ctx = parent.get_span_context()
    assert span.parent is not None
    assert span.parent.span_id == parent_ctx.span_id
    assert span.context.trace_id == parent_ctx.trace_id


@pytest.mark.asyncio
async def test_extracted_w3c_context_is_task_span_parent():
    """A context extracted from a traceparent carrier parents the task span."""
    engine, telemetry, exporter = await _make_engine()
    app_tracer = TracerProvider().get_tracer("application")

    @engine.function_task
    async def t():
        return 1

    # The carrier a remote or resumed caller would hold
    carrier = {}
    with app_tracer.start_as_current_span(
        "remote.caller", context=otel_context.Context()
    ) as remote:
        inject(carrier)
    assert "traceparent" in carrier

    token = otel_context.attach(extract(carrier))
    try:
        fut = t()
    finally:
        otel_context.detach(token)
    await fut
    await telemetry.stop()
    await engine.shutdown()

    span = _task_span(exporter, fut.uid)
    remote_ctx = remote.get_span_context()
    assert span.parent is not None
    assert span.parent.span_id == remote_ctx.span_id
    assert span.context.trace_id == remote_ctx.trace_id

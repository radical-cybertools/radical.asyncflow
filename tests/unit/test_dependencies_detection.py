import asyncio

import pytest

from radical.asyncflow import (
    LocalExecutionBackend,
    NoopExecutionBackend,
    WorkflowEngine,
)
from radical.asyncflow.data import InputFile, OutputFile


@pytest.mark.asyncio
async def test_detect_data_dependencies():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())
    a = InputFile("a.txt")
    b = OutputFile("b.txt")

    _, input_deps, output_deps = engine._detect_dependencies([a, b, 42, "string"])

    assert a.filename in input_deps
    assert b.filename in output_deps
    assert 42 not in input_deps
    assert "string" not in input_deps


@pytest.mark.asyncio
async def test_detect_task_dependencies():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    @engine.function_task
    async def task1():
        return 1

    @engine.function_task
    async def task2():
        return 2

    task = task2(task1)
    await task

    task_deps, _, _ = engine._detect_dependencies([task])

    assert len(task_deps) == 1
    assert task1 in task_deps[0]["args"]


@pytest.mark.asyncio
async def test_kwarg_future_is_tracked_as_dependency():
    """A future passed as kwarg must delay submission until it is resolved.

    Regression test: kwarg futures were resolved at submission but never
    registered as dependencies, so a consumer could be submitted while a
    slow kwarg producer was still running (InvalidStateError).
    """
    backend = await LocalExecutionBackend()
    flow = await WorkflowEngine.create(backend=backend)
    try:

        @flow.function_task
        async def slow_producer():
            await asyncio.sleep(0.05)
            return "value"

        @flow.function_task
        async def consumer(kw=None):
            return kw

        result = await asyncio.wait_for(consumer(kw=slow_producer()), timeout=10)
        assert result == "value"
    finally:
        await flow.shutdown()


@pytest.mark.asyncio
async def test_duplicate_future_as_arg_and_kwarg():
    """The same future passed twice must count as one dependency.

    Without deduplication the dependency count is incremented twice but only decremented once on
    completion, deadlocking the consumer.
    """
    backend = await LocalExecutionBackend()
    flow = await WorkflowEngine.create(backend=backend)
    try:

        @flow.function_task
        async def producer():
            return 7

        @flow.function_task
        async def consumer(pos, kw=None):
            return pos + kw

        fut = producer()
        result = await asyncio.wait_for(consumer(fut, kw=fut), timeout=10)
        assert result == 14
    finally:
        await flow.shutdown()


@pytest.mark.asyncio
async def test_plain_future_dependency_rejected():
    """A plain asyncio.Future (not produced by a task or block) fails with a clear TypeError instead
    of an unhelpful error downstream."""
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())
    with pytest.raises(TypeError, match="asyncflow tasks or blocks"):
        engine._detect_dependencies([asyncio.Future()])

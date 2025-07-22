import asyncio
import time

import pytest

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import DaskExecutionBackend


@pytest.mark.asyncio
async def test_task_cancellation_simple():
    """Basic: Cancel tasks while running, they raise CancelledError."""
    backend = DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def task():
        await asyncio.sleep(10)

    t1 = task()
    t2 = task()

    await asyncio.sleep(1)

    t1.cancel()
    t2.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1
    with pytest.raises(asyncio.CancelledError):
        await t2

    await flow.shutdown()


@pytest.mark.asyncio
async def test_cancel_before_start():
    """Cancel a task before it even schedules."""
    backend = DaskExecutionBackend({'n_workers': 1, 'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def slow_task():
        await asyncio.sleep(5)

    @flow.function_task
    async def fast_task():
        return "done"

    t1 = slow_task()   # occupies worker
    t2 = slow_task()   # queued

    await asyncio.sleep(1)

    t2.cancel()  # cancel before it starts

    # Let t1 finish
    await t1

    with pytest.raises(asyncio.CancelledError):
        await t2

    await flow.shutdown()


@pytest.mark.asyncio
async def test_cancel_after_completion():
    """Cancel a task after it already completed — no effect."""
    backend = DaskExecutionBackend({'n_workers': 1, 'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def quick_task():
        return "done"

    t = quick_task()
    result = await t

    assert result == "done"

    # Cancel after done — should not throw
    t.cancel()

    # Still returns the result
    assert await t == "done"

    await flow.shutdown()


@pytest.mark.asyncio
async def test_cancel_one_of_many():
    """Cancel one task out of many — others should not be affected."""
    backend = DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def task(n):
        await asyncio.sleep(5)
        return n

    t1 = task(1)
    t2 = task(2)

    await asyncio.sleep(1)
    t1.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1

    assert await t2 == 2

    await flow.shutdown()


@pytest.mark.asyncio
async def test_cancel_with_dependencies():
    """Cancel a task that another task depends on — dependency should error."""
    backend = DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def parent():
        await asyncio.sleep(5)
        return 42

    @flow.function_task
    async def child(x):
        return x + 1

    t1 = parent()
    t2 = child(t1)

    await asyncio.sleep(1)

    t1.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1

    with pytest.raises(asyncio.CancelledError):  # child cannot complete
        await t2

    await flow.shutdown()


@pytest.mark.asyncio
async def test_cancel_during_shutdown():
    """Cancel tasks and then shutdown — shutdown must complete cleanly."""
    backend = DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def task():
        await asyncio.sleep(10)

    t = task()

    await asyncio.sleep(1)
    t.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t

    # Even if tasks cancelled, shutdown completes
    await flow.shutdown()

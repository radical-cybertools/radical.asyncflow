# tests/integration/test_dask_workflow.py

import pytest
import asyncio
import pytest_asyncio
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import DaskExecutionBackend


@pytest_asyncio.fixture
async def flow():
    # Setup: create backend and flow
    backend = DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)
    
    # provide the flow to the test
    yield flow

@pytest.mark.asyncio
async def test_funnel_dag_with_dask_backend(flow):

    @flow.function_task
    async def task1(*args):
        return 1 * 1

    @flow.function_task
    async def task2(*args):
        return 2 * 2

    @flow.function_task
    async def task3(t1, t2):
        return 3 * 3 * t1 * t2

    t1 = await task1()
    t2 = await task2()
    
    t3 = await task3(t1, t2)

    assert isinstance(t3, int)
    assert t3 == 36

@pytest.mark.asyncio
async def test_async_funnel_dag_with_dask_backend(flow):

    @flow.function_task
    async def task1():
        return 1 * 1

    @flow.function_task
    async def task2():
        return 2 * 2

    @flow.function_task
    async def task3(t1_result, t2_result):
        return 3 * 3 * t1_result * t2_result

    # Launch task1 and task2 in parallel
    t1_future = task1()
    t2_future = task2()

    # Await both in parallel
    t1_result, t2_result = await asyncio.gather(t1_future, t2_future)

    # Call task3 with the gathered results
    t3_future = task3(t1_result, t2_result)

    # Await result of task3
    t3_result = await t3_future

    assert isinstance(t3_result, int)
    assert t3_result == 36

@pytest.mark.asyncio
async def test_dask_backend_rejects_executable_task(flow):

    with pytest.raises(ValueError, match="DaskExecutionBackend does not support executable tasks"):
        @flow.executable_task
        async def bad_task1():
            return '/bin/date'

        not_supported_task1 = bad_task1()
        await not_supported_task1

@pytest.mark.asyncio
async def test_dask_backend_regular_task_failure(flow):
    with pytest.raises(RuntimeError, match="Some error"):
        @flow.function_task
        async def bad_task2():
            raise RuntimeError('Some error')
    

        not_supported_task2 = bad_task2()
        await not_supported_task2

@pytest.mark.asyncio
async def test_task_cancellation(flow):
    """Basic: Cancel tasks while running, they raise CancelledError."""

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


@pytest.mark.asyncio
async def test_cancel_before_start(flow):
    """Cancel a task before it even schedules."""

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


@pytest.mark.asyncio
async def test_cancel_after_completion(flow):
    """Cancel a task after it already completed — no effect."""

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


@pytest.mark.asyncio
async def test_cancel_one_of_many(flow):
    """Cancel one task out of many — others should not be affected."""

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


@pytest.mark.asyncio
async def test_cancel_with_dependencies(flow):
    """Cancel a task that another task depends on — dependency should error."""

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

import pytest
import asyncio

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import OutputFile, InputFile
from radical.asyncflow import RadicalExecutionBackend

backend = RadicalExecutionBackend({'resource': 'local.localhost'})

@pytest.mark.asyncio
async def test_async_bag_of_tasks():
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def echo_task(i):
        return f'/bin/echo "Task {i} executed at" && /bin/date'

    # Create a bag of independent tasks
    bag_size = 10
    tasks = [echo_task(i) for i in range(bag_size)]

    # Await all tasks in parallel
    results = await asyncio.gather(*tasks)

    assert len(results) == bag_size
    for i, result in enumerate(results):
        assert result is not None, f"Task {i} returned None"
        assert isinstance(result, str) or isinstance(result, bytes)

@pytest.mark.asyncio
async def test_radical_backend_reject_service_task_function():
    flow = await WorkflowEngine.create(backend=backend)

    with pytest.raises(ValueError, match="RadicalExecutionBackend does not support function service tasks"):
        @flow.function_task(service=True)
        async def bad_task2():
            return True

        not_supported_task2 = bad_task2()
        await not_supported_task2

    await flow.shutdown(skip_execution_backend=True)

@pytest.mark.asyncio
async def test_radical_backend_reject_function_task_with_raptor_off():
    flow = await WorkflowEngine.create(backend=backend)

    with pytest.raises(RuntimeError):
        @flow.function_task
        async def bad_task3():
            return True

        not_supported_task3 = bad_task3()
        await not_supported_task3

    # the last test will shutdown everything
    await flow.shutdown(skip_execution_backend=True)

@pytest.mark.asyncio
async def test_radical_backend_implicit_data():
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task1(*args):
        return 'echo "This is a file from task1" > t1_output.txt'

    @flow.executable_task
    async def task2(*args):
        return '/bin/cat t1_output.txt'

    t1 = task1()
    t2 = task2(t1)

    assert await t2 == 'This is a file from task1\n'

    await flow.shutdown(skip_execution_backend=True)

@pytest.mark.asyncio
async def test_radical_backend_explicit_data():
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task1(*args):
        return 'echo "This is a file from task1" > t1_output.txt'

    @flow.executable_task
    async def task2(*args):
        return '/bin/cat t1_output.txt'

    t1 = task1(OutputFile('t1_output.txt'))
    t2 = task2(t1, InputFile('t1_output.txt'))

    assert await t2 == 'This is a file from task1\n'

    await flow.shutdown(skip_execution_backend=True)

@pytest.mark.asyncio
async def test_radical_backend_input_data_staging():
    flow = await WorkflowEngine.create(backend=backend)

    with open('t1_input.txt', 'w') as f:
        f.write('This is a file staged in to task1\n')

    @flow.executable_task
    async def task1(*args):
        return '/bin/cat t1_input.txt'

    t1 = task1(InputFile('t1_input.txt'))

    assert await t1 == 'This is a file staged in to task1\n'

    await flow.shutdown(skip_execution_backend=True)

@pytest.mark.asyncio
async def test_async_cancel_tasks():
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task():
        return '/bin/sleep 10'

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
async def test_async_cancel_before_start():
    """Cancel a task before it even schedules."""
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def slow_task():
        return '/bin/sleep 5'

    @flow.executable_task
    async def fast_task():
        return '/bin/echo "done"'

    t1 = slow_task()   # occupies worker
    t2 = slow_task()   # queued

    await asyncio.sleep(1)

    t2.cancel()  # cancel before it starts

    # Let t1 finish
    await t1

    with pytest.raises(asyncio.CancelledError):
        await t2

@pytest.mark.asyncio
async def test_async_cancel_after_completion():
    """Cancel a task after it already completed — no effect."""
    flow = await WorkflowEngine.create(backend=backend)
    
    @flow.executable_task
    async def quick_task():
        return "/bin/echo 'done'"

    t = quick_task()
    result = await t

    assert result.strip() == "done"

    # Cancel after done — should not throw
    t.cancel()

    # Still returns the result
    t_result = await t
    assert t_result.strip() == "done"

@pytest.mark.asyncio
async def test_async_cancel_one_of_many():
    """Cancel one task out of many — others should not be affected."""
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task(n):
        return '/bin/sleep 5 && /bin/echo "2"'

    t1 = task(1)
    t2 = task(2)

    await asyncio.sleep(1)
    t1.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1

    str_result = await t2
    assert int(str_result) == 2

    await flow.shutdown()

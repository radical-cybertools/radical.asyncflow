import pytest
import asyncio
import time

from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionBackend

backend = RadicalExecutionBackend({'resource': 'local.localhost'})

def test_sync_bag_of_tasks():
    flow = WorkflowEngine(backend=backend)

    @flow.executable_task
    def echo_task(i):
        return f'/bin/echo "Task {i} executed at" && /bin/date'

    # Create a bag of independent tasks
    bag_size = 10
    tasks = [echo_task(i) for i in range(bag_size)]

    # Await all tasks in parallel
    results = [t.result() for t in tasks]

    assert len(results) == bag_size
    for i, result in enumerate(results):
        assert result is not None, f"Task {i} returned None"
        assert isinstance(result, str) or isinstance(result, bytes)

    flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_async_bag_of_tasks():
    flow = WorkflowEngine(backend=backend)

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

    await flow.shutdown(skip_execution_backend=True)

def test_radical_backend_reject_service_task_function():
    flow = WorkflowEngine(backend=backend)

    with pytest.raises(ValueError, match="RadicalExecutionBackend does not support function service tasks"):
        @flow.function_task(service=True)
        def bad_task2():
            return True
    
        not_supported_task2 = bad_task2()
        not_supported_task2.result()

    flow.shutdown(skip_execution_backend=True)

def test_radical_backend_reject_function_task_with_raptor_off():
    flow = WorkflowEngine(backend=backend)

    with pytest.raises(RuntimeError):
        @flow.function_task
        def bad_task3():
            return True

        not_supported_task3 = bad_task3()
        not_supported_task3.result()

    # the last test will shutdown everything
    flow.shutdown()

import pytest
import asyncio
import time

from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionBackend

def test_sync_bag_of_tasks():
    backend = RadicalExecutionBackend({'resource': 'local.localhost'})
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

    flow.shutdown()


@pytest.mark.asyncio
async def test_async_bag_of_tasks():
    backend = RadicalExecutionBackend({'resource': 'local.localhost'})
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

    await flow.shutdown()

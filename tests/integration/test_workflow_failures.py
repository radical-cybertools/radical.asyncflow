import pytest
import asyncio

from radical.flow import WorkflowEngine
from radical.flow import ThreadExecutionBackend

@pytest.mark.asyncio
async def test_task_failure_handling():
    """
    Test the workflow engine's ability to handle task failures.

    This test verifies that:
    - A successful task returns the expected result.
    - A failing task raises the expected exception with the correct message.
    - The workflow engine can properly shut down after task execution.

    Tasks defined:
    - good_task: Returns "success".
    - failing_task: Raises an exception with the message "Simulated failure".
    - dependent_task: Returns a formatted string based on the result of a previous task.

    Assertions:
    - The result of good_task is "success".
    - failing_task raises an Exception with the message "Simulated failure".
    """
    backend = ThreadExecutionBackend({})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def good_task():
        return "success"

    @flow.function_task
    async def failing_task():
        raise Exception("Simulated failure")

    @flow.function_task
    async def dependent_task(prev_result):
        return f"Got: {prev_result}"

    try:
        # Run good task first
        res = await good_task()
        assert res == "success"

        with pytest.raises(Exception, match="Simulated failure"):
            await failing_task()
    finally:
        await flow.shutdown()


@pytest.mark.asyncio
async def test_awaiting_failed_task_propagates_exception():
    """
    Test that awaiting a failed task in a workflow propagates
    the exception to dependent tasks.
    This test verifies that when a task (`task1`) raises an
    exception, any subsequent task (`task2`) that awaits its
    result does not execute, and the exception is properly
    propagated. The test expects an exception with the message
    "Intentional failure in task1" to be raised when awaiting
    `task1()`. After the test, the workflow engine is properly
    shut down.
    """
    backend = ThreadExecutionBackend({})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def task1():
        raise Exception("Intentional failure in task1")

    @flow.function_task
    async def task2(x):
        return f"Received: {x}"

    try:
        # The test will fail at await task1(), so task2 should never run
        with pytest.raises(Exception, match="Intentional failure in task1"):
            await task2(await task1())
    
    finally:
        await flow.shutdown()


@pytest.mark.asyncio
async def test_independent_workflow_failures_do_not_affect_others():
    """
    Test that failure in one async workflow does not impact other
    concurrently running workflows. Workflow 0 is designed to fail
    at task1. Other workflows should complete successfully regardless
    of the failure in workflow 0.
    """

    backend = ThreadExecutionBackend({})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def task1(wf_id):
        if wf_id == 0:
            raise Exception("Intentional failure in workflow 0 task1")
        return f"task1 success from wf {wf_id}"

    @flow.function_task
    async def task2(x):
        return f"task2 got: {x}"

    async def run_wf(wf_id):
        try:
            t1 = await task1(wf_id)
            t2 = await task2(t1)
            return f"Workflow {wf_id} success: {t2}"
        except Exception as e:
            return f"Workflow {wf_id} failed: {str(e)}"

    try:
        # Run 5 workflows concurrently, workflow 0 should fail
        results = await asyncio.gather(*[run_wf(i) for i in range(5)])
        
        # Assert workflow 0 failed with the expected message
        assert "Workflow 0 failed: Intentional failure in workflow 0 task1" in results[0]

        # All other workflows should succeed
        for i in range(1, 5):
            assert f"Workflow {i} success:" in results[i]

    finally:
        await flow.shutdown()

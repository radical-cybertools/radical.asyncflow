import asyncio

import pytest

from radical.asyncflow import WorkflowEngine


@pytest.mark.asyncio
async def test_async_context_startup():
    engine = await WorkflowEngine.create(dry_run=True)
    await asyncio.sleep(0.1)  # Give some time for the loop to process startup tasks

    # Check that tasks have been scheduled
    assert hasattr(engine, "_run_task"), "Engine did not schedule run task."
    assert not engine._run_task.done(), "Run task finished prematurely."

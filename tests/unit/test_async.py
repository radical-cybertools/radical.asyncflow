import time
import pytest
import asyncio
import threading

from radical.asyncflow import WorkflowEngine


@pytest.mark.asyncio
async def test_async_context_startup():
    engine = WorkflowEngine(dry_run=True)
    await asyncio.sleep(0.1)  # Give some time for the loop to process startup tasks

    # Check that tasks have been scheduled
    assert hasattr(engine, '_run_task'), "Engine did not schedule run task."
    assert hasattr(engine, '_submit_task'), "Engine did not schedule submit task."
    assert not engine._run_task.done(), "Run task finished prematurely."
    assert not engine._submit_task.done(), "Submit task finished prematurely."


def test_sync_context_startup():
    engine = WorkflowEngine(dry_run=True)

    # Simulate sync context (event loop is not running)
    engine.loop = asyncio.new_event_loop()

    thread = threading.Thread(target=lambda: engine.loop.run_forever(), daemon=True)
    thread.start()

    time.sleep(0.2)  # Allow the background thread to start

    assert hasattr(engine, '_run_task'), "Engine did not schedule run task in sync context."
    assert hasattr(engine, '_submit_task'), "Engine did not schedule submit task in sync context."
    assert not engine._run_task.done(), "Run task finished prematurely."
    assert not engine._submit_task.done(), "Submit task finished prematurely."

    engine.loop.call_soon_threadsafe(engine.loop.stop)
    thread.join(timeout=1)

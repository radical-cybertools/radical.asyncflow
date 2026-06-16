"""Tests for task and block cancellation behaviour."""

import asyncio

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine


async def _make_engine():
    return await WorkflowEngine.create(backend=NoopExecutionBackend())


@pytest.mark.asyncio
async def test_pending_cancel_actually_cancels():
    """Cancel() on a pending future must mark it cancelled, not return the method
    object."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "PENDING"
    uid = "task.pending-test"
    fut.cancel = engine._setup_future_cancel_hook(fut, uid)

    result = fut.cancel()

    assert result is True
    assert fut.cancelled()
    assert fut.state == "CANCELLED"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_pending_cancel_forwards_msg():
    """Cancel(msg) must forward the message through to the underlying future."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "PENDING"
    fut.cancel = engine._setup_future_cancel_hook(fut, "task.msg-test")

    fut.cancel("cancel-reason")

    assert fut.cancelled()
    assert fut.state == "CANCELLED"

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await fut
    assert "cancel-reason" in str(exc_info.value)

    await engine.shutdown()


@pytest.mark.asyncio
async def test_block_cancel_stops_execution():
    """Cancelling a block future must stop the underlying execute_block coroutine."""
    engine = await _make_engine()
    execution_reached_end = False
    block_started = asyncio.Event()

    @engine.block
    async def long_block():
        nonlocal execution_reached_end
        block_started.set()
        await asyncio.sleep(2)
        execution_reached_end = True

    block_fut = long_block()
    await asyncio.sleep(0.05)
    await block_started.wait()

    block_fut.cancel()
    await asyncio.sleep(0.1)

    assert block_fut.cancelled()
    assert not execution_reached_end

    await engine.shutdown()


@pytest.mark.asyncio
async def test_shutdown_with_running_block_does_not_crash():
    """Shutdown() must not raise AttributeError when a block is still running."""
    engine = await _make_engine()
    block_started = asyncio.Event()

    @engine.block
    async def long_block():
        block_started.set()
        await asyncio.sleep(10)

    block_fut = long_block()
    await block_started.wait()

    await engine.shutdown()

    assert block_fut.cancelled() or block_fut.done()


@pytest.mark.asyncio
async def test_handle_task_cancellation_is_idempotent():
    """Calling handle_task_cancellation twice on the same future is a silent no-op."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "PENDING"
    fut.original_cancel = fut.cancel
    task_desc = {"uid": "task.idempotent-test"}

    engine.handle_task_cancellation(task_desc, fut)
    assert fut.cancelled()

    engine.handle_task_cancellation(task_desc, fut)  # must not raise

    await engine.shutdown()

"""Tests for the public future.state attribute lifecycle."""

import asyncio

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine


async def _make_engine():
    return await WorkflowEngine.create(backend=NoopExecutionBackend())


@pytest.mark.asyncio
async def test_future_state_is_pending_on_creation():
    """A freshly returned future must have state == 'PENDING'."""
    engine = await _make_engine()

    @engine.function_task
    async def some_task():
        return 1

    fut = some_task()
    assert fut.state == "PENDING"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_future_state_transitions_to_done():
    """A successfully completed task future must have state == 'DONE'."""
    engine = await _make_engine()

    @engine.function_task
    async def simple_task():
        return "ok"

    fut = simple_task()
    await asyncio.sleep(0.1)

    assert fut.state == "DONE"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_future_state_transitions_to_running():
    """task_callbacks(RUNNING) must set state == 'RUNNING' on the future."""
    engine = await _make_engine()

    @engine.function_task
    async def some_task():
        return "ok"

    some_task()
    await asyncio.sleep(0.05)  # let async_wrapper register the component

    comp = next(iter(engine.components.values()))
    task_fut = comp["future"]

    # Simulate the backend emitting a RUNNING transition
    engine.task_callbacks(comp["description"], "RUNNING")

    assert task_fut.state == "RUNNING"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_future_state_transitions_to_failed():
    """handle_task_failure must set state == 'FAILED' on the future."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "RUNNING"
    task_desc = {
        "uid": "task.fail-test",
        "exception": RuntimeError("boom"),
        "stderr": "boom",
    }

    engine.handle_task_failure(task_desc, fut)

    assert fut.state == "FAILED"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_future_state_transitions_to_cancelled():
    """handle_task_cancellation must set state == 'CANCELLED' on the future."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "RUNNING"
    fut.original_cancel = fut.cancel
    task_desc = {"uid": "task.cancel-test"}

    engine.handle_task_cancellation(task_desc, fut)

    assert fut.state == "CANCELLED"

    await engine.shutdown()


# ---------------------------------------------------------------------------
# Block future state lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_block_future_state_transitions_running_then_done():
    """Block future must be RUNNING after submit and DONE after normal completion."""
    engine = await _make_engine()
    block_release = asyncio.Event()

    @engine.block
    async def held_block():
        await block_release.wait()

    block_fut = held_block()
    await asyncio.sleep(0.05)

    assert block_fut.state == "RUNNING"

    block_release.set()
    await asyncio.sleep(0.05)

    assert block_fut.state == "DONE"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_block_future_state_transitions_to_failed():
    """A block that raises must have state == 'FAILED' after the exception
    propagates."""
    engine = await _make_engine()

    @engine.block
    async def failing_block():
        raise ValueError("block error")

    block_fut = failing_block()
    await asyncio.sleep(0.1)

    assert block_fut.state == "FAILED"
    assert isinstance(block_fut.exception(), ValueError)

    await engine.shutdown()

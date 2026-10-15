"""Tests for block submission, task registry, and member cancellation propagation."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.backends import LocalExecutionBackend


async def _make_engine():
    return await WorkflowEngine.create(backend=NoopExecutionBackend())


async def _make_local_engine():
    backend = await LocalExecutionBackend(ThreadPoolExecutor(max_workers=8))
    return await WorkflowEngine.create(backend=backend)


# ---------------------------------------------------------------------------
# _block_asyncio_tasks registry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_block_asyncio_tasks_registry_populated():
    """_block_asyncio_tasks must hold an entry for a block while it is running."""
    engine = await _make_engine()
    block_started = asyncio.Event()
    block_release = asyncio.Event()

    @engine.block
    async def my_block():
        block_started.set()
        await block_release.wait()

    my_block()
    await asyncio.sleep(0.05)
    await block_started.wait()

    block_uid = next(iter(engine.components))
    assert block_uid in engine._block_asyncio_tasks

    block_release.set()
    await asyncio.sleep(0.05)

    assert block_uid not in engine._block_asyncio_tasks

    await engine.shutdown()


@pytest.mark.asyncio
async def test_block_asyncio_tasks_registry_cleared_on_completion():
    """Registry must be empty after a block completes normally."""
    engine = await _make_engine()

    @engine.block
    async def quick_block():
        return 42

    quick_block()
    await asyncio.sleep(0.1)

    assert len(engine._block_asyncio_tasks) == 0

    await engine.shutdown()


# ---------------------------------------------------------------------------
# _block_members — member registration and cleanup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_block_members_cleared_on_exception():
    """_block_members must be empty after a block that raises, not just on normal
    completion."""
    engine = await _make_engine()

    @engine.block
    async def failing_block():
        @engine.function_task
        async def inner():
            return "ok"

        await inner()
        raise RuntimeError("block error")

    block_fut = failing_block()
    await asyncio.sleep(0.2)

    assert block_fut.done() and not block_fut.cancelled()
    assert isinstance(block_fut.exception(), RuntimeError)
    assert len(engine._block_members) == 0

    await engine.shutdown()


@pytest.mark.asyncio
async def test_block_members_cleared_on_normal_completion():
    """_block_members must be empty after a block and its tasks complete normally."""
    engine = await _make_engine()

    @engine.block
    async def normal_block():
        @engine.function_task
        async def inner():
            return "ok"

        await inner()

    block_fut = normal_block()
    await asyncio.sleep(0.2)

    assert block_fut.done() and not block_fut.cancelled()
    assert len(engine._block_members) == 0

    await engine.shutdown()


@pytest.mark.asyncio
async def test_completed_tasks_removed_from_block_members():
    """Tasks that finish before block cancellation are cleaned up by their
    done_callback."""
    engine = await _make_engine()
    block_gate = asyncio.Event()

    @engine.block
    async def my_block():
        @engine.function_task
        async def quick_task():
            return "done"

        await quick_task()  # NoopBackend resolves immediately
        await block_gate.wait()  # keep block alive for inspection

    my_block()
    await asyncio.sleep(0.15)

    block_uid = next(
        uid for uid, c in engine.components.items() if c["type"] == "block"
    )
    assert len(engine._block_members.get(block_uid, set())) == 0

    block_gate.set()
    await asyncio.sleep(0.05)
    await engine.shutdown()


# ---------------------------------------------------------------------------
# Cancellation propagation to block members
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_block_cancellation_propagates_to_member_tasks():
    """All tasks registered inside a block before cancellation must be cancelled."""
    engine = await _make_local_engine()

    @engine.block
    async def my_block():
        @engine.function_task
        async def t1():
            await asyncio.sleep(1)
            return "t1"

        @engine.function_task
        async def t2(dep):
            await asyncio.sleep(1)
            return "t2"

        @engine.function_task
        async def t3(dep1, dep2):
            await asyncio.sleep(1)
            return "t3"

        f1 = t1()
        f2 = t2(f1)
        f3 = t3(f1, f2)
        await f3

    block_fut = my_block()
    await asyncio.sleep(0.4)

    block_fut.cancel()
    await asyncio.sleep(0.3)

    assert block_fut.cancelled()
    task_comps = [c for c in engine.components.values() if c["type"] == "task"]
    assert len(task_comps) == 3
    assert all(c["future"].cancelled() for c in task_comps)

    await engine.shutdown()


@pytest.mark.asyncio
async def test_nested_block_cancellation_propagates():
    """Cancelling an outer block propagates recursively to inner blocks and their
    tasks."""
    engine = await _make_local_engine()

    @engine.block
    async def outer_block():
        @engine.block
        async def inner_block():
            @engine.function_task
            async def deep_task():
                await asyncio.sleep(1)
                return "never"

            f = deep_task()
            await f

        ib = inner_block()
        await ib

    outer_fut = outer_block()
    await asyncio.sleep(0.5)

    outer_fut.cancel()
    await asyncio.sleep(0.3)

    assert outer_fut.cancelled()
    deep_comp = next(c for c in engine.components.values() if c["type"] == "task")
    assert deep_comp["future"].cancelled()

    await engine.shutdown()


# ---------------------------------------------------------------------------
# CancelledError raised inside a block body
# ---------------------------------------------------------------------------


async def _cancelled_body_block(engine):
    """Register a block whose body awaits a member task that was cancelled."""

    @engine.function_task
    async def slow():
        await asyncio.sleep(10)

    @engine.block
    async def blk():
        member = slow()
        member.cancel()
        await member  # raises CancelledError inside the block body

    return blk()


@pytest.mark.asyncio
async def test_block_body_cancelled_error_cancels_block():
    """A CancelledError raised inside a block body cancels the block instead of leaving
    its future pending forever."""
    engine = await _make_local_engine()

    block_fut = await _cancelled_body_block(engine)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(block_fut), timeout=5)

    assert block_fut.cancelled()
    assert block_fut.state == "CANCELLED"
    assert not engine._run_task.done()

    await engine.shutdown()


@pytest.mark.asyncio
async def test_dependent_of_block_with_cancelled_body_is_cancelled():
    """A component depending on a block whose body raised CancelledError is cancelled
    instead of hanging."""
    engine = await _make_local_engine()

    @engine.function_task
    async def downstream(x):
        return x

    block_fut = await _cancelled_body_block(engine)
    dependent = downstream(block_fut)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(dependent), timeout=5)
    assert dependent.cancelled()

    await engine.shutdown()


@pytest.mark.asyncio
async def test_block_body_cancelled_error_cancels_running_members():
    """Members still running when the block body raises CancelledError are cancelled, as
    when the block is cancelled from outside."""
    engine = await _make_local_engine()
    members = {}

    @engine.function_task
    async def slow():
        await asyncio.sleep(10)

    @engine.block
    async def blk():
        members["running"] = slow()
        await asyncio.sleep(0.2)  # let the running member register and start
        cancelled = slow()
        cancelled.cancel()
        await cancelled

    block_fut = blk()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(block_fut), timeout=5)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(members["running"]), timeout=5)
    assert members["running"].cancelled()

    await engine.shutdown()


@pytest.mark.asyncio
async def test_cancelled_inner_block_cancels_outer_block():
    """An outer block awaiting an inner block whose body raised CancelledError is
    cancelled too (inner-to-outer propagation)."""
    engine = await _make_local_engine()

    @engine.function_task
    async def slow():
        await asyncio.sleep(10)

    @engine.block
    async def inner():
        member = slow()
        member.cancel()
        await member

    @engine.block
    async def outer():
        await inner()

    outer_fut = outer()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(outer_fut), timeout=5)
    assert outer_fut.cancelled()
    assert outer_fut.state == "CANCELLED"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_block_success_and_failure_states_unchanged():
    """Blocks still resolve with DONE + result or FAILED + the original exception."""
    engine = await _make_local_engine()

    @engine.block
    async def ok_block():
        return 42

    @engine.block
    async def failing_block():
        raise ValueError("boom")

    ok = ok_block()
    assert await asyncio.wait_for(ok, timeout=5) == 42
    assert ok.state == "DONE"

    bad = failing_block()
    with pytest.raises(ValueError, match="boom"):
        await asyncio.wait_for(bad, timeout=5)
    assert bad.state == "FAILED"

    await engine.shutdown()

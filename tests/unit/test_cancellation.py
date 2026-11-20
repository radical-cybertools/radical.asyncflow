"""Tests for task and block cancellation behaviour."""

import asyncio

import pytest

from radical.asyncflow import (
    LocalExecutionBackend,
    NoopExecutionBackend,
    WorkflowEngine,
)
from radical.asyncflow.workflow_manager import _ComponentFuture


async def _make_engine():
    return await WorkflowEngine.create(backend=NoopExecutionBackend())


def _pending_future(engine, uid):
    fut = _ComponentFuture()
    fut.state = "PENDING"
    fut.uid = uid
    fut._engine = engine
    return fut


class GatedBackend(NoopExecutionBackend):
    """Holds submit_tasks open until `gate` is set."""

    def __init__(self):
        super().__init__()
        self.entered = asyncio.Event()
        self.gate = asyncio.Event()

    async def submit_tasks(self, tasks):
        self.entered.set()
        await self.gate.wait()
        await super().submit_tasks(tasks)


@pytest.mark.asyncio
async def test_pending_cancel_actually_cancels():
    """Cancel() on a pending future must mark it cancelled, not return the method
    object."""
    engine = await _make_engine()

    fut = _pending_future(engine, "task.pending-test")

    result = fut.cancel()

    assert result is True
    assert fut.cancelled()
    assert fut.state == "CANCELLED"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_pending_cancel_forwards_msg():
    """Cancel(msg) must forward the message through to the underlying future."""
    engine = await _make_engine()

    fut = _pending_future(engine, "task.msg-test")

    fut.cancel("cancel-reason")

    assert fut.cancelled()
    assert fut.state == "CANCELLED"

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await fut
    assert "cancel-reason" in str(exc_info.value)

    await engine.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("task_type", ["executable", "function"])
async def test_task_cancelled_before_registration_never_registers(task_type):
    """A task cancelled before it registers must not register, emit events, or run
    its body to the end: cancel() also cancels its registration."""
    engine = await _make_engine()
    emitted = []
    emit = engine._emit

    def record(name, **kwargs):
        emitted.append(kwargs.get("task_id"))
        return emit(name, **kwargs)

    engine._emit = record
    body_finished = []

    @engine.executable_task
    async def slow_command():
        await asyncio.sleep(0.05)
        body_finished.append(True)
        return "/bin/true"

    @engine.function_task
    async def some_task():
        return 1

    fut = slow_command() if task_type == "executable" else some_task()
    if hasattr(fut, "task"):
        pytest.skip("registered inside the call, under an eager task factory")

    assert fut.cancel()
    await asyncio.sleep(0.1)

    assert fut.state == "CANCELLED"
    assert fut.uid not in engine.components
    assert fut.uid not in emitted
    assert fut not in engine._pending_registrations
    assert not body_finished

    await engine.shutdown()


@pytest.mark.asyncio
async def test_cancel_bound_before_registration_reaches_the_backend():
    """A `cancel` bound right after the call, before the task registers, must cancel the
    task on the backend once it runs there, as `fut.cancel()` would."""
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    cancelled_on_backend = []
    cancel_task = engine.backend.cancel_task

    async def record(uid):
        cancelled_on_backend.append(uid)
        return await cancel_task(uid)

    engine.backend.cancel_task = record

    @engine.function_task
    async def slow():
        await asyncio.sleep(0.5)

    fut = slow()
    cancel = fut.cancel
    for _ in range(200):
        if fut.uid in engine.running:
            break
        await asyncio.sleep(0.01)
    assert fut.uid in engine.running

    assert cancel()
    await asyncio.sleep(0.05)

    assert cancelled_on_backend == [fut.uid]
    assert fut.cancelled()
    assert fut.state == "CANCELLED"

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
async def test_block_cancelled_during_submit_never_starts():
    """A block cancelled while the tasks of its batch are being submitted must not run
    its body, and what depends on it must still be released."""
    backend = GatedBackend()
    engine = await WorkflowEngine.create(backend=backend)
    started = []

    @engine.function_task
    async def task():
        return 1

    @engine.block
    async def block():
        started.append("body")

    @engine.function_task
    async def dependent(value):
        return value

    # Called in one tick, so the task and the block are one batch
    task_fut = task()
    block_fut = block()
    dependent_fut = dependent(block_fut)
    await asyncio.wait_for(backend.entered.wait(), timeout=5)

    assert block_fut.cancel() is True
    backend.gate.set()
    await asyncio.wait_for(asyncio.wait([task_fut, dependent_fut]), timeout=5)

    assert started == []
    assert block_fut.cancelled()
    assert block_fut.state == "CANCELLED"
    assert dependent_fut.cancelled()
    assert task_fut.done() and not task_fut.cancelled()
    assert block_fut.uid not in engine._block_asyncio_tasks
    assert block_fut.uid not in engine._block_members

    await engine.shutdown()


@pytest.mark.asyncio
async def test_block_cancelled_by_a_block_of_its_batch_never_starts():
    """A block cancelled by an earlier block of its batch must not run its body.

    Under the default task factory its asyncio.Task exists by then but has not taken a
    step; under an eager one it does not exist yet.
    """
    engine = await _make_engine()
    started = []
    futures = {}

    @engine.block
    async def first():
        futures["second"].cancel()

    @engine.block
    async def second():
        started.append("body")

    first_fut = first()
    second_fut = futures["second"] = second()
    await asyncio.wait_for(asyncio.wait([first_fut, second_fut]), timeout=5)

    assert started == []
    assert first_fut.state == "DONE"
    assert second_fut.cancelled()
    assert second_fut.state == "CANCELLED"

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
async def test_handle_component_cancellation_is_idempotent():
    """Calling handle_component_cancellation twice on the same future is a silent no-
    op."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "PENDING"
    task_desc = {"uid": "task.idempotent-test"}

    engine.handle_component_cancellation(task_desc, fut)
    assert fut.cancelled()

    engine.handle_component_cancellation(task_desc, fut)  # must not raise

    await engine.shutdown()


@pytest.mark.asyncio
async def test_handle_component_cancellation_on_foreign_future():
    """A future that the engine did not create has no `_cancel_locally`; the
    cancellation handler must still cancel it."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "RUNNING"
    assert not hasattr(fut, "_cancel_locally")

    engine.handle_component_cancellation({"uid": "block.foreign-future"}, fut)

    assert fut.cancelled()
    assert fut.state == "CANCELLED"

    await engine.shutdown()

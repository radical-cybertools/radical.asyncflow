"""Tests for the public future.state attribute lifecycle."""

import asyncio
import sys

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
    """handle_component_failure must set state == 'FAILED' on the future."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "RUNNING"
    task_desc = {
        "uid": "task.fail-test",
        "exception": RuntimeError("boom"),
        "stderr": "boom",
    }

    engine.handle_component_failure(task_desc, fut)

    assert fut.state == "FAILED"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_future_state_transitions_to_cancelled():
    """handle_component_cancellation must set state == 'CANCELLED' on the future."""
    engine = await _make_engine()

    fut = asyncio.Future()
    fut.state = "RUNNING"
    fut.original_cancel = fut.cancel
    task_desc = {"uid": "task.cancel-test"}

    engine.handle_component_cancellation(task_desc, fut)

    assert fut.state == "CANCELLED"

    await engine.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("comp_type", ["task", "block"])
async def test_future_state_cancelled_before_registration(comp_type):
    """A future cancelled before its component registers must have state == 'CANCELLED',
    already set when its done callbacks run."""
    engine = await _make_engine()

    @engine.executable_task
    async def slow_command():
        await asyncio.sleep(0.01)  # the task registers after its body returns
        return "/bin/true"

    @engine.block
    async def some_block():
        return "ok"

    fut = slow_command() if comp_type == "task" else some_block()
    seen = []
    fut.add_done_callback(lambda f: seen.append(f.state))

    assert fut.cancel()
    await asyncio.sleep(0.05)  # let registration run

    assert fut.cancelled()
    assert fut.state == "CANCELLED"
    assert seen == ["CANCELLED"]

    await engine.shutdown()


@pytest.mark.asyncio
async def test_call_patches_no_cancel_on_the_future():
    """Calling a task or block must not patch `cancel` onto its future: a patch per call
    cost 25 MB and 29% more gen-0 collections at 40K tasks.

    The state set on cancellation before registration comes from the future's class.
    """
    engine = await _make_engine()

    @engine.executable_task
    async def slow_command():
        await asyncio.sleep(0.01)  # not registered when the call returns
        return "/bin/true"

    @engine.block
    async def some_block():
        return "ok"

    for fut in (slow_command(), some_block()):
        assert getattr(fut.cancel, "__func__", None) is type(fut).cancel
        assert not hasattr(fut, "original_cancel")

    await engine.shutdown()


@pytest.mark.asyncio
async def test_future_state_failed_registration():
    """A task whose registration fails, here an executable task that returns an empty
    command, must have state == 'FAILED'."""
    engine = await _make_engine()

    @engine.executable_task
    async def empty_command():
        return ""

    fut = empty_command()
    with pytest.raises(ValueError, match="non-empty command string"):
        await fut

    assert fut.state == "FAILED"

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


@pytest.mark.asyncio
async def test_block_future_state_cancelled_while_running():
    """A running block cancelled with cancel() must have state == 'CANCELLED', already
    set when its done callbacks run."""
    engine = await _make_engine()
    block_started = asyncio.Event()

    @engine.block
    async def long_block():
        block_started.set()
        await asyncio.sleep(10)

    block_fut = long_block()
    seen = []
    block_fut.add_done_callback(lambda f: seen.append(f.state))
    await block_started.wait()
    assert block_fut.state == "RUNNING"

    assert block_fut.cancel()
    await asyncio.sleep(0.05)

    assert block_fut.state == "CANCELLED"
    assert seen == ["CANCELLED"]

    await engine.shutdown()


@pytest.mark.skipif(
    sys.version_info < (3, 12), reason="asyncio.eager_task_factory needs Python 3.12"
)
@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True], ids=["done", "failed"])
async def test_block_future_state_under_eager_task_factory(fails):
    """Under an eager task factory, a block that does not suspend finishes inside
    create_task; its state must be DONE or FAILED, not RUNNING."""
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)
    engine = await _make_engine()

    @engine.block
    async def quick_block():
        if fails:
            raise ValueError("block error")
        return "ok"

    try:
        block_fut = quick_block()
        if fails:
            with pytest.raises(ValueError, match="block error"):
                await block_fut
            assert block_fut.state == "FAILED"
        else:
            assert await block_fut == "ok"
            assert block_fut.state == "DONE"
    finally:
        await engine.shutdown()
        loop.set_task_factory(previous_factory)

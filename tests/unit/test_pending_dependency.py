"""Dependents called with the future of a component that is not registered yet.

Executable and prompt tasks register only after their body returns. A dependent called
right after one of them, with its future, therefore reaches registration first, before
the future is recognisable as a dependency.
"""

import asyncio
import threading

import pytest
import pytest_asyncio

from radical.asyncflow import (
    LocalExecutionBackend,
    NoopExecutionBackend,
    WorkflowEngine,
)
from radical.asyncflow.errors import DependencyFailureError


@pytest_asyncio.fixture
async def flow():
    # Executables and functions run locally; prompts go to the Noop "llm" backend.
    backend = await LocalExecutionBackend()
    flow = await WorkflowEngine.create(
        backend=[backend, NoopExecutionBackend(name="llm")]
    )
    yield flow
    await flow.shutdown()


def _identity(decorator):
    @decorator
    async def consumer(x):
        return x

    return consumer


@pytest.mark.asyncio
async def test_dry_run_executable_dependency():
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def a():
        await asyncio.sleep(0.01)  # a registers after b is called
        return "echo hi"

    @engine.function_task
    async def b(x):
        return x

    fa = a()
    fb = b(fa)

    assert await asyncio.wait_for(fb, timeout=5) == "Dummy Output"
    deps = fb.task["metadata"]["dependencies"]
    assert [d["uid"] for d in deps] == [fa.task["uid"]]
    await engine.shutdown()


@pytest.mark.asyncio
async def test_executable_dependency(flow):
    @flow.executable_task
    async def a():
        await asyncio.sleep(0.01)
        return "echo hi"

    b = _identity(flow.function_task)

    assert await asyncio.wait_for(b(a()), timeout=5) == "hi\n"


@pytest.mark.asyncio
async def test_prompt_dependency(flow):
    @flow.prompt_task(backend="llm")
    async def ask():
        await asyncio.sleep(0.01)
        return "say hi"

    b = _identity(flow.function_task)

    assert await asyncio.wait_for(b(ask()), timeout=5) == "Dummy Prompt Output"


@pytest.mark.asyncio
async def test_keyword_dependency(flow):
    @flow.executable_task
    async def a():
        await asyncio.sleep(0.01)
        return "echo hi"

    b = _identity(flow.function_task)

    assert await asyncio.wait_for(b(x=a()), timeout=5) == "hi\n"


@pytest.mark.asyncio
async def test_block_dependent(flow):
    @flow.executable_task
    async def a():
        await asyncio.sleep(0.01)
        return "echo hi"

    blk = _identity(flow.block)

    assert await asyncio.wait_for(blk(a()), timeout=5) == "hi\n"


@pytest.mark.asyncio
async def test_failed_dependency_propagates(flow):
    @flow.executable_task
    async def a():
        await asyncio.sleep(0.01)
        return "ls /nonexistent-asyncflow-path"

    b = _identity(flow.function_task)

    fa = a()
    fb = b(fa)

    with pytest.raises(DependencyFailureError) as exc_info:
        await asyncio.wait_for(fb, timeout=5)
    assert exc_info.value.failed_dependencies == ["a"]
    assert exc_info.value.root_cause is fa.exception()


@pytest.mark.asyncio
async def test_cancelled_dependency_propagates(flow):
    @flow.executable_task
    async def a():
        await asyncio.sleep(0.01)
        return "echo hi"

    b = _identity(flow.function_task)

    fa = a()
    fb = b(fa)
    fa.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(fb, timeout=5)
    assert fb.cancelled()


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["", None])
async def test_failed_registration_propagates(flow, command):
    @flow.executable_task
    async def a():
        await asyncio.sleep(0.01)
        if command is None:
            raise ValueError("body failed")
        return command  # not a command, so a never registers

    b = _identity(flow.function_task)

    fa = a()
    fb = b(fa)

    with pytest.raises(DependencyFailureError) as exc_info:
        await asyncio.wait_for(fb, timeout=5)
    assert exc_info.value.failed_dependencies == ["a"]
    assert isinstance(fa.exception(), ValueError)
    assert exc_info.value.root_cause is fa.exception()


@pytest.mark.asyncio
async def test_cancelled_registration_propagates(flow):
    @flow.executable_task
    async def a():
        await asyncio.sleep(0.01)
        return "echo hi"

    b = _identity(flow.function_task)

    fa = a()
    fb = b(fa)
    fa._registration.cancel()  # a never registers, and its future stays pending

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(fb, timeout=5)
    assert fb.cancelled()
    assert not fa.done()


@pytest.mark.asyncio
async def test_dependent_waits_for_every_registration(flow):
    @flow.executable_task
    async def fails():
        await asyncio.sleep(0.01)
        return ""  # not a command, so fails never registers

    @flow.executable_task
    async def slow():
        await asyncio.sleep(5)
        return "echo hi"

    @flow.function_task
    async def both(x, y):
        return x, y

    fx = fails()
    fy = slow()
    fb = both(fx, fy)

    await asyncio.wait([fx], timeout=5)
    assert isinstance(fx.exception(), ValueError)
    assert not fb.done()  # still waiting for slow's registration

    # A cancelled dependency takes precedence over the failed one, as in run.
    fy._registration.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(fb, timeout=5)
    assert fb.cancelled()


def _fail_step_for(flow, monkeypatch, step, fut):
    """Makes the engine method `step` raise, instead of running, for fut's component."""
    original = getattr(flow, step)

    def fail_for_fut(*args, **kwargs):
        # Both steps take the uid, positionally or as task_id.
        if fut.uid in (*args, *kwargs.values()):
            raise RuntimeError(f"{step} failed")
        return original(*args, **kwargs)

    monkeypatch.setattr(flow, step, fail_for_fut)


@pytest.mark.asyncio
@pytest.mark.parametrize("step", ["_update_dependency_tracking", "_emit"])
@pytest.mark.parametrize("late", [False, True], ids=["called-before", "called-after"])
async def test_registration_failing_after_storing_leaves_no_trace(
    flow, monkeypatch, step, late
):
    """A component whose registration fails after it was stored is not registered.

    Its future kept the task attribute, so a dependent registered against it and waited
    for a completion that never came.
    """

    @flow.function_task
    async def a():
        return 1

    b = _identity(flow.function_task)

    fa = a()
    _fail_step_for(flow, monkeypatch, step, fa)
    if late:
        await asyncio.wait([fa], timeout=5)
    fb = b(fa)

    with pytest.raises(DependencyFailureError) as exc_info:
        await asyncio.wait_for(fb, timeout=5)
    assert exc_info.value.failed_dependencies == ["a"]
    assert isinstance(fa.exception(), RuntimeError)
    assert exc_info.value.root_cause is fa.exception()
    assert not hasattr(fa, "task")
    assert not hasattr(fa, "original_cancel")
    for records in (
        flow.components,
        flow.dependencies,
        flow._dependency_count,
        flow._ready_queue,
    ):
        assert fa.uid not in records
    # The run loop still runs tasks: it did not meet a's uid in the ready queue.
    assert await asyncio.wait_for(b(2), timeout=5) == 2


@pytest.mark.asyncio
async def test_registration_failing_after_storing_untracks_dependencies(
    flow, monkeypatch
):
    """Undoing a registration also undoes the tracking of its pending dependencies."""
    release = threading.Event()

    @flow.function_task
    async def upstream():
        release.wait(timeout=5)

    a = _identity(flow.function_task)

    fu = upstream()
    fa = a(fu)
    _fail_step_for(flow, monkeypatch, "_emit", fa)
    try:
        await asyncio.wait([fa], timeout=5)
        assert isinstance(fa.exception(), RuntimeError)
        assert fa.uid not in flow._dependency_count
        assert not any(fa.uid in deps for deps in flow._dependents_map.values())
    finally:
        release.set()
    assert await asyncio.wait_for(fu, timeout=5) is None

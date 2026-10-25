"""Dependents called with the future of a component that is not registered yet.

Executable and prompt tasks register only after their body returns. A dependent called
right after one of them, with its future, therefore reaches registration first, before
the future is recognisable as a dependency.
"""

import asyncio

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


@pytest.mark.asyncio
async def test_shutdown_cancels_pending_registrations():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())
    started = asyncio.Event()

    @engine.executable_task
    async def slow():
        started.set()
        await asyncio.sleep(60)  # still running at shutdown, so slow never registers
        return "echo hi"

    b = _identity(engine.function_task)

    fa = slow()
    fb = b(fa)  # waits for slow's registration
    await asyncio.wait_for(started.wait(), timeout=5)
    await engine.shutdown()

    for fut in (fa, fb):
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(fut, timeout=5)
        assert fut.cancelled()


@pytest.mark.asyncio
async def test_body_finishing_after_shutdown_does_not_register():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())
    started = asyncio.Event()
    release = asyncio.Event()

    @engine.executable_task
    async def late():
        started.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                pass  # ignores shutdown's cancellation and finishes after it
        return "echo hi"

    fut = late()
    registration = fut._registration
    await asyncio.wait_for(started.wait(), timeout=5)
    await engine.shutdown()
    release.set()
    await asyncio.wait([registration], timeout=5)

    assert registration.done()
    assert fut.cancelled()
    assert fut.uid not in engine.components

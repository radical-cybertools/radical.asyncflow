"""Dependents called with the future of a component that is not registered yet.

Executable and prompt tasks register only after their body returns. A dependent called
right after one of them, with its future, therefore reaches registration first, before
the future is recognisable as a dependency.
"""

import asyncio
import sys

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
@pytest.mark.parametrize("outcome", ["failed", "cancelled"])
async def test_dependent_state_goes_through_component_handlers(
    flow, monkeypatch, outcome
):
    """A dependent of an unregistered component changes state through the handlers.

    Every other component does, so a second path would drift from them.
    """
    calls = []
    for name in ("handle_component_failure", "handle_component_cancellation"):
        original = getattr(flow, name)

        def record(component, fut, *args, _original=original, _name=name):
            calls.append((_name, component["uid"], fut))
            return _original(component, fut, *args)

        monkeypatch.setattr(flow, name, record)

    @flow.executable_task
    async def a():
        await asyncio.sleep(0.01)
        return ""  # not a command, so a never registers

    b = _identity(flow.function_task)

    fa = a()
    fb = b(fa)
    if outcome == "cancelled":
        fa._registration.cancel()  # a's future stays pending

    await asyncio.wait([fb], timeout=5)
    if outcome == "failed":
        assert isinstance(fa.exception(), ValueError)
        assert ("handle_component_failure", fb.uid, fb) in calls
        assert fb.state == "FAILED"
    else:
        assert ("handle_component_cancellation", fb.uid, fb) in calls
        assert fb.state == "CANCELLED"


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.version_info < (3, 12), reason="asyncio.eager_task_factory is new in 3.12"
)
async def test_registration_task_dropped_under_eager_task_factory():
    """A component registered inside the call keeps no registration task."""
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        engine = await WorkflowEngine.create(dry_run=True)

        @engine.function_task
        async def a():
            return 1

        @engine.executable_task
        async def bad():
            return ""  # not a command: registration fails inside the call

        b = _identity(engine.function_task)

        fa = a()
        assert hasattr(fa, "task")  # registered inside the call
        assert fa._registration is None

        fbad = bad()
        assert fbad._registration.done()  # kept: its dependents read its name
        with pytest.raises(DependencyFailureError) as exc_info:
            await asyncio.wait_for(b(fbad), timeout=5)
        assert exc_info.value.failed_dependencies == ["bad"]
        assert await asyncio.wait_for(b(fa), timeout=5) == "Dummy Output"
        await engine.shutdown()
    finally:
        loop.set_task_factory(None)

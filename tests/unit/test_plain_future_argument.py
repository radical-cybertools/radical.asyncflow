"""Plain asyncio futures passed as arguments to a task or block.

Only the futures returned by this engine's tasks and blocks are dependencies. A plain
asyncio Future or Task has no component to wait for, so calling a task or block with one
raises a TypeError that names the argument.
"""

import asyncio

import pytest
import pytest_asyncio

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine


@pytest_asyncio.fixture
async def flow():
    backend = await LocalExecutionBackend()
    flow = await WorkflowEngine.create(backend=backend)
    yield flow
    await flow.shutdown()


async def _value():
    await asyncio.sleep(0.01)
    return 42


def _plain_task():
    return asyncio.ensure_future(_value())


def _plain_future():
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    loop.call_later(0.01, fut.set_result, 42)
    return fut


@pytest.mark.asyncio
@pytest.mark.parametrize("decorator", ["function_task", "block"])
@pytest.mark.parametrize(
    "make, kind",
    [(_plain_task, "Task"), (_plain_future, "Future")],
    ids=["task", "future"],
)
@pytest.mark.parametrize("keyword", [False, True], ids=["positional", "keyword"])
async def test_plain_future_argument_is_rejected(flow, decorator, make, kind, keyword):
    @getattr(flow, decorator)
    async def consumer(x):
        return x

    arg = make()
    with pytest.raises(
        TypeError, match=f"argument 'x' is a plain asyncio {kind}, not the future of"
    ):
        consumer(x=arg) if keyword else consumer(arg)

    assert not flow.components
    # The rejected future is left alone, and the engine still runs components.
    assert await asyncio.wait_for(arg, timeout=5) == 42
    assert await asyncio.wait_for(consumer(1), timeout=5) == 1


@pytest.mark.asyncio
async def test_plain_future_beyond_named_parameters_is_rejected(flow):
    @flow.function_task
    async def consumer(*xs):
        return xs

    arg = _plain_task()
    with pytest.raises(TypeError, match="positional argument 2 is a plain asyncio"):
        consumer(1, arg)

    assert await asyncio.wait_for(arg, timeout=5) == 42

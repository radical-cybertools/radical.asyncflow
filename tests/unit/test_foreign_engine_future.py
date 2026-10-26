"""Futures of another engine passed as arguments to a task or block.

Only the futures returned by this engine's tasks and blocks are dependencies. The future
of another engine's task or block names a component this engine never resolves, so
calling a task or block with one raises a TypeError that names the argument instead of
waiting forever.
"""

import asyncio
import re

import pytest
import pytest_asyncio

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine


@pytest_asyncio.fixture
async def flows():
    f1 = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    f2 = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    yield f1, f2
    await f2.shutdown()
    await f1.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("producer_type", ["function_task", "block"])
@pytest.mark.parametrize("consumer_type", ["function_task", "block"])
@pytest.mark.parametrize("registered", [True, False], ids=["registered", "pending"])
@pytest.mark.parametrize("keyword", [False, True], ids=["positional", "keyword"])
async def test_foreign_engine_future_is_rejected(
    flows, producer_type, consumer_type, registered, keyword
):
    f1, f2 = flows

    @f1.executable_task
    async def gate():
        await asyncio.sleep(0.01)  # gate registers after consumer is called
        return "true"

    @getattr(f1, producer_type)
    async def producer(*_):
        return 42

    @getattr(f2, consumer_type)
    async def consumer(x):
        return x

    if registered:
        arg = producer()
        assert await asyncio.wait_for(arg, timeout=5) == 42
    else:
        # producer waits for gate to register. Without a pending dependency it would
        # register inside the call with an eager task factory.
        arg = producer(gate())
        assert not hasattr(arg, "task") and not hasattr(arg, "block")

    # Without the check, consumer waits for a component f2 never resolves.
    with pytest.raises(
        TypeError,
        match=f"argument 'x' is the future of {re.escape(arg.uid)} of another "
        "WorkflowEngine",
    ):
        await asyncio.wait_for(consumer(x=arg) if keyword else consumer(arg), timeout=3)

    assert not f2.components
    # The rejected future is left alone, and both engines still run components.
    assert await asyncio.wait_for(arg, timeout=5) == 42
    assert await asyncio.wait_for(consumer(1), timeout=5) == 1
    assert await asyncio.wait_for(producer(), timeout=5) == 42

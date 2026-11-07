"""A backend that refuses a submit fails only its own tasks.

When ``submit_tasks`` raises (for example because the backend's remote
endpoint is gone), the engine must not shut down: the tasks of that batch
fail with the backend's error, their dependents fail with a dependency
error, and every other backend keeps running.
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


class RefusingBackend(NoopExecutionBackend):
    """Accepts nothing: every submit raises, as a lost endpoint does."""

    async def submit_tasks(self, tasks):
        raise RuntimeError("HTTP 504 -- submit 1 task(s) -- endpoint lost")


@pytest_asyncio.fixture
async def flow():
    local = await LocalExecutionBackend()
    flow = await WorkflowEngine.create(backend=[local, RefusingBackend(name="doomed")])
    yield flow
    await flow.shutdown()


@pytest.mark.asyncio
async def test_refused_submit_fails_only_that_backends_tasks(flow):
    @flow.function_task(backend="doomed")
    async def learn():
        return "never"

    @flow.function_task
    async def infer(x):
        return x * 2

    doomed = learn()
    healthy = infer(21)

    with pytest.raises(RuntimeError, match="endpoint lost"):
        await asyncio.wait_for(doomed, timeout=5)

    assert await asyncio.wait_for(healthy, timeout=5) == 42


@pytest.mark.asyncio
async def test_engine_keeps_running_after_a_refused_submit(flow):
    @flow.function_task(backend="doomed")
    async def learn():
        return "never"

    @flow.function_task
    async def infer(x):
        return x + 1

    with pytest.raises(RuntimeError):
        await asyncio.wait_for(learn(), timeout=5)

    # later work on the healthy backend still runs, several times over
    results = await asyncio.wait_for(
        asyncio.gather(*(infer(i) for i in range(10))), timeout=10
    )
    assert results == list(range(1, 11))


@pytest.mark.asyncio
async def test_dependent_of_a_refused_task_fails_as_a_dependency(flow):
    @flow.function_task(backend="doomed")
    async def learn():
        return "never"

    @flow.function_task
    async def use(model):
        return model

    with pytest.raises(DependencyFailureError):
        await asyncio.wait_for(use(learn()), timeout=5)

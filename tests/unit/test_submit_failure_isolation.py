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


class SlowBackend(NoopExecutionBackend):
    """Holds the run loop in submit_tasks, as a backend waiting on remote I/O does."""

    async def submit_tasks(self, tasks):
        await asyncio.sleep(0.1)
        await super().submit_tasks(tasks)


@pytest.mark.asyncio
async def test_slow_submit_logs_a_warning(monkeypatch, caplog):
    import radical.asyncflow.workflow_manager as wm

    monkeypatch.setattr(wm, "SLOW_SUBMIT_WARNING", 0.05)
    flow = await WorkflowEngine.create(backend=SlowBackend(name="slow"))
    try:

        @flow.function_task
        async def task():
            return 1

        with caplog.at_level("WARNING", logger="radical.asyncflow.workflow_manager"):
            await asyncio.wait_for(task(), timeout=5)
            await asyncio.sleep(0.05)  # the warning follows the task's completion
        assert any(
            "held the run loop" in r.getMessage() and "slow" in r.getMessage()
            for r in caplog.records
        )
    finally:
        await flow.shutdown()


class SlowRefusingBackend(NoopExecutionBackend):
    async def submit_tasks(self, tasks):
        await asyncio.sleep(0.1)
        raise RuntimeError("endpoint lost")


def _slow_warnings(caplog):
    return [
        r.getMessage() for r in caplog.records if "held the run loop" in r.getMessage()
    ]


@pytest.mark.asyncio
async def test_slow_warning_names_only_the_slowest_backend_once(monkeypatch, caplog):
    import radical.asyncflow.workflow_manager as wm

    monkeypatch.setattr(wm, "SLOW_SUBMIT_WARNING", 0.05)
    flow = await WorkflowEngine.create(
        backend=[NoopExecutionBackend(name="fast"), SlowBackend(name="slow")]
    )
    try:

        @flow.function_task(backend="slow")
        async def on_slow():
            return 1

        @flow.function_task(backend="fast")
        async def on_fast():
            return 2

        with caplog.at_level("WARNING", logger="radical.asyncflow.workflow_manager"):
            for _ in range(3):
                await asyncio.wait_for(asyncio.gather(on_slow(), on_fast()), timeout=5)
                await asyncio.sleep(0.05)
        msgs = _slow_warnings(caplog)
        assert len(msgs) == 1  # rate limited per backend
        assert "Backend slow" in msgs[0]
        assert not any("Backend fast" in m for m in msgs)
    finally:
        await flow.shutdown()


@pytest.mark.asyncio
async def test_refused_slow_submit_is_not_reported_as_slow(monkeypatch, caplog):
    import radical.asyncflow.workflow_manager as wm

    monkeypatch.setattr(wm, "SLOW_SUBMIT_WARNING", 0.05)
    flow = await WorkflowEngine.create(backend=SlowRefusingBackend(name="gone"))
    try:

        @flow.function_task
        async def task():
            return 1

        with caplog.at_level("WARNING", logger="radical.asyncflow.workflow_manager"):
            with pytest.raises(RuntimeError, match="endpoint lost"):
                await asyncio.wait_for(task(), timeout=5)
            await asyncio.sleep(0.05)
        assert _slow_warnings(caplog) == []
    finally:
        await flow.shutdown()

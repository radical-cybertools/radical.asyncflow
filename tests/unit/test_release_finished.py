"""Finished components are released, so a long-lived engine does not grow.

Once a task or block is done and its dependents were notified, the engine forgets it.
Its future stays usable as a dependency, and a late report about it is recognised as one
about a released task.
"""

import asyncio
import gc
import logging
import weakref

import pytest

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine
from radical.asyncflow.errors import DependencyFailureError


async def _released(engine, uid, timeout=5.0):
    """Wait until the run loop released *uid* (no fixed sleeps)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not engine._is_released(uid):
        assert asyncio.get_running_loop().time() < deadline, f"{uid} not released"
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_many_tasks_keep_the_engine_bounded():
    backend = await LocalExecutionBackend()
    engine = await WorkflowEngine.create(backend=backend)
    try:

        @engine.function_task
        async def work(x):
            return x

        for i in range(300):
            assert await work(i) == i
        await asyncio.gather(*(work(i) for i in range(300)))

        last = work(0)
        await last
        await _released(engine, last.task["uid"])
        gc.collect()
        for name in ("components", "dependencies", "resolved", "running"):
            assert len(getattr(engine, name)) <= 1, name
        assert len(engine._dependency_count) <= 1
        assert len(engine._dependents_map) <= 1
        assert len(engine._dependency_futures) <= 1
        # the backend drops a task when it finished (also under --eager-tasks)
        assert not backend.tasks

        # nothing in the engine keeps a finished call's arguments: the whole
        # entry goes, description (with its args) and dependency list alike
        class Payload:
            pass

        payload = Payload()
        ref = weakref.ref(payload)
        fut = work(payload)
        await fut
        uid = fut.task["uid"]
        del payload, fut
        await _released(engine, uid)
        # the run loop's locals hold its latest batch until the next one
        await work(0)
        gc.collect()
        assert ref() is None
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_released_future_still_works_as_a_dependency():
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    try:

        @engine.function_task
        async def produce():
            return 20

        @engine.function_task
        async def consume(x):
            return x + 1

        first = produce()
        await first
        await _released(engine, first.task["uid"])

        assert await consume(first) == 21
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_released_failed_dependency_still_fails_its_dependent():
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    try:

        @engine.function_task
        async def broken():
            raise ValueError("boom")

        @engine.function_task
        async def consume(x):
            return x

        first = broken()
        with pytest.raises(ValueError):
            await first
        await _released(engine, first.task["uid"])

        with pytest.raises(DependencyFailureError):
            await consume(first)
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_pending_dependent_reads_a_released_dependency():
    """A dependent registered before its first dependency finished, and still waiting
    for its second, gets the released first one's result."""
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    try:
        gate = asyncio.Event()

        @engine.function_task
        async def fast():
            return 1

        @engine.block
        async def slow():
            await gate.wait()
            return 2

        @engine.function_task
        async def add(a, b):
            return a + b

        a, b = fast(), slow()
        total = add(a, b)
        await a
        await _released(engine, a.task["uid"])
        assert not total.done()

        gate.set()
        assert await asyncio.wait_for(total, 5) == 3
        await _released(engine, b.block["uid"])
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_late_report_on_a_released_task_names_the_release(caplog):
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    other = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    try:

        @engine.function_task
        async def work():
            return 1

        fut = work()
        await fut
        uid = fut.task["uid"]
        await _released(engine, uid)
        assert not other._is_released(uid)

        # a late state report for a released task is not "unknown"
        with caplog.at_level(
            logging.DEBUG, logger="radical.asyncflow.workflow_manager"
        ):
            engine.task_callbacks(dict(fut.task), "DONE")
        assert any("was released" in r.getMessage() for r in caplog.records)
        assert not any("unknown task" in r.getMessage() for r in caplog.records)
    finally:
        await engine.shutdown()
        await other.shutdown()


@pytest.mark.asyncio
async def test_a_service_is_not_released():
    """A service's future resolves when it is ready; the backend reports on it later, so
    the engine keeps it."""
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    try:

        @engine.function_task(service=True)
        async def service():
            return "ready"

        @engine.function_task
        async def work():
            return 1

        svc = service()
        await svc
        fut = work()
        await fut
        await _released(engine, fut.task["uid"])
        assert svc.task["uid"] in engine.components
    finally:
        await engine.shutdown()

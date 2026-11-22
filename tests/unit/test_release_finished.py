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


def _record_handlers(backend) -> list:
    """Weak references to the handler Tasks *backend* creates, as it submits them."""
    handlers = []
    submit_tasks = backend.submit_tasks

    async def recording(tasks):
        submitted = await submit_tasks(tasks)
        handlers.extend(weakref.ref(task) for task in submitted)
        return submitted

    backend.submit_tasks = recording
    return handlers


@pytest.mark.asyncio
async def test_kept_futures_do_not_keep_the_backends_handlers():
    """A caller that keeps its futures keeps their descriptions, not the backend's
    handler Tasks.

    On Python 3.11 to 3.13, asyncio.run walks every Task alive in the process, so a
    handler that a kept future still reached made every later function task slower.
    """
    backend = await LocalExecutionBackend()
    handlers = _record_handlers(backend)
    engine = await WorkflowEngine.create(backend=backend)
    try:

        @engine.function_task
        async def work(x):
            return x

        futs = [work(i) for i in range(300)]
        assert await asyncio.wait_for(asyncio.gather(*futs), 10) == list(range(300))
        await _released(engine, futs[-1].task["uid"])
        gc.collect()

        assert len(handlers) == 300
        assert not [h for h in handlers if h() is not None]
        assert not [f.uid for f in futs if "future" in f.task]
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_a_dropped_chain_is_freed():
    """Once the caller lets go of a finished chain x = step(x), the engine keeps none
    of it.

    Each description reaches the ones before it through its dependencies, so any
    reference left to one link, such as a run loop variable, keeps the whole chain.
    """
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    try:

        class Payload:
            pass

        @engine.function_task
        async def first(payload):
            return 0

        @engine.function_task
        async def step(x):
            return x + 1

        payload = Payload()
        ref = weakref.ref(payload)
        x = first(payload)
        del payload
        for _ in range(20):
            x = step(x)
            await asyncio.wait_for(x, 5)
        assert x.result() == 20
        uid = x.task["uid"]
        del x
        await _released(engine, uid)
        await asyncio.sleep(0.05)  # the run loop finishes its pass and goes idle
        gc.collect()
        assert ref() is None
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_dependents_leave_no_dependency_records():
    """A fan-in and a chain leave nothing in the dependency records once released."""
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    try:

        @engine.function_task
        async def work(x):
            return x

        @engine.function_task
        async def add(a, b):
            return a + b

        @engine.function_task
        async def inc(x):
            return x + 1

        leaves = [work(i) for i in range(100)]
        sums = [add(a, b) for a, b in zip(leaves[::2], leaves[1::2], strict=True)]
        x = work(0)
        for _ in range(50):
            x = inc(x)
        results = await asyncio.wait_for(asyncio.gather(*sums, x), 10)
        assert results == [2 * i + 1 for i in range(0, 100, 2)] + [50]
        for fut in (*leaves, *sums, x):
            await _released(engine, fut.uid)

        assert not engine.dependencies
        assert not engine._dependency_futures
        assert not engine._dependency_count
        assert not engine._dependents_map
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_a_task_cancelled_during_its_submit_leaves_no_telemetry_timing():
    """A task cancelled while the backend accepts it is released before the backend
    reports on it, and the report is then ignored: the release drops its telemetry
    timing entries."""
    pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")

    class SlowSubmit(LocalExecutionBackend):
        async def submit_tasks(self, tasks):
            await asyncio.sleep(0.05)  # the caller cancels meanwhile
            return await super().submit_tasks(tasks)

    engine = await WorkflowEngine.create(backend=await SlowSubmit())
    telemetry = await engine.start_telemetry()
    try:

        @engine.function_task
        async def work():
            await asyncio.sleep(0.3)  # reports after the release
            return 1

        fut = work()
        while fut.uid not in engine._task_submit_times:
            await asyncio.sleep(0.005)
        fut.cancel()
        await _released(engine, fut.uid)
        await asyncio.sleep(0.5)  # the backend's late report arrives, and is ignored

        assert fut.cancelled()
        assert fut.uid not in engine._task_submit_times
        assert fut.uid not in engine._task_start_times
    finally:
        await telemetry.stop()
        await engine.shutdown()


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
        await asyncio.sleep(0.05)  # the run loop finishes its pass and goes idle
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

        assert await asyncio.wait_for(consume(first), 5) == 21
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
            await asyncio.wait_for(consume(first), 5)
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

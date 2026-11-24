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

from radical.asyncflow import (
    LocalExecutionBackend,
    NoopExecutionBackend,
    WorkflowEngine,
)
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
@pytest.mark.parametrize("cancel_all", [False, True])
async def test_a_task_cancelled_before_it_starts_is_released(cancel_all):
    """A handler cancelled before its first step never runs its own cleanup."""
    backend = await LocalExecutionBackend()
    backend.register_callback(lambda task, state: None)

    async def work():
        await asyncio.sleep(10)

    task = {"uid": "task.000001", "function": work, "args": (), "kwargs": {}}
    try:
        await backend.submit_tasks([task])  # no await before the cancel
        if cancel_all:
            assert await backend.cancel_all_tasks() == 1
        else:
            assert await backend.cancel_task(task["uid"])
        assert backend.tasks == {}
        assert "future" not in task
    finally:
        await backend.shutdown()


@pytest.mark.asyncio
async def test_a_dropped_chain_is_freed():
    """A finished chain x = step(x) that the caller lets go of is freed.

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


class _HeldBackend(NoopExecutionBackend):
    """Parks tasks as RUNNING, and takes its time to return from submit_tasks."""

    def __init__(self, name, submit_time=0.0):
        super().__init__(name=name)
        self.submit_time = submit_time
        self.parked = {}

    async def submit_tasks(self, tasks):
        for task in tasks:
            self.parked[task["uid"]] = task
            self._callback_func(task, "RUNNING")
        await asyncio.sleep(self.submit_time)

    def report(self, uid, state):
        self._callback_func({**self.parked[uid], "return_value": 1}, state)


def _record_events(engine, monkeypatch):
    events = []

    def emit(event_name, *, task_id, **kwargs):
        events.append((event_name, task_id, kwargs))

    # any object will do, so the test runs without RHAPSODY's TelemetryManager
    monkeypatch.setattr(engine, "_telemetry", object())
    monkeypatch.setattr(engine, "_emit", emit)
    return events


def _ends(events, uid):
    terminal = ("TaskCompleted", "TaskFailed", "TaskCanceled")
    return [name for name, task_id, _ in events if task_id == uid and name in terminal]


@pytest.mark.asyncio
@pytest.mark.parametrize("report", ["DONE", "FAILED", "CANCELED"])
@pytest.mark.parametrize("late", [False, True])
@pytest.mark.parametrize("several", [False, True])
async def test_a_task_cancelled_during_its_submit_emits_one_terminal_event(
    monkeypatch, several, late, report
):
    """The backend's late report on a released task is ignored, so the release emits the
    terminal event; an early report emits it itself.

    With several backends, a slow submit to one leaves the tasks that already run on
    another in this state.
    """
    if several:
        backend, slow = _HeldBackend("fast"), _HeldBackend("slow", 0.2)
        engine = await WorkflowEngine.create(backend=[backend, slow])
    else:
        backend = _HeldBackend("only", 0.2)
        engine = await WorkflowEngine.create(backend=backend)
    events = _record_events(engine, monkeypatch)
    try:

        @engine.function_task
        async def work():
            return 1

        @engine.function_task(backend="slow" if several else None)
        async def other():
            return 1

        fut = work()
        if several:
            other()
        deadline = asyncio.get_running_loop().time() + 5
        while fut.uid not in backend.parked:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.005)
        fut.cancel()
        assert fut.cancelled()
        if not late:
            backend.report(fut.uid, report)
        await _released(engine, fut.uid)
        if late:
            backend.report(fut.uid, report)

        ends = _ends(events, fut.uid)
        assert len(ends) == 1
        if late:
            assert ends == ["TaskCanceled"]
        assert fut.uid not in engine._task_submit_times
        assert fut.uid not in engine._task_start_times
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_a_reported_end_is_emitted_once(monkeypatch):
    """The release adds no event for a task whose end the backend reported, nor for one
    that was never submitted."""
    backend = _HeldBackend("only")
    engine = await WorkflowEngine.create(backend=backend)
    events = _record_events(engine, monkeypatch)
    try:

        @engine.function_task
        async def work(*args):
            return 1

        done, failed = work(), work()
        dependent = work(failed)
        while not {done.uid, failed.uid} <= engine.running:
            await asyncio.sleep(0.005)
        backend.report(done.uid, "DONE")
        backend.parked[failed.uid]["exception"] = ValueError("boom")
        backend.report(failed.uid, "FAILED")
        with pytest.raises(DependencyFailureError):
            await asyncio.wait_for(dependent, 5)
        for fut in (done, failed, dependent):
            await _released(engine, fut.uid)

        assert _ends(events, done.uid) == ["TaskCompleted"]
        assert _ends(events, failed.uid) == ["TaskFailed"]
        assert _ends(events, dependent.uid) == []
    finally:
        await engine.shutdown()


@pytest.mark.asyncio
async def test_a_kept_future_names_its_dependencies_without_keeping_them():
    """A caller that keeps only the newest future of a chain x = step(x) does not keep
    the arguments of the earlier tasks; the description still names its dependencies."""
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
        for _ in range(5):
            previous = x.uid
            x = step(x)
            await asyncio.wait_for(x, 5)
        await _released(engine, x.uid)
        await asyncio.sleep(0.05)  # the run loop finishes its pass and goes idle
        gc.collect()

        assert ref() is None
        assert x.task["metadata"]["dependencies"] == [{"uid": previous, "name": "step"}]
    finally:
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

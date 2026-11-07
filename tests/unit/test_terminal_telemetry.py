"""Tests that every component with TaskCreated gets one terminal telemetry event, and
that shutdown stops telemetry after those events and before the backends."""

import asyncio
import contextlib
import logging

import pytest

from radical.asyncflow import (
    LocalExecutionBackend,
    NoopExecutionBackend,
    WorkflowEngine,
)

TERMINAL = ("TaskCompleted", "TaskFailed", "TaskCanceled")


class _Recorder:
    """Stand-in for RHAPSODY's TelemetryManager: records events and stop()."""

    _session_id = "session"

    def __init__(self, log, fail_stop=False):
        self.log = log
        self.fail_stop = fail_stop

    def emit(self, event):
        name, kwargs = event
        self.log.append((name, kwargs["task_id"], kwargs))

    def span_scope(self, *args, **kwargs):
        return contextlib.nullcontext()

    async def stop(self):
        self.log.append(("stop",))
        if self.fail_stop:
            raise RuntimeError("stop failed")


def _record_telemetry(engine, log, fail_stop=False):
    engine._telemetry = _Recorder(log, fail_stop)
    engine._tel_make_event = lambda cls, **kwargs: (cls, kwargs)
    engine._tel_events = {
        name: name
        for name in (
            "TaskCreated",
            "TaskQueued",
            "asyncflow.TaskResolved",
            "TaskSubmitted",
            "TaskStarted",
            *TERMINAL,
        )
    }


def _terminal(log, uid):
    return [entry for entry in log if entry[0] in TERMINAL and entry[1] == uid]


class ParkingBackend(NoopExecutionBackend):
    """Parks submitted tasks as RUNNING; logs its shutdown.

    With `report_at_shutdown`, it reports CANCELED for the parked tasks when it shuts
    down, as rhapsody's Dask backend does.
    """

    def __init__(self, log, report_at_shutdown=False):
        super().__init__()
        self.log = log
        self.parked = {}
        self.report_at_shutdown = report_at_shutdown

    async def submit_tasks(self, tasks):
        for t in tasks:
            self.parked[t["uid"]] = t
            self._callback_func(t, "RUNNING")

    async def shutdown(self):
        self.log.append(("backend shutdown",))
        if self.report_at_shutdown:
            for t in self.parked.values():
                self._callback_func(t, "CANCELED")


async def _wait_running(engine, fut):
    for _ in range(500):
        if hasattr(fut, "task") and fut.uid in engine.running:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("task never ran")


async def _settle(*futs):
    await asyncio.wait_for(asyncio.wait(futs), timeout=10)


async def test_shutdown_emits_canceled_then_stops_telemetry():
    log = []
    engine = await WorkflowEngine.create(backend=ParkingBackend(log))
    _record_telemetry(engine, log)
    gate = asyncio.Event()

    @engine.function_task
    async def slow():
        return 1

    @engine.function_task
    async def dependent(x):
        return 2

    @engine.executable_task
    async def unregistered():
        await gate.wait()
        return "true"

    running = slow()
    pending = dependent(running)
    never = unregistered()
    await _wait_running(engine, running)
    await engine.shutdown()

    assert [e[0] for e in _terminal(log, running.uid)] == ["TaskCanceled"]
    assert [e[0] for e in _terminal(log, pending.uid)] == ["TaskCanceled"]
    # It never registered, so it never emitted TaskCreated either.
    assert [e for e in log if len(e) > 1 and e[1] == never.uid] == []
    names = [e[0] for e in log]
    assert names.count("stop") == 1
    last_event = max(i for i, e in enumerate(log) if e[0] in TERMINAL)
    assert last_event < names.index("stop") < names.index("backend shutdown")
    assert engine._telemetry is None


async def test_shutdown_after_run_loop_ended_still_emits_canceled():
    """With the run loop already gone, shutdown does not yield before stop()."""
    log = []
    engine = await WorkflowEngine.create(backend=ParkingBackend(log))
    _record_telemetry(engine, log)

    @engine.function_task
    async def slow():
        return 1

    fut = slow()
    await _wait_running(engine, fut)
    engine._run_task.cancel()
    await asyncio.wait([engine._run_task])
    await engine.shutdown()
    assert [e[0] for e in _terminal(log, fut.uid)] == ["TaskCanceled"]


async def test_telemetry_started_after_the_call_still_emits_terminal():
    log = []
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    gate = asyncio.Event()
    try:

        @engine.executable_task
        async def later():
            await gate.wait()
            return "true"

        fut = later()
        # Telemetry starts while the task's body runs, before it registers
        _record_telemetry(engine, log)
        gate.set()
        await _settle(fut)
        await asyncio.sleep(0)
        assert "TaskCreated" in [e[0] for e in log if e[1] == fut.uid]
        assert [e[0] for e in _terminal(log, fut.uid)] == ["TaskCompleted"]
    finally:
        await engine.shutdown()


async def test_backend_report_after_shutdown_cancel_emits_nothing():
    log = []
    backend = ParkingBackend(log, report_at_shutdown=True)
    engine = await WorkflowEngine.create(backend=backend)
    _record_telemetry(engine, log)

    @engine.function_task
    async def slow():
        return 1

    fut = slow()
    await _wait_running(engine, fut)
    await engine.shutdown()
    assert [e[0] for e in _terminal(log, fut.uid)] == ["TaskCanceled"]


async def test_backend_report_after_outside_resolution_emits_once():
    log = []
    backend = ParkingBackend(log)
    engine = await WorkflowEngine.create(backend=backend)
    _record_telemetry(engine, log)
    try:

        @engine.function_task
        async def slow():
            return 1

        fut = slow()
        await _wait_running(engine, fut)
        asyncio.Future.set_result(fut, "outside")
        await asyncio.sleep(0)
        backend._callback_func(backend.parked[fut.uid], "DONE")
        await asyncio.sleep(0)
        assert [e[0] for e in _terminal(log, fut.uid)] == ["TaskCompleted"]
    finally:
        await engine.shutdown()


async def test_dependency_failure_emits_task_failed():
    log = []
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    _record_telemetry(engine, log)
    try:

        @engine.function_task
        async def bad():
            raise ValueError("expected")

        @engine.function_task
        async def dependent(x):
            return x

        b = bad()
        d = dependent(b)
        await _settle(b, d)
        await asyncio.sleep(0)
        [(_, _, failed)] = _terminal(log, b.uid)
        [(name, _, dep_failed)] = _terminal(log, d.uid)
        assert failed["error_type"] == type(b.exception()).__name__
        assert name == "TaskFailed"
        assert dep_failed["error_type"] == type(d.exception()).__name__
    finally:
        await engine.shutdown()


async def test_cancel_of_pending_task_emits_for_it_and_its_dependent():
    log = []
    engine = await WorkflowEngine.create(backend=ParkingBackend(log))
    _record_telemetry(engine, log)
    try:

        @engine.function_task
        async def slow():
            return 1

        @engine.function_task
        async def dependent(x):
            return 2

        running = slow()
        pending = dependent(running)
        downstream = dependent(pending)
        await _wait_running(engine, running)
        assert pending.cancel() is True
        await _settle(pending, downstream)
        await asyncio.sleep(0)
        assert [e[0] for e in _terminal(log, pending.uid)] == ["TaskCanceled"]
        assert [e[0] for e in _terminal(log, downstream.uid)] == ["TaskCanceled"]
        assert _terminal(log, running.uid) == []
    finally:
        await engine.shutdown()


@pytest.mark.parametrize(
    "outcome, event", [("done", "TaskCompleted"), ("fail", "TaskFailed")]
)
async def test_block_emits_terminal_event(outcome, event):
    log = []
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    _record_telemetry(engine, log)
    try:

        @engine.block
        async def blk():
            if outcome == "fail":
                raise ValueError("expected")
            return 1

        fut = blk()
        await _settle(fut)
        await asyncio.sleep(0)
        assert [e[0] for e in _terminal(log, fut.uid)] == [event]
        assert fut.exception() is None or outcome == "fail"
    finally:
        await engine.shutdown()


async def test_cancelled_block_emits_task_canceled():
    log = []
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    _record_telemetry(engine, log)
    started = asyncio.Event()
    try:

        @engine.block
        async def blk():
            started.set()
            await asyncio.Event().wait()

        fut = blk()
        await asyncio.wait_for(started.wait(), timeout=10)
        fut.cancel()
        await _settle(fut)
        await asyncio.sleep(0)
        assert [e[0] for e in _terminal(log, fut.uid)] == ["TaskCanceled"]
    finally:
        await engine.shutdown()


@pytest.mark.no_engine_invariants  # the stop failure is logged at ERROR
async def test_failing_stop_still_shuts_down_backends(caplog):
    log = []
    engine = await WorkflowEngine.create(backend=ParkingBackend(log))
    _record_telemetry(engine, log, fail_stop=True)
    with caplog.at_level(logging.ERROR, logger="radical.asyncflow"):
        await engine.shutdown()
    assert [e[0] for e in log] == ["stop", "backend shutdown"]
    assert "Stopping telemetry failed" in caplog.text
    assert engine._telemetry is None


async def test_shutdown_stops_telemetry_when_skipping_backends():
    log = []
    engine = await WorkflowEngine.create(backend=ParkingBackend(log))
    _record_telemetry(engine, log)
    await engine.shutdown(skip_execution_backend=True)
    assert [e[0] for e in log] == ["stop"]


async def test_real_manager_gets_one_terminal_event_per_component():
    pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")
    log = []
    engine = await WorkflowEngine.create(backend=ParkingBackend(log))
    telemetry = await engine.start_telemetry()
    events = []
    telemetry.subscribe(events.append)

    @engine.function_task
    async def slow():
        return 1

    @engine.function_task
    async def dependent(x):
        return 2

    running = slow()
    pending = dependent(running)
    await _wait_running(engine, running)
    await engine.shutdown()

    for fut in (running, pending):
        assert [
            e.event_type
            for e in events
            if e.task_id == fut.uid and e.event_type in TERMINAL
        ] == ["TaskCanceled"]
    assert any(e.event_type == "SessionEnded" for e in events)


async def test_terminal_event_is_emitted_before_the_awaiting_caller_resumes():
    """A caller that awaits the future at once can stop telemetry right after."""
    log = []
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    _record_telemetry(engine, log)
    try:

        @engine.function_task
        async def quick():
            return 1

        fut = quick()
        await fut
        assert [e[0] for e in _terminal(log, fut.uid)] == ["TaskCompleted"]
    finally:
        await engine.shutdown()


async def test_terminal_event_leaves_a_failure_unretrieved():
    """A failed future that nobody awaits is still logged by asyncio."""
    log = []
    engine = await WorkflowEngine.create(backend=await LocalExecutionBackend())
    _record_telemetry(engine, log)
    try:

        @engine.function_task
        async def bad():
            raise ValueError("expected")

        fut = bad()
        await asyncio.wait([fut])
        await asyncio.sleep(0)
        assert [e[0] for e in _terminal(log, fut.uid)] == ["TaskFailed"]
        assert fut._log_traceback
        fut.exception()  # retrieved here, so the test logs nothing
    finally:
        await engine.shutdown()

"""The engine shuts down once: every call, signal and engine error joins it."""

import asyncio
import logging
import signal
import threading

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine


class CountingBackend(NoopExecutionBackend):
    """Counts shutdown() calls; can be slow, and can fail its first `fail` calls."""

    def __init__(self, delay=0.0, fail=0):
        super().__init__()
        self.calls = 0
        self.finished = 0
        self.delay = delay
        self.fail = fail

    async def shutdown(self):
        self.calls += 1
        await asyncio.sleep(self.delay)
        if self.fail:
            self.fail -= 1
            raise RuntimeError("backend shutdown failed")
        self.finished += 1


async def _engine(**kwargs):
    backend = CountingBackend(**kwargs)
    return await WorkflowEngine.create(backend=backend), backend


async def test_concurrent_calls_shut_the_backend_down_once():
    engine, backend = await _engine(delay=0.2)
    await asyncio.gather(engine.shutdown(), engine.shutdown(), engine.shutdown())
    assert (backend.calls, backend.finished) == (1, 1)


async def test_every_caller_returns_after_the_backend_shutdown():
    engine, backend = await _engine(delay=0.2)
    first = asyncio.create_task(engine.shutdown())
    await asyncio.sleep(0)
    await engine.shutdown()  # joins the first call
    assert backend.finished == 1
    await first


async def test_a_call_after_completion_returns_at_once():
    engine, backend = await _engine()
    await engine.shutdown()
    await asyncio.wait_for(engine.shutdown(), 0.5)
    assert backend.calls == 1


async def test_is_shutting_down_from_the_call_on():
    engine, _ = await _engine(delay=0.1)
    assert not engine.is_shutting_down
    task = asyncio.create_task(engine.shutdown())
    await asyncio.sleep(0)  # the call has run, the shutdown task has not yet
    assert engine.is_shutting_down
    await task
    assert engine.is_shutting_down  # stays True: an engine never restarts


async def test_the_first_calls_skip_decides(caplog):
    engine, backend = await _engine()
    await engine.shutdown(skip_execution_backend=True)
    with caplog.at_level(logging.INFO, logger="radical.asyncflow.workflow_manager"):
        await engine.shutdown()
    assert backend.calls == 0  # the kept backend stays running
    assert "Shutdown already started with skip_execution_backend=True" in caplog.text
    await backend.shutdown()  # the backend's owner shuts it down


@pytest.mark.parametrize("cancels", [1, 2])
async def test_a_cancelled_caller_stops_waiting_the_shutdown_continues(cancels):
    engine, backend = await _engine(delay=0.3)
    waiter = asyncio.create_task(engine.shutdown())
    await asyncio.sleep(0.05)
    for _ in range(cancels):
        waiter.cancel()
        await asyncio.sleep(0)
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await engine.shutdown()  # joins the shutdown still running
    assert (backend.calls, backend.finished) == (1, 1)


async def test_a_timeout_bounds_the_wait_not_the_shutdown():
    engine, backend = await _engine(delay=0.3)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await engine.shutdown()
    assert backend.finished == 0
    await engine.shutdown()
    assert (backend.calls, backend.finished) == (1, 1)


async def test_a_failed_shutdown_is_retried_by_the_next_call(caplog):
    engine, backend = await _engine(delay=0.05, fail=1)
    results = await asyncio.gather(
        engine.shutdown(), engine.shutdown(), return_exceptions=True
    )
    assert [type(r) for r in results] == [RuntimeError, RuntimeError]  # one outcome
    assert "the next shutdown() call retries it" in caplog.text
    await engine.shutdown()  # retried
    assert (backend.calls, backend.finished) == (2, 1)
    await engine.shutdown()  # done: nothing more
    assert backend.calls == 2


async def test_a_shutdown_cancelled_by_a_third_party_is_restarted():
    engine, backend = await _engine(delay=0.3)
    first = asyncio.create_task(engine.shutdown())
    await asyncio.sleep(0.05)
    engine._shutdown_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    await engine.shutdown()
    assert backend.finished == 1


async def test_a_signal_during_a_shutdown_joins_it():
    engine, backend = await _engine(delay=0.3)
    manual = asyncio.create_task(engine.shutdown())
    await asyncio.sleep(0.05)
    await engine._handle_shutdown_signal(signal.SIGTERM)
    assert (backend.calls, backend.finished) == (1, 1)  # returned after it, once
    await manual


@pytest.mark.no_engine_invariants  # the run loop logs the injected error at ERROR
async def test_an_engine_error_shuts_the_engine_down_once():
    engine, backend = await _engine()
    engine._ready_queue.append("task.does-not-exist")  # the run loop fails on it
    engine._component_change_event.set()
    for _ in range(100):
        if backend.finished:
            break
        await asyncio.sleep(0.01)
    assert engine.is_shutting_down and backend.finished == 1
    await engine.shutdown()
    assert backend.calls == 1


async def test_a_call_from_another_loop_is_refused_and_harmless():
    engine, backend = await _engine()
    error = []

    def other_thread():
        try:
            asyncio.run(engine.shutdown())
        except RuntimeError as exc:
            error.append(exc)

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join()
    assert error and "engine's event loop" in str(error[0])
    await engine.shutdown()  # the engine's own loop still shuts it down
    assert backend.finished == 1


async def test_telemetry_is_the_latest_manager(tmp_path):
    pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")
    engine, _ = await _engine()
    assert engine.telemetry is None
    first = await engine.start_telemetry(checkpoint_path=str(tmp_path / "a"))
    assert engine.telemetry is first
    second = await engine.start_telemetry(checkpoint_path=str(tmp_path / "b"))
    assert engine.telemetry is second
    await engine.shutdown()
    assert engine.telemetry is second  # shutdown() does not stop or drop it
    await second.stop()
    await first.stop()

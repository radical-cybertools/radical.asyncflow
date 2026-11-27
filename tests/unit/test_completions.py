"""The run loop handles each completion once, as it happens, and prepares work in
slices.

It found completions by checking the future of every running component on every pass,
about a hundred passes a second while anything happens: with tens of thousands of tasks
in flight, that check was most of the loop's time, and a run's time grew with the
square of its tasks. An idle loop also noticed a completion only at its next one-second
timeout. And it prepared every ready component of a generation before it submitted
any, without letting other coroutines run.
"""

import asyncio
import math
import threading
import time

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.workflow_manager import _ComponentFuture


def _sweep_off(self):
    self._next_sweep = math.inf


@pytest.fixture(autouse=True)
def no_sweep(request, monkeypatch):
    """The run loop's check of every running future is off here.

    It (_sweep_running) would hide an end that the engine's own paths fail to record.
    Its own tests ask for `sweep`.
    """
    if "sweep" not in request.fixturenames:
        monkeypatch.setattr(WorkflowEngine, "_sweep_running", _sweep_off)


@pytest.fixture
def sweep():
    """The run loop's check of every running future stays on."""


class HoldingBackend(NoopExecutionBackend):
    """Parks submitted tasks as RUNNING; the test ends them, in the order it chooses."""

    def __init__(self, name="holding"):
        super().__init__(name=name)
        self.parked = {}
        self.order = []
        self.batches = []

    async def submit_tasks(self, tasks):
        self.batches.append(len(tasks))
        for task in tasks:
            self.parked[task["uid"]] = task
            self.order.append(task["uid"])
            self._callback_func(task, "RUNNING")

    async def cancel_task(self, uid):
        task = self.parked.pop(uid, None)
        if task is None:
            return False
        self._callback_func(task, "CANCELED")
        return True

    def finish(self, uid, value=None, error=None):
        task = self.parked.pop(uid)
        if error is None:
            task["return_value"] = value
            task["stdout"] = str(value)
            self._callback_func(task, "DONE")
        else:
            task["exception"] = error
            task["stderr"] = str(error)
            self._callback_func(task, "FAILED")


async def until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


async def _engine():
    backend = HoldingBackend()
    engine = await WorkflowEngine.create(backend=backend)
    return engine, backend


@pytest.mark.asyncio
async def test_a_pass_checks_no_running_task_that_did_not_finish(monkeypatch):
    """With 200 tasks in flight, finishing them one by one checked every running future
    on every pass: about ten thousand checks. Now each task's future is checked a few
    times, whatever is in flight."""
    engine, backend = await _engine()

    @engine.function_task
    async def work(i):
        return i

    futures = [work(i) for i in range(200)]
    await until(lambda: len(backend.parked) == 200)
    checks = []
    done = _ComponentFuture.done

    def counting(self):
        checks.append(self.uid)
        return done(self)

    monkeypatch.setattr(_ComponentFuture, "done", counting)
    for i, uid in enumerate(list(backend.parked)):
        backend.finish(uid, i)
        await asyncio.sleep(0.002)
    assert await asyncio.gather(*futures) == list(range(200))
    await until(lambda: not engine.running)
    monkeypatch.undo()
    assert len(checks) < 5 * len(futures)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_dependents_go_out_in_the_order_their_dependencies_completed():
    engine, backend = await _engine()

    @engine.function_task
    async def work(i):
        return i

    @engine.function_task
    async def after(x):
        return x

    a, b = work(1), work(2)
    await until(lambda: len(backend.parked) == 2)
    c, d = after(a), after(b)
    await asyncio.sleep(0.05)
    assert c.uid not in backend.order and d.uid not in backend.order
    backend.finish(b.uid, 2)
    backend.finish(a.uid, 1)
    await until(lambda: c.uid in backend.parked and d.uid in backend.parked)
    assert backend.order.index(d.uid) < backend.order.index(c.uid)
    assert a.uid not in engine.running and b.uid not in engine.running
    backend.finish(c.uid, 1)
    backend.finish(d.uid, 2)
    assert (await c, await d) == (1, 2)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_an_idle_loop_notices_a_completion_at_once():
    """It waited for its next one-second timeout before releasing the dependents."""
    engine, backend = await _engine()

    @engine.function_task
    async def work(i):
        return i

    @engine.function_task
    async def after(x):
        return x

    a = work(1)
    await until(lambda: a.uid in backend.parked)
    c = after(a)
    await asyncio.sleep(0.1)  # the loop has nothing to do and waits
    t0 = time.monotonic()
    backend.finish(a.uid, 1)
    await until(lambda: c.uid in backend.parked)
    assert time.monotonic() - t0 < 0.3
    backend.finish(c.uid, 1)
    assert await c == 1
    await engine.shutdown()


@pytest.mark.asyncio
async def test_a_failure_or_a_cancellation_releases_its_dependents():
    engine, backend = await _engine()

    @engine.function_task
    async def work(i):
        return i

    @engine.function_task
    async def after(x):
        return x

    failing, cancelled, ok = work(1), work(2), work(3)
    await until(lambda: len(backend.parked) == 3)
    on_failed, on_cancelled, on_ok = after(failing), after(cancelled), after(ok)
    backend.finish(failing.uid, error=ValueError("boom"))
    cancelled.cancel()
    backend.finish(ok.uid, 3)
    with pytest.raises(ValueError, match="boom"):
        await failing
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    with pytest.raises(Exception, match="dependency failure"):
        await asyncio.wait_for(on_failed, 5)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(on_cancelled, 5)
    await until(lambda: on_ok.uid in backend.parked)
    backend.finish(on_ok.uid, 3)
    assert await on_ok == 3
    await until(lambda: not engine.running)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_a_block_that_ends_releases_its_dependents():
    """A block runs in the engine, not on the backend: no backend reports its end."""
    engine, backend = await _engine()

    @engine.function_task
    async def work(i):
        return i

    @engine.block
    async def stage():
        return await work(1)

    @engine.function_task
    async def after(x):
        return x

    block = stage()
    await until(lambda: len(backend.parked) == 1)
    c = after(block)
    await asyncio.sleep(0.05)
    assert c.uid not in backend.order
    backend.finish(next(iter(backend.parked)), 1)
    assert await block == 1
    await until(lambda: c.uid in backend.parked)
    backend.finish(c.uid, 1)
    assert await c == 1
    await until(lambda: not engine.running)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_a_large_generation_is_prepared_in_slices_that_let_the_loop_run(
    monkeypatch,
):
    """Every ready component was prepared before any was submitted, in one go."""
    engine, backend = await _engine()
    extract = engine._extract_dependency_values

    async def slow_extract(comp_desc):
        time.sleep(0.002)
        return await extract(comp_desc)

    monkeypatch.setattr(engine, "_extract_dependency_values", slow_extract)

    @engine.function_task
    async def work(i):
        return i

    seen = []
    futures = [work(i) for i in range(100)]

    async def watch():
        while len(backend.parked) < len(futures):
            seen.append(len(backend.parked))
            await asyncio.sleep(0)

    await watch()
    assert any(0 < n < len(futures) for n in seen)
    assert len(backend.batches) > 1 and sum(backend.batches) == len(futures)
    assert backend.order == [f.uid for f in futures]
    for i, uid in enumerate(list(backend.parked)):
        backend.finish(uid, i)
    assert await asyncio.gather(*futures) == list(range(100))
    await engine.shutdown()


class YieldingBackend(HoldingBackend):
    """Lets the loop run while it submits, as a backend that sends in slices does."""

    async def submit_tasks(self, tasks):
        await asyncio.sleep(0.05)
        await super().submit_tasks(tasks)


@pytest.mark.asyncio
async def test_a_task_cancelled_while_its_batch_is_submitted_releases_its_dependents():
    """Not running yet, it is cancelled here, not by the backend."""
    backend = YieldingBackend()
    engine = await WorkflowEngine.create(backend=backend)

    @engine.function_task
    async def work(i):
        return i

    @engine.function_task
    async def after(x):
        return x

    a = work(1)
    c = after(a)
    await asyncio.sleep(0.02)  # the batch with `a` is being submitted
    a.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(c, 5)
    await until(lambda: a.uid not in engine.running)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_a_running_future_finished_outside_the_engine_is_still_seen(sweep):
    """A backend that sets a future itself is not reported: a check of every running
    future, at most once a second, still finds it."""
    engine, backend = await _engine()

    @engine.function_task
    async def work(i):
        return i

    @engine.function_task
    async def after(x):
        return x

    a = work(1)
    await until(lambda: a.uid in backend.parked)
    c = after(a)
    await asyncio.sleep(0.1)  # the dependent waits for `a`, and the loop is idle
    asyncio.Future.set_result(a, 7)  # neither the backend's report nor the engine
    await until(lambda: c.uid in backend.parked, timeout=3)
    backend.finish(c.uid, 7)
    assert await c == 7
    await engine.shutdown()


@pytest.mark.asyncio
async def test_the_safety_check_costs_at_most_about_one_percent_of_the_loop(
    monkeypatch, sweep
):
    """With 160,000 tasks in flight, checking them all took about 100 ms: once a second,
    a tenth of the loop.

    The next check waits at least a hundred times as long as the last one took, and at
    least a second.
    """
    from radical.asyncflow import workflow_manager

    engine, backend = await _engine()
    clock = iter([100.0, 100.05])  # the check takes 50 ms
    monkeypatch.setattr(workflow_manager.time, "monotonic", lambda: next(clock))
    engine._sweep_running()
    monkeypatch.undo()
    assert engine._next_sweep == pytest.approx(100.05 + 100 * 0.05)
    clock = iter([200.0, 200.001])  # a cheap check: at least a second
    monkeypatch.setattr(workflow_manager.time, "monotonic", lambda: next(clock))
    engine._sweep_running()
    monkeypatch.undo()
    assert engine._next_sweep == pytest.approx(200.001 + 1.0)
    await engine.shutdown()


class ServiceBackend(HoldingBackend):
    """Reports a service RUNNING with a callback that keeps its future.

    The backend, not the engine, sets it when the service is ready, as backends do.
    """

    def __init__(self):
        super().__init__()
        self.service_futures = {}

    async def submit_tasks(self, tasks):
        self.batches.append(len(tasks))
        for task in tasks:
            self.parked[task["uid"]] = task
            self.order.append(task["uid"])
            if task.get("is_service"):

                def keep(fut, task, state, uid=task["uid"]):
                    self.service_futures.setdefault(uid, fut)

                self._callback_func(task, "RUNNING", keep)
            else:
                self._callback_func(task, "RUNNING")


@pytest.mark.asyncio
async def test_a_service_that_becomes_ready_releases_its_dependents():
    backend = ServiceBackend()
    engine = await WorkflowEngine.create(backend=backend)

    @engine.function_task(service=True)
    async def server():
        return "unused"

    @engine.function_task
    async def client(address):
        return address

    svc = server()
    await until(lambda: svc.uid in backend.service_futures)
    c = client(svc)
    await asyncio.sleep(0.05)
    assert c.uid not in backend.order
    backend.service_futures[svc.uid].set_result("host:1234")
    await until(lambda: c.uid in backend.parked, timeout=1)
    backend.finish(c.uid, "host:1234")
    assert await c == "host:1234"
    await engine.shutdown()


@pytest.mark.asyncio
async def test_a_block_cancelled_from_outside_releases_its_dependents():
    """Its future is cancelled directly, not through a handler of the engine."""
    engine, backend = await _engine()

    @engine.function_task
    async def work(i):
        return i

    @engine.block
    async def stage():
        return await work(1)

    @engine.function_task
    async def after(x):
        return x

    block = stage()
    await until(lambda: len(backend.parked) == 1)
    c = after(block)
    await asyncio.sleep(0.05)
    block.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(c, 1)
    await until(lambda: block.uid not in engine.running)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_a_block_cancelled_as_it_starts_releases_its_dependents(monkeypatch):
    """Cancelled after its submit and before its body runs: its body then finds it done
    and reports nothing, so only the block's done callback sees its end."""
    engine, backend = await _engine()
    submit_blocks = engine._submit_blocks

    async def submit_then_cancel(blocks):
        loop = asyncio.get_running_loop()
        for block in blocks:  # runs after the submit, before the block's body
            loop.call_soon(engine.components[block["uid"]]["future"].cancel)
        await submit_blocks(blocks)

    monkeypatch.setattr(engine, "_submit_blocks", submit_then_cancel)

    @engine.block
    async def stage():
        return 1

    @engine.function_task
    async def after(x):
        return x

    block = stage()
    c = after(block)
    # Cancelled, or, with an eager task factory, done before the cancel
    await until(lambda: c.done() or c.uid in backend.parked, timeout=1)
    await until(lambda: block.uid not in engine.running)
    if c.uid in backend.parked:
        backend.finish(c.uid, 1)
    await asyncio.gather(c, return_exceptions=True)
    await engine.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["DONE", "FAILED", "CANCELED"])
async def test_a_report_for_a_future_finished_outside_the_engine_releases_dependents(
    state,
):
    """The future was set outside the engine, then the backend reports the task: the
    dependents waited for the safety check, longer the more tasks are running."""
    engine, backend = await _engine()

    @engine.function_task
    async def work(i):
        return i

    @engine.function_task
    async def after(x):
        return x

    a = work(1)
    await until(lambda: a.uid in backend.parked)
    c = after(a)
    await asyncio.sleep(0.05)
    asyncio.Future.set_result(a, 7)
    if state == "DONE":
        backend.finish(a.uid, 1)
    elif state == "FAILED":
        backend.finish(a.uid, error=ValueError("late"))
    else:
        backend._callback_func(backend.parked.pop(a.uid), "CANCELED")
    await until(lambda: c.uid in backend.parked, timeout=0.5)
    backend.finish(c.uid, 7)
    assert await c == 7
    await engine.shutdown()


@pytest.mark.asyncio
async def test_a_failure_cascade_is_handled_in_slices_that_let_the_loop_run(
    monkeypatch,
):
    """The slice limit applied only to components going to the backend: the failures of
    a whole generation of dependents were handled in one go."""
    engine, backend = await _engine()
    fail = engine.handle_component_failure

    def slow_failure(*args, **kwargs):
        time.sleep(0.002)
        return fail(*args, **kwargs)

    monkeypatch.setattr(engine, "handle_component_failure", slow_failure)

    @engine.function_task
    async def work(i):
        return i

    @engine.function_task
    async def after(x):
        return x

    a = work(1)
    await until(lambda: a.uid in backend.parked)
    dependents = [after(a) for _ in range(100)]
    await asyncio.sleep(0.05)
    backend.finish(a.uid, error=ValueError("boom"))
    seen = []
    while not all(d.done() for d in dependents):
        seen.append(sum(d.done() for d in dependents))
        await asyncio.sleep(0)
    assert any(0 < n < len(dependents) for n in seen)
    for d in dependents:
        with pytest.raises(Exception, match="dependency failure"):
            await d
    with pytest.raises(ValueError, match="boom"):
        await a
    await engine.shutdown()


@pytest.mark.asyncio
async def test_a_report_from_another_thread_wakes_the_loop_safely():
    """A backend that reports from its own thread: waking the loop must not touch
    asyncio from that thread (asyncio's debug mode raises if it does)."""
    engine, backend = await _engine()
    loop = asyncio.get_running_loop()
    debug = loop.get_debug()
    loop.set_debug(True)
    try:

        @engine.function_task
        async def work(i):
            return i

        @engine.function_task
        async def after(x):
            return x

        a = work(1)
        await until(lambda: a.uid in backend.parked)
        c = after(a)
        await asyncio.sleep(0.1)  # the loop is idle, waiting for a change
        errors = []

        def report():
            try:
                backend.finish(a.uid, 1)
            except Exception as e:  # raised in this thread, not the test's
                errors.append(e)

        reporter = threading.Thread(target=report)
        t0 = time.monotonic()
        reporter.start()
        reporter.join()
        assert errors == []
        await until(lambda: c.uid in backend.parked, timeout=0.5)
        assert time.monotonic() - t0 < 0.5
        backend.finish(c.uid, 1)
        assert await c == 1
    finally:
        loop.set_debug(debug)
        await engine.shutdown()


@pytest.mark.asyncio
async def test_completions_wait_for_more_before_their_dependents_go_out(monkeypatch):
    """The loop pauses 10 ms after a pass, as on main, to batch the next dependents.

    The dependents of completions in that time go to the backend together. It yields
    at once only when the slice limit cut its preparation short: yielding after every
    completion sent about five times as many, smaller, batches to the backend.
    """
    engine, backend = await _engine()
    real_sleep = asyncio.sleep
    pauses = []

    async def recording_sleep(delay, *args, **kwargs):
        if asyncio.current_task() is engine._run_task:
            pauses.append(delay)
        return await real_sleep(delay, *args, **kwargs)

    @engine.function_task
    async def work(i):
        return i

    @engine.function_task
    async def after(x):
        return x

    heads = [work(i) for i in range(20)]
    await until(lambda: len(backend.parked) == 20)
    tails = [after(h) for h in heads]
    await asyncio.sleep(0.05)
    monkeypatch.setattr(asyncio, "sleep", recording_sleep)
    for i, h in enumerate(heads):
        backend.finish(h.uid, i)
        await real_sleep(0.001)
    await until(lambda: all(t.uid in backend.parked for t in tails))
    monkeypatch.undo()
    assert pauses and 0 not in pauses
    for i, t in enumerate(tails):
        backend.finish(t.uid, i)
    assert await asyncio.gather(*tails) == list(range(20))
    await engine.shutdown()

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
import time

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.workflow_manager import _ComponentFuture


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

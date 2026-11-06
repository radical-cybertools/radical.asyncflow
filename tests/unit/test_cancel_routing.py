import asyncio
import logging
import time
from contextlib import asynccontextmanager

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine


class ParkingBackend(NoopExecutionBackend):
    """Parks submitted tasks as RUNNING; cancel_task reports CANCELED.

    The first `refuse` cancels return False (the backend could not cancel).
    """

    def __init__(self, name, refuse=0):
        super().__init__(name=name)
        self.cancelled = []
        self.parked = {}
        self.refuse = refuse

    async def submit_tasks(self, tasks):
        for t in tasks:
            self.parked[t["uid"]] = t
            self._callback_func(t, "RUNNING")

    async def cancel_task(self, uid):
        self.cancelled.append(uid)
        if self.refuse:
            self.refuse -= 1
            return False
        task = self.parked.pop(uid, None)
        if task is None:
            return False
        self._callback_func(task, "CANCELED")
        return True


async def _settle(*futs):
    """Waits until all futs are done (fails after 10 s)."""
    await asyncio.wait_for(asyncio.wait(futs), timeout=10)


async def _wait_running(engine, fut):
    for _ in range(500):
        if hasattr(fut, "task") and fut.uid in engine.running:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("task never ran")


async def _run(target):
    first, second = ParkingBackend("first"), ParkingBackend("second")
    engine = await WorkflowEngine.create(backend=[first, second])
    try:

        @engine.function_task(backend=target)
        async def slow():
            return 1

        @engine.function_task
        async def dependent(x):
            return 2

        fut = slow()
        dep = dependent(fut)
        await _wait_running(engine, fut)
        assert fut.cancel() is True
        await _settle(fut, dep)
        return first, second, fut, dep
    finally:
        await engine.shutdown()


async def test_cancel_reaches_task_backend():
    first, second, fut, dep = await _run("second")
    assert second.cancelled == [fut.uid]
    assert first.cancelled == []
    assert fut.cancelled() and fut.state == "CANCELLED"
    assert dep.cancelled()


async def test_cancel_default_backend_task():
    first, second, fut, dep = await _run(None)
    assert first.cancelled == [fut.uid]
    assert second.cancelled == []
    assert fut.cancelled()
    assert dep.cancelled()


async def test_block_cancel_reaches_member_backend():
    first, second = ParkingBackend("first"), ParkingBackend("second")
    engine = await WorkflowEngine.create(backend=[first, second])
    try:

        @engine.function_task(backend="second")
        async def slow():
            return 1

        holder = {}

        @engine.block
        async def blk():
            holder["fut"] = slow()
            return await holder["fut"]

        b = blk()
        for _ in range(500):
            f = holder.get("fut")
            if f is not None and hasattr(f, "task") and f.uid in engine.running:
                break
            await asyncio.sleep(0.01)
        b.cancel()
        await _settle(holder["fut"])
        # sent once, although the cascade reaches the member twice
        assert second.cancelled == [holder["fut"].uid]
        assert first.cancelled == []
        assert holder["fut"].cancelled()
    finally:
        await engine.shutdown()


async def test_failed_backend_cancel_can_be_retried():
    first, second = ParkingBackend("first"), ParkingBackend("second", refuse=1)
    engine = await WorkflowEngine.create(backend=[first, second])
    try:

        @engine.function_task(backend="second")
        async def slow():
            return 1

        fut = slow()
        await _wait_running(engine, fut)
        assert fut.cancel() is True  # refused by the backend
        for _ in range(500):  # until the refusal is recorded
            if second.cancelled and not fut._cancel_sent:
                break
            await asyncio.sleep(0.01)
        assert not fut.done()
        assert fut.cancel() is True  # sent again, now accepted
        await _settle(fut)
        assert second.cancelled == [fut.uid, fut.uid]
        assert fut.cancelled()
    finally:
        await engine.shutdown()


class YieldingBackend(ParkingBackend):
    """Parks a task, then suspends once more inside submit_tasks."""

    async def submit_tasks(self, tasks):
        await super().submit_tasks(tasks)
        await asyncio.sleep(0)


class GatedBackend(ParkingBackend):
    """Stays inside submit_tasks until `gate` is set.

    With `park_first` it knows the tasks while it waits; without it, only once the gate
    opens. As Dragon's does, its cancel_task raises for a uid it does not know.
    """

    def __init__(self, name, park_first=True, refuse=0):
        super().__init__(name, refuse=refuse)
        self.park_first = park_first
        self.submitting = asyncio.Event()
        self.gate = asyncio.Event()

    async def submit_tasks(self, tasks):
        if self.park_first:
            await super().submit_tasks(tasks)
        self.submitting.set()
        await self.gate.wait()
        if not self.park_first:
            await super().submit_tasks(tasks)

    async def cancel_task(self, uid):
        if uid not in self.parked:
            self.cancelled.append(uid)
            raise ValueError(f"Task {uid} not found")
        return await super().cancel_task(uid)


@asynccontextmanager
async def _submitting(routed=True, **kwargs):
    """Yields a task held inside the submit_tasks of a GatedBackend.

    Args:
        routed: The task names that backend, so submit gathers the backends' submits;
            otherwise it is the default backend, which submit awaits directly.
        kwargs: For the GatedBackend.
    """
    gated, other = GatedBackend("gated", **kwargs), ParkingBackend("other")
    backends = [other, gated] if routed else [gated, other]
    engine = await WorkflowEngine.create(backend=backends)
    try:

        @engine.function_task(backend="gated" if routed else None)
        async def slow():
            return 1

        fut = slow()
        await asyncio.wait_for(gated.submitting.wait(), timeout=10)
        assert fut.uid not in engine.running
        yield engine, gated, other, fut
    finally:
        gated.gate.set()
        await engine.shutdown()


async def _until(condition):
    """Waits for condition at every loop step (fails after 10 s)."""
    deadline = time.monotonic() + 10
    while not condition():
        assert time.monotonic() < deadline, "condition never met"
        await asyncio.sleep(0)


async def test_cancel_during_submit_reaches_backend():
    first, second = ParkingBackend("first"), YieldingBackend("second")
    engine = await WorkflowEngine.create(backend=[first, second])
    try:

        @engine.function_task(backend="second")
        async def slow():
            return 1

        fut = slow()
        await _until(lambda: second.parked)  # the window lasts only a few steps
        assert fut.uid not in engine.running
        assert fut.cancel() is True
        assert fut.cancelled() and fut.state == "CANCELLED"
        await _until(lambda: second.cancelled)
        assert second.cancelled == [fut.uid]
        assert first.cancelled == []
        assert not second.parked
    finally:
        await engine.shutdown()


@pytest.mark.parametrize("routed", [True, False])
async def test_cancel_during_submit_is_sent_when_submit_returns(routed):
    # The backend does not know the task until its submit ends: a cancel sent
    # during submit would raise there, and the task would run on.
    async with _submitting(routed, park_first=False) as (_, gated, other, fut):
        assert fut.cancel() is True
        await asyncio.sleep(0.02)
        assert gated.cancelled == []
        gated.gate.set()
        await _until(lambda: gated.cancelled)
        assert gated.cancelled == [fut.uid]
        assert other.cancelled == []
        assert not gated.parked


async def test_cancel_during_submit_cancels_dependents():
    gated, other = GatedBackend("gated"), ParkingBackend("other")
    engine = await WorkflowEngine.create(backend=[other, gated])
    try:

        @engine.function_task(backend="gated")
        async def slow():
            return 1

        @engine.function_task
        async def dependent(x):
            return 2

        fut = slow()
        dep = dependent(fut)
        await asyncio.wait_for(gated.submitting.wait(), timeout=10)
        fut.cancel()
        gated.gate.set()
        await _settle(dep)
        assert dep.cancelled()
    finally:
        await engine.shutdown()


async def test_repeated_cancel_during_submit_sends_once():
    async with _submitting() as (_, gated, _, fut):
        assert fut.cancel() is True
        assert fut.cancel() is False  # as for any cancelled future
        gated.gate.set()
        await _until(lambda: gated.cancelled)
        await asyncio.sleep(0.02)
        assert gated.cancelled == [fut.uid]


@pytest.mark.parametrize("state", ["DONE", "FAILED", "CANCELED"])
async def test_task_finishing_during_submit_is_not_sent_a_cancel(state):
    async with _submitting() as (engine, gated, _, fut):
        assert fut.cancel() is True
        task = gated.parked.pop(fut.uid)
        task["return_value"] = 1
        gated._callback_func(task, state)
        assert not engine._pending_cancels
        gated.gate.set()
        await asyncio.sleep(0.02)
        assert gated.cancelled == []


async def test_failed_cancel_during_submit_is_logged(caplog):
    async with _submitting() as (_, gated, _, fut):
        assert fut.cancel() is True
        del gated.parked[fut.uid]  # finished, but not reported
        with caplog.at_level(logging.WARNING):
            gated.gate.set()
            await _until(lambda: gated.cancelled)
            await asyncio.sleep(0)  # the send's done callback
        assert "on its backend failed" in caplog.text


async def test_block_cancel_during_member_submit():
    first, second = ParkingBackend("first"), GatedBackend("second")
    engine = await WorkflowEngine.create(backend=[first, second])
    try:

        @engine.function_task(backend="second")
        async def slow():
            return 1

        holder = {}

        @engine.block
        async def blk():
            holder["fut"] = slow()
            return await holder["fut"]

        b = blk()
        await asyncio.wait_for(second.submitting.wait(), timeout=10)
        b.cancel()
        second.gate.set()
        await _until(lambda: second.cancelled)
        await asyncio.sleep(0.02)
        # sent once, although the cascade reaches the member twice
        assert second.cancelled == [holder["fut"].uid]
        assert first.cancelled == []
        assert holder["fut"].cancelled()
        assert b.cancelled()
    finally:
        await engine.shutdown()


async def test_cancel_waiting_for_a_submit_of_others_stays_local():
    async with _submitting() as (engine, gated, other, fut):

        @engine.function_task(backend="other")
        async def later(x):
            return 2

        waiting = later(fut)
        await _until(lambda: hasattr(waiting, "task"))
        assert waiting.cancel() is True
        assert not engine._pending_cancels
        gated.gate.set()
        await asyncio.sleep(0.02)
        assert other.cancelled == []


async def test_shutdown_during_submit_with_cancel_held():
    first, second = ParkingBackend("first"), GatedBackend("second")
    engine = await WorkflowEngine.create(backend=[first, second])

    @engine.function_task(backend="second")
    async def slow():
        return 1

    fut = slow()
    await asyncio.wait_for(second.submitting.wait(), timeout=10)
    assert fut.cancel() is True
    await engine.shutdown()
    assert fut.cancelled() and fut.state == "CANCELLED"
    assert second.cancelled == []  # the backend's shutdown stops what it holds

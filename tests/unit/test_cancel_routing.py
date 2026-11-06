import asyncio

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

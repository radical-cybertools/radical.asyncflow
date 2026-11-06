import asyncio
import logging

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

    def finish(self, uid, state="DONE"):
        task = self.parked.pop(uid)
        task["return_value"] = task["stdout"] = "finished"
        if state == "FAILED":
            task["exception"] = RuntimeError("member failed")
        self._callback_func(task, state)


class RefusingBackend(ParkingBackend):
    """Parks submitted tasks as RUNNING; cancel_task always returns False."""

    async def cancel_task(self, uid):
        self.cancelled.append(uid)
        return False


class RaisingBackend(ParkingBackend):
    """Parks submitted tasks as RUNNING; cancel_task always raises."""

    async def cancel_task(self, uid):
        self.cancelled.append(uid)
        raise RuntimeError("cancel failed")


class TooLateBackend(ParkingBackend):
    """The task finishes as its cancel arrives, so cancel_task returns False."""

    async def cancel_task(self, uid):
        self.cancelled.append(uid)
        self.finish(uid)
        return False


async def _settle(*futs):
    """Waits until all futs are done (fails after 10 s)."""
    await asyncio.wait_for(asyncio.wait(futs), timeout=10)


async def _until(condition):
    for _ in range(500):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never held")


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


async def _cancel_block_on_uncancellable_member(
    backend, end_state="DONE", wait=None, from_outside=False
):
    """Cancels a block whose body awaits a task the backend does not cancel, then ends
    that task with end_state.

    Args:
        wait: gives what the body awaits, from the task's future and holder.
        from_outside: the task is not a member of the block.

    Returns:
        What the body did.
    """
    engine = await WorkflowEngine.create(backend=backend)
    try:

        @engine.function_task
        async def slow():
            return 1

        holder = {"resumed": False, "caught": None}
        outside = slow() if from_outside else None

        @engine.block
        async def blk():
            holder["body"] = asyncio.current_task()
            holder["fut"] = outside or slow()
            try:
                await (wait(holder["fut"], holder) if wait else holder["fut"])
            except BaseException as e:
                holder["caught"] = e
                raise
            holder["resumed"] = True

        b = blk()
        await _until(lambda: "fut" in holder)
        fut = holder["fut"]
        await _wait_running(engine, fut)
        b.cancel()
        await _until(lambda: backend.cancelled and not fut._cancel_sent)
        assert b.cancelled() and b.state == "CANCELLED"
        assert fut.state == "RUNNING"
        backend.finish(fut.uid, end_state)
        await _settle(fut, holder["body"])
        return holder
    finally:
        await engine.shutdown()


def _assert_body_stopped(holder):
    assert holder["resumed"] is False
    assert holder["body"].cancelled()
    assert isinstance(holder["caught"], asyncio.CancelledError)


async def test_block_body_stops_when_backend_refuses_member_cancel():
    holder = await _cancel_block_on_uncancellable_member(RefusingBackend("r"))
    _assert_body_stopped(holder)
    assert holder["fut"].state == "DONE"  # as the backend reported it


async def test_block_body_stops_when_backend_fails_member_cancel():
    holder = await _cancel_block_on_uncancellable_member(RaisingBackend("r"))
    _assert_body_stopped(holder)
    assert holder["fut"].state == "DONE"


async def test_block_body_gets_cancelled_not_the_failure_of_its_member():
    holder = await _cancel_block_on_uncancellable_member(
        RefusingBackend("r"), end_state="FAILED"
    )
    _assert_body_stopped(holder)
    assert holder["fut"].state == "FAILED"


async def test_block_body_stops_on_a_task_from_outside_the_block():
    holder = await _cancel_block_on_uncancellable_member(
        RefusingBackend("r"), from_outside=True
    )
    _assert_body_stopped(holder)


async def test_block_body_stops_in_wait_for():
    holder = await _cancel_block_on_uncancellable_member(
        RefusingBackend("r"), wait=lambda fut, _: asyncio.wait_for(fut, 60)
    )
    _assert_body_stopped(holder)


async def test_block_body_stops_in_a_task_it_awaits():
    async def helper(fut, holder):
        await fut
        holder["helper_resumed"] = True

    holder = await _cancel_block_on_uncancellable_member(
        RefusingBackend("r"),
        wait=lambda fut, holder: asyncio.create_task(helper(fut, holder)),
    )
    _assert_body_stopped(holder)
    assert "helper_resumed" not in holder


async def test_refused_member_of_a_cancelled_block_can_be_cancelled_again():
    backend = ParkingBackend("p", refuse=99)
    engine = await WorkflowEngine.create(backend=backend)
    try:

        @engine.function_task
        async def slow():
            return 1

        holder = {}

        @engine.block
        async def blk():
            holder["body"] = asyncio.current_task()
            holder["fut"] = slow()
            await holder["fut"]

        b = blk()
        await _until(lambda: "fut" in holder)
        fut = holder["fut"]
        await _wait_running(engine, fut)
        b.cancel()
        await _until(lambda: backend.cancelled and not fut._cancel_sent)
        backend.refuse = 0
        assert fut.cancel() is True  # sent again, now accepted
        await _settle(fut, holder["body"])
        assert fut.cancelled() and holder["body"].cancelled()
    finally:
        await engine.shutdown()


async def test_member_ending_does_not_interrupt_cleanup_of_a_cancelled_block():
    backend = RefusingBackend("r")
    engine = await WorkflowEngine.create(backend=backend)
    try:

        @engine.function_task
        async def slow():
            return 1

        holder = {"cleaned_up": False}
        release = asyncio.Event()

        @engine.block
        async def blk():
            holder["body"] = asyncio.current_task()
            holder["fut"] = slow()
            try:
                await asyncio.Event().wait()  # not the member
            finally:
                await release.wait()
                holder["cleaned_up"] = True

        b = blk()
        await _until(lambda: "fut" in holder)
        fut = holder["fut"]
        await _wait_running(engine, fut)
        b.cancel()
        await _until(lambda: backend.cancelled and not fut._cancel_sent)
        backend.finish(fut.uid)
        await _settle(fut)
        await asyncio.sleep(0)  # the body's wakeup, if the member's end caused one
        release.set()
        await _settle(holder["body"])
        assert holder["cleaned_up"] is True
    finally:
        await engine.shutdown()


async def _cancel_running_task(backend):
    engine = await WorkflowEngine.create(backend=backend)
    try:

        @engine.function_task
        async def slow():
            return 1

        fut = slow()
        await _wait_running(engine, fut)
        fut.cancel()
        await _until(lambda: backend.cancelled and not fut._cancel_sent)
        return fut
    finally:
        await engine.shutdown()


async def test_refused_cancel_is_logged(caplog):
    with caplog.at_level(logging.WARNING, "radical.asyncflow.workflow_manager"):
        fut = await _cancel_running_task(RefusingBackend("r"))
    assert f"Backend refused to cancel {fut.uid}" in caplog.text


async def test_cancel_of_a_task_that_just_finished_is_not_logged(caplog):
    with caplog.at_level(logging.WARNING, "radical.asyncflow.workflow_manager"):
        fut = await _cancel_running_task(TooLateBackend("t"))
    assert fut.state == "DONE"
    assert "refused" not in caplog.text

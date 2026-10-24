"""Scheduler completion paths: every way a component future can resolve
releases its dependents promptly, also while the run loop is kept busy.

Each path releases dependents through the engine's ``_on_done``.  Nothing
polls: a path that skipped it would leave its dependents waiting forever,
so every wait in these tests is bounded.  A task future resolved outside the
engine is the exception: its dependents are released when the backend reports
the task.
"""

import asyncio
import queue
import threading
import time

import pytest
import pytest_asyncio

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine


class _Backend(NoopExecutionBackend):
    """Noop backend with task kinds selected by name prefix.

    - ``park*``: accepted, never reported (stays running); kept in ``parked``
    - ``svc*``: reported RUNNING with a service_callback resolving the future
    - ``fail*``: reported FAILED
    - a submit containing a ``park_slow*`` task sets ``slow_started`` and
      then awaits ``slow`` seconds
    """

    def __init__(self):
        super().__init__()
        self.slow = 0.0
        self.slow_started = asyncio.Event()
        self.parked = {}  # uid -> task

    async def submit_tasks(self, tasks):
        if self.slow and any(t["name"].startswith("park_slow") for t in tasks):
            self.slow_started.set()
            await asyncio.sleep(self.slow)
        rest = []
        for t in tasks:
            if t["name"].startswith("park"):
                self.parked[t["uid"]] = t
                continue
            if t["name"].startswith("svc"):

                def ready(fut, task, state):
                    if not fut.done():
                        fut.set_result("ready")

                self._callback_func(t, "RUNNING", service_callback=ready)
                continue
            if t["name"].startswith("fail"):
                t["exception"] = RuntimeError("boom")
                self._callback_func(t, "FAILED")
                continue
            rest.append(t)
        await super().submit_tasks(rest)


class _ThreadBackend(NoopExecutionBackend):
    """Completes each task ``delay`` seconds after submission, calling the engine
    callback directly from a worker thread, as Dragon's monitor thread does for
    cancellations made on the Dragon side."""

    def __init__(self, delay):
        super().__init__()
        self.delay = delay
        self.queue = queue.Queue()
        threading.Thread(target=self._work, daemon=True).start()

    def _work(self):
        while True:
            task = self.queue.get()
            if task is None:
                return
            time.sleep(self.delay)
            task["return_value"] = task["name"]
            self._callback_func(task, "DONE")

    async def submit_tasks(self, tasks):
        for t in tasks:
            self.queue.put(t)

    async def shutdown(self):
        self.queue.put(None)


class _ServiceThreadBackend(NoopExecutionBackend):
    """Reports each service RUNNING with a service_callback that resolves the future
    from a daemon thread ``delay`` seconds later, as the RADICAL-Pilot backend's
    service_ready_callback does; other tasks complete at once."""

    def __init__(self, delay, outcome="result"):
        super().__init__()
        self.delay = delay
        self.outcome = outcome

    def _service_ready(self, fut, task, state):
        def wait_and_set():
            time.sleep(self.delay)
            if self.outcome == "result":
                fut.set_result("ready")
            else:
                fut.set_exception(RuntimeError("service failed to start"))

        threading.Thread(target=wait_and_set, daemon=True).start()

    async def submit_tasks(self, tasks):
        rest = []
        for t in tasks:
            if t.get("is_service"):
                self._callback_func(t, "RUNNING", service_callback=self._service_ready)
            else:
                rest.append(t)
        await super().submit_tasks(rest)


@pytest_asyncio.fixture
async def make_flow():
    """Factory: engine on a ``_Backend`` with a stream of short tasks keeping
    the run loop busy; everything is stopped and shut down on teardown."""
    made = []

    async def _make():
        backend = _Backend()
        flow = await WorkflowEngine.create(backend=backend)
        stop = asyncio.Event()

        @flow.function_task
        async def tick():
            return None

        async def load():
            while not stop.is_set():
                await tick()

        load_task = asyncio.create_task(load())
        made.append((flow, stop, load_task))
        await asyncio.sleep(0.05)  # let the load stream start
        return flow, backend

    yield _make

    for flow, stop, load_task in made:
        stop.set()
        await load_task
        await flow.shutdown()


async def _until(cond, limit=5.0):
    """Wait until ``cond()`` is true (fails the test after ``limit`` s)."""
    deadline = asyncio.get_running_loop().time() + limit
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("test setup did not reach the expected state")
        await asyncio.sleep(0.001)


async def _resolves_within(fut, limit):
    # shield: a timeout must not cancel the dependent via its patched cancel
    try:
        await asyncio.wait_for(asyncio.shield(fut), limit)
    except asyncio.TimeoutError:
        pytest.fail(f"dependent not released within {limit}s")
    except (Exception, asyncio.CancelledError):
        pass  # failed or cancelled dependents count as released


class TestCompletionPaths:
    @pytest.mark.asyncio
    async def test_block_cancelled_by_user(self, make_flow):
        flow, _ = await make_flow()

        @flow.block
        async def blk():
            await asyncio.sleep(100)

        @flow.function_task
        async def dep(x):
            return 1

        b = blk()
        d = dep(b)
        await _until(lambda: hasattr(b, "block") and b.block["uid"] in flow.running)
        b.cancel()
        await _resolves_within(d, 0.5)

    @pytest.mark.asyncio
    async def test_task_cancelled_during_submit(self, make_flow):
        flow, backend = await make_flow()
        backend.slow = 0.1

        @flow.function_task
        async def park_slow():
            return 1

        @flow.function_task
        async def dep(x):
            return 2

        x = park_slow()
        d = dep(x)
        await backend.slow_started.wait()
        x.cancel()
        await _resolves_within(d, 0.5)

    @pytest.mark.asyncio
    async def test_pending_task_cancelled_before_its_dependencies_finish(
        self, make_flow
    ):
        """Cancelling a task that still waits for its own dependencies releases its
        dependents at once, also those registered after the cancel."""
        flow, _ = await make_flow()

        @flow.function_task
        async def park():
            return 1

        @flow.function_task
        async def mid(x):
            return 2

        @flow.function_task
        async def leaf(x):
            return 3

        m = mid(park())  # park never finishes, so m stays pending
        before = leaf(m)
        await _until(lambda: hasattr(before, "task"))
        m.cancel()
        after = leaf(m)
        await _resolves_within(before, 0.5)
        await _resolves_within(after, 0.5)
        assert before.cancelled() and after.cancelled()

    @pytest.mark.asyncio
    async def test_service_resolved_by_service_callback(self, make_flow):
        flow, _ = await make_flow()

        @flow.function_task(service=True)
        async def svc():
            return 1

        @flow.function_task
        async def dep(x):
            return 2

        d = dep(svc())
        await _resolves_within(d, 0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["DONE", "FAILED", "CANCELED"])
    async def test_task_resolved_outside_engine(self, make_flow, state):
        """A running task's future resolved outside the engine releases its dependents
        when the backend reports the task, whatever the report."""
        flow, backend = await make_flow()

        @flow.function_task
        async def park():
            return 1

        @flow.function_task
        async def dep(x):
            return 2

        p = park()
        d = dep(p)
        await _until(lambda: hasattr(p, "task") and p.task["uid"] in backend.parked)
        p.set_result("set outside the engine")
        task = backend.parked.pop(p.task["uid"])
        task["exception"] = RuntimeError("late")  # read for FAILED only
        backend._callback_func(task, state)
        await _resolves_within(d, 0.5)
        assert d.exception() is None
        assert p.task["uid"] not in flow.running

    @pytest.mark.asyncio
    async def test_running_set_drains(self, make_flow):
        flow, _ = await make_flow()

        @flow.function_task
        async def ok():
            return 1

        @flow.function_task
        async def fail_one():
            return 1

        @flow.function_task
        async def after(x):
            return 2

        futs = []
        for _ in range(20):
            futs += [after(ok()), after(fail_one())]
        await asyncio.gather(*futs, return_exceptions=True)
        # only the load stream's current tick may still be in flight
        await _until(lambda: len(flow.running) <= 1)


class TestBackendThread:
    """Backend callbacks from another thread are handled on the loop thread."""

    @pytest.mark.asyncio
    async def test_handled_on_loop_thread(self):
        flow = await WorkflowEngine.create(backend=_ThreadBackend(delay=0.02))
        handled_on = []
        handle_success = flow.handle_component_success

        def record(component, fut):
            handled_on.append(threading.get_ident())
            return handle_success(component, fut)

        flow.handle_component_success = record
        try:

            @flow.function_task
            async def first():
                return 1

            @flow.function_task
            async def second(x):
                return 2

            d = second(first())
            await _resolves_within(d, 0.5)
            assert d.result() == "second"
            assert handled_on and set(handled_on) == {threading.get_ident()}
        finally:
            await flow.shutdown()

    @pytest.mark.asyncio
    async def test_chain_has_no_poll_delay(self):
        """Each edge costs the task's 10 ms, not a wait for a periodic wake-up."""
        flow = await WorkflowEngine.create(backend=_ThreadBackend(delay=0.01))
        try:

            @flow.function_task
            async def step(*x):
                return None

            start = time.perf_counter()
            x = step()
            for _ in range(9):
                x = step(x)
            await asyncio.wait_for(x, 5)
            elapsed = time.perf_counter() - start
            # ~0.1 s expected for 10 x 10 ms; generous margin for slow runners
            assert elapsed < 0.5, f"10-step chain took {elapsed:.3f}s"
        finally:
            await flow.shutdown()


class TestServiceThread:
    """A service_callback may resolve the service future from its own thread."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("outcome", ["result", "exception"])
    async def test_resolved_from_callback_thread(self, outcome):
        """The engine is otherwise idle, so nothing else wakes the run loop: a future
        resolved off the loop thread would wait for the timeout."""
        flow = await WorkflowEngine.create(
            backend=_ServiceThreadBackend(delay=0.02, outcome=outcome)
        )
        try:

            @flow.function_task(service=True)
            async def svc():
                return 1

            @flow.function_task
            async def dep(x):
                return 2

            start = time.perf_counter()
            s = svc()
            d = dep(s)
            await _resolves_within(d, 2.0)
            elapsed = time.perf_counter() - start
            # ~0.02 s expected; a missed wake-up shows up as the 2 s timeout
            assert elapsed < 1.0, f"service seen after {elapsed:.3f}s"
            if outcome == "result":
                assert s.result() == "ready"
            else:
                assert isinstance(s.exception(), RuntimeError)
        finally:
            await flow.shutdown()


class TestIdle:
    @pytest.mark.asyncio
    async def test_idle_loop_does_not_poll(self):
        """With tasks in flight and nothing ready, the run loop stays asleep."""
        flow = await WorkflowEngine.create(backend=_Backend())
        try:

            @flow.function_task
            async def park():
                return 1

            futs = [park() for _ in range(100)]
            await _until(lambda: len(flow.running) == 100 and flow._wakeup)
            wakeup = flow._wakeup
            await asyncio.sleep(0.3)
            # any periodic wake-up would have resolved and replaced this future
            assert flow._wakeup is wakeup and not wakeup.done()
            assert not any(f.done() for f in futs)
        finally:
            await flow.shutdown()

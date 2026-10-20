"""Scheduler completion paths: every way a component future can resolve
releases its dependents promptly, also while the run loop is kept busy.

Completions reported through the engine's handlers wake the run loop
directly; futures resolved elsewhere are picked up by the safety scan every
``poll_timeout``.  Event-path tests use a long ``poll_timeout`` so that the
scan cannot mask a missing wake-up; the scan-path test uses a short one.
"""

import asyncio
import math

import pytest
import pytest_asyncio

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine

LONG = 30  # poll_timeout that keeps the safety scan out of the picture
SHORT = 0.05


class _Backend(NoopExecutionBackend):
    """Noop backend with task kinds selected by name prefix.

    - ``park*``: accepted, never reported (stays running)
    - ``svc*``: reported RUNNING with a service_callback resolving the future
    - ``fail*``: reported FAILED
    - a submit containing a ``park_slow*`` task sets ``slow_started`` and
      then awaits ``slow`` seconds
    """

    def __init__(self):
        super().__init__()
        self.slow = 0.0
        self.slow_started = asyncio.Event()

    async def submit_tasks(self, tasks):
        if self.slow and any(t["name"].startswith("park_slow") for t in tasks):
            self.slow_started.set()
            await asyncio.sleep(self.slow)
        rest = []
        for t in tasks:
            if t["name"].startswith("park"):
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


@pytest_asyncio.fixture
async def make_flow():
    """Factory: engine on a ``_Backend`` with a stream of short tasks keeping
    the run loop busy; everything is stopped and shut down on teardown."""
    made = []

    async def _make(poll_timeout):
        backend = _Backend()
        flow = await WorkflowEngine.create(backend=backend, poll_timeout=poll_timeout)
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
        flow, _ = await make_flow(LONG)

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
        flow, backend = await make_flow(LONG)
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
    async def test_service_resolved_by_service_callback(self, make_flow):
        flow, _ = await make_flow(LONG)

        @flow.function_task(service=True)
        async def svc():
            return 1

        @flow.function_task
        async def dep(x):
            return 2

        d = dep(svc())
        await _resolves_within(d, 0.5)

    @pytest.mark.asyncio
    async def test_future_resolved_outside_engine(self, make_flow):
        flow, _ = await make_flow(SHORT)

        @flow.function_task
        async def park():
            return 1

        @flow.function_task
        async def dep(x):
            return 2

        p = park()
        d = dep(p)
        await _until(lambda: hasattr(p, "task") and p.task["uid"] in flow.running)
        # bypasses every engine handler: only the safety scan sees it
        asyncio.Future.set_result(p, 7)
        await _resolves_within(d, 0.5)
        assert d.done() and not d.cancelled() and d.exception() is None

    @pytest.mark.asyncio
    async def test_running_set_drains(self, make_flow):
        flow, _ = await make_flow(LONG)

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


class TestPollTimeout:
    @pytest.mark.asyncio
    async def test_default_and_custom(self):
        flow = await WorkflowEngine.create(dry_run=True)
        assert flow._poll_timeout == 0.1
        await flow.shutdown()

        flow = await WorkflowEngine.create(dry_run=True, poll_timeout=0.5)
        assert flow._poll_timeout == 0.5
        await flow.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [0, -1.0, math.nan, math.inf])
    async def test_invalid_value(self, value):
        with pytest.raises(ValueError, match="poll_timeout"):
            await WorkflowEngine.create(dry_run=True, poll_timeout=value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["0.1", None])
    async def test_invalid_type(self, value):
        with pytest.raises(TypeError):
            await WorkflowEngine.create(dry_run=True, poll_timeout=value)

    @pytest.mark.asyncio
    async def test_keyword_only(self):
        with pytest.raises(TypeError, match="positional"):
            await WorkflowEngine.create(None, True, True, None, None, 0.1)

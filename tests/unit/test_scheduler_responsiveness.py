"""Scheduler responsiveness: the run loop must react to component completion
via events, not a fixed poll interval.

Regression tests for two latency defects of the former poll-based loop:

- a hardcoded 10ms sleep after every active pass put a ~10ms floor on each
  task round-trip, and
- a dependent task behind a dependency running longer than that poll
  interval was only submitted when the 1s event-wait timed out (~1s stall
  per dependency edge).
"""

import asyncio
import time

import pytest
import pytest_asyncio

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine


class TestSchedulerResponsiveness:
    @pytest_asyncio.fixture
    async def flow(self):
        backend = await LocalExecutionBackend()
        flow = await WorkflowEngine.create(backend=backend)
        yield flow
        await flow.shutdown()

    @pytest.mark.asyncio
    async def test_dependent_starts_promptly_after_dependency(self, flow):
        """A chain behind a 50ms task must not stall in the 1s event-wait."""

        @flow.function_task
        async def slow():
            await asyncio.sleep(0.05)
            return 1

        @flow.function_task
        async def fast(dep):
            return dep + 1

        start = time.perf_counter()
        result = await fast(slow())
        elapsed = time.perf_counter() - start

        assert result == 2
        # poll-based loop needed ~1.05s here; allow generous CI margin
        assert elapsed < 0.5, f"dependent task stalled: {elapsed:.3f}s"

    @pytest.mark.asyncio
    async def test_sequential_latency_below_poll_interval(self, flow):
        """Per-task round-trip must beat the former 10ms poll floor."""

        @flow.function_task
        async def noop():
            return None

        n = 20
        await noop()  # warmup
        start = time.perf_counter()
        for _ in range(n):
            await noop()
        avg = (time.perf_counter() - start) / n

        # poll-based loop could not go below 10ms per task
        assert avg < 0.008, f"avg task round-trip too slow: {avg * 1000:.1f}ms"

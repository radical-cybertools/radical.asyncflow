"""Scheduler responsiveness: the run loop reacts to component completion
immediately.

- A dependent task starts as soon as its dependency resolves.
- A task round-trip carries no fixed per-pass sleep.
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
        """A dependent starts as soon as its 50ms dependency resolves."""

        @flow.function_task
        async def slow():
            await asyncio.sleep(0.05)
            return 1

        @flow.function_task
        async def fast(dep):
            return dep + 1

        start = time.perf_counter()
        # bounded: nothing polls, so a missed wake-up would hang here
        result = await asyncio.wait_for(fast(slow()), 5)
        elapsed = time.perf_counter() - start

        assert result == 2
        # ~0.05s expected; generous margin for slow CI runners
        assert elapsed < 0.5, f"dependent task stalled: {elapsed:.3f}s"

    @pytest.mark.asyncio
    async def test_sequential_round_trip_latency(self, flow):
        """A no-op task round-trip carries no fixed per-pass sleep."""

        @flow.function_task
        async def noop():
            return None

        async def round_trips(n):
            for _ in range(n):
                await noop()

        # bounded: nothing polls, so a missed wake-up would hang here
        n = 20
        await asyncio.wait_for(round_trips(1), 5)  # warmup
        start = time.perf_counter()
        await asyncio.wait_for(round_trips(n), 5)
        avg = (time.perf_counter() - start) / n

        # ~1ms expected; a 10ms per-pass sleep would exceed the bound
        assert avg < 0.008, f"avg task round-trip too slow: {avg * 1000:.1f}ms"

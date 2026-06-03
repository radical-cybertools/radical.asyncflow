"""Tests for engine internal state correctness across component lifecycle."""

import asyncio

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine


async def _make_engine():
    return await WorkflowEngine.create(backend=NoopExecutionBackend())


# ---------------------------------------------------------------------------
# H1 — self.running is a set
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_running_is_a_set():
    """engine.running must be a set, not a list, for O(1) membership and removal."""
    engine = await _make_engine()
    assert isinstance(engine.running, set)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_running_membership_and_cleanup():
    """Tasks must appear in running while submitted and be removed on completion."""
    engine = await _make_engine()

    @engine.function_task
    async def t():
        return 1

    t()
    # Let the run loop submit the task and the Noop backend resolve it
    await asyncio.sleep(0.1)

    # After Noop resolves immediately, the completed task should have left running
    assert len(engine.running) == 0

    await engine.shutdown()


# ---------------------------------------------------------------------------
# H2 — _clear_internal_records clears resolved, running, timing dicts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_internal_records_resets_resolved_and_running():
    """resolved and running must be empty after shutdown (which calls _clear_internal_records)."""
    engine = await _make_engine()

    @engine.function_task
    async def work():
        return 42

    work()
    await asyncio.sleep(0.1)

    # Confirm something was processed (resolved is non-empty before shutdown)
    assert len(engine.resolved) > 0

    await engine.shutdown()

    assert len(engine.resolved) == 0
    assert len(engine.running) == 0


@pytest.mark.asyncio
async def test_clear_internal_records_resets_timing_dicts():
    """_task_submit_times and _task_start_times must be empty after _clear_internal_records."""
    engine = await _make_engine()

    # Populate the timing dicts directly to simulate telemetry state
    engine._task_submit_times["task.000001"] = 1.0
    engine._task_start_times["task.000001"] = 2.0

    engine._clear_internal_records()

    assert len(engine._task_submit_times) == 0
    assert len(engine._task_start_times) == 0

    await engine.shutdown()

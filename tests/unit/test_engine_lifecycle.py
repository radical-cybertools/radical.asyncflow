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
    """Resolved and running must be empty after shutdown (which calls
    _clear_internal_records)."""
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
    """_task_submit_times and _task_start_times must be empty after
    _clear_internal_records."""
    engine = await _make_engine()

    # Populate the timing dicts directly to simulate telemetry state
    engine._task_submit_times["task.000001"] = 1.0
    engine._task_start_times["task.000001"] = 2.0

    engine._clear_internal_records()

    assert len(engine._task_submit_times) == 0
    assert len(engine._task_start_times) == 0

    await engine.shutdown()


# ---------------------------------------------------------------------------
# Component uids stay unique across engines and shutdowns (#85)
# ---------------------------------------------------------------------------


async def _shut_down_an_engine():
    """Shut down a throwaway engine, as earlier code in the process may have.

    A counter that resets on shutdown would restart at 000001 here, so the tests below
    catch a reset whatever number of uids earlier tests used.
    """
    engine = await WorkflowEngine.create(dry_run=True)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_shutdown_of_one_engine_does_not_reset_another_engines_uids():
    """Shutting down e2 must not make the still-running e1 reuse a uid.

    A reused uid overwrites e1's earlier component record and the new task never
    completes, so the timeout turns a regression into a failure instead of a hang.
    """
    await _shut_down_an_engine()
    e1 = await WorkflowEngine.create(dry_run=True)
    e2 = await WorkflowEngine.create(dry_run=True)

    @e1.function_task
    async def work():
        return 1

    first = work()
    await asyncio.wait_for(first, timeout=5)

    await e2.shutdown()

    second = work()
    try:
        await asyncio.wait_for(second, timeout=5)
    except asyncio.TimeoutError:
        pytest.fail(
            f"e1 reused {second.task['uid']} after e2.shutdown() and the task hung"
        )

    assert second.task["uid"] != first.task["uid"]
    assert set(e1.components) == {first.task["uid"], second.task["uid"]}

    await e1.shutdown()


@pytest.mark.asyncio
async def test_uids_are_unique_across_engines_and_shutdowns():
    """Layers above AsyncFlow identify tasks by uid alone, so no two components in a
    process may share one: not on two live engines, and not on an engine created
    after another has shut down."""
    await _shut_down_an_engine()
    e1 = await WorkflowEngine.create(dry_run=True)
    e2 = await WorkflowEngine.create(dry_run=True)

    @e1.function_task
    async def t1():
        return 1

    @e1.block
    async def b1():
        return 2

    @e2.function_task
    async def t2():
        return 3

    await asyncio.wait_for(asyncio.gather(t1(), b1(), t2()), timeout=5)
    uids = [*e1.components, *e2.components]
    await e2.shutdown()

    e3 = await WorkflowEngine.create(dry_run=True)

    @e3.function_task
    async def t3():
        return 4

    await asyncio.wait_for(t3(), timeout=5)
    uids += [*e3.components]

    assert len(set(uids)) == len(uids) == 4, uids

    await e1.shutdown()
    await e3.shutdown()

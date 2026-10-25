"""Tests for component identity: the uid is on the future when the call returns."""

import asyncio

import pytest

from radical.asyncflow import WorkflowEngine
from radical.asyncflow.workflow_manager import BLOCK, TASK


def _decorate(engine, kind):
    """Return a component of the given kind, defined on ``engine``."""
    if kind == "function_task":

        @engine.function_task
        async def comp():
            return 1

    elif kind == "executable_task":

        @engine.executable_task
        async def comp():
            return "echo hello"

    elif kind == "prompt_task":

        @engine.prompt_task
        async def comp():
            return "What is 2+2?"

    else:

        @engine.block
        async def comp():
            return 1

    return comp


COMPONENTS = [
    ("function_task", TASK),
    ("executable_task", TASK),
    ("prompt_task", TASK),
    ("block", BLOCK),
]


# ---------------------------------------------------------------------------
# uid at call time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind, comp_type", COMPONENTS)
async def test_uid_set_when_call_returns(kind, comp_type):
    """future.id and future.uid exist before the caller awaits anything."""
    engine = await WorkflowEngine.create(dry_run=True)
    comp = _decorate(engine, kind)

    fut = comp()
    # No await between the call and these reads: registration has not run yet.
    assert fut.id
    assert fut.uid == f"{comp_type}.{fut.id}"

    await fut
    await engine.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind, comp_type", COMPONENTS)
async def test_uid_matches_registered_description(kind, comp_type):
    """The uid read at call time is the uid the component is registered under."""
    engine = await WorkflowEngine.create(dry_run=True)
    comp = _decorate(engine, kind)

    fut = comp()
    uid_at_call = fut.uid

    await fut
    assert getattr(fut, comp_type)["uid"] == uid_at_call
    assert engine.components[uid_at_call]["future"] is fut
    await engine.shutdown()


@pytest.mark.asyncio
async def test_uids_distinct_within_one_loop_step():
    """Two tasks called in the same loop step get different uids."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.function_task
    async def t():
        return 1

    a = t()
    b = t()
    assert a.uid != b.uid

    await asyncio.gather(a, b)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_uids_follow_call_order():
    """Task uids follow call order, even when the earlier task's body yields first."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def slow():
        await asyncio.sleep(0.01)
        return "echo hello"

    @engine.function_task
    async def fast():
        return 1

    a = slow()
    b = fast()
    await asyncio.gather(a, b)
    assert int(a.id) < int(b.id)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_uid_kept_when_registration_fails():
    """A call whose registration fails keeps its uid and registers nothing."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def bad():
        return ""

    fut = bad()
    uid_at_call = fut.uid

    with pytest.raises(ValueError, match="non-empty command string"):
        await fut

    assert fut.uid == uid_at_call
    assert uid_at_call not in engine.components
    await engine.shutdown()


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uid_at_call_matches_task_created_event():
    """TaskCreated's task_id is the uid the caller read from the future."""
    pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")

    engine = await WorkflowEngine.create(dry_run=True)
    telemetry = await engine.start_telemetry()
    created = []
    telemetry.subscribe(
        lambda event: (
            created.append(event.task_id) if event.event_type == "TaskCreated" else None
        )
    )

    @engine.function_task
    async def t():
        return 1

    fut = t()
    uid_at_call = fut.uid

    await fut
    await telemetry.stop()  # drains the event queue through the subscribers
    await engine.shutdown()

    assert created == [uid_at_call]

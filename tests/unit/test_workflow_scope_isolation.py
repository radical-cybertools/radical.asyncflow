"""Unit tests for workflow_scope() ContextVar isolation (per-instance fix)."""

import asyncio

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine


async def test_workflow_scope_does_not_leak_to_sibling_engine():
    """engine1.workflow_scope() must not stamp workflow_id onto engine2's tasks."""
    engine1 = await WorkflowEngine.create(backend=NoopExecutionBackend())
    engine2 = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async with engine1.workflow_scope("wf-A"):

        @engine1.function_task
        async def task1(x):
            return x

        @engine2.function_task
        async def task2(x):
            return x

        await task1(1)
        await task2(2)
        await asyncio.sleep(0.05)

    desc1 = next(iter(engine1.components.values()))["description"]
    desc2 = next(iter(engine2.components.values()))["description"]

    assert desc1["workflow_id"] == "wf-A", "engine1 task must carry the workflow_id"
    assert desc2["workflow_id"] is None, "engine2 task must NOT inherit engine1's scope"

    await engine1.shutdown()
    await engine2.shutdown()


async def test_workflow_scope_stamps_all_tasks_on_same_engine():
    """All tasks registered inside workflow_scope() on the same engine get the id."""
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async with engine.workflow_scope("wf-B"):

        @engine.function_task
        async def t1(x):
            return x

        @engine.function_task
        async def t2(x):
            return x

        await t1(1)
        await t2(2)
        await asyncio.sleep(0.05)

    wids = [c["description"]["workflow_id"] for c in engine.components.values()]
    assert all(w == "wf-B" for w in wids), f"Expected all 'wf-B', got {wids}"

    await engine.shutdown()


async def test_workflow_scope_resets_after_exit():
    """After exiting workflow_scope(), tasks on the same engine get workflow_id=None."""
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async with engine.workflow_scope("wf-C"):

        @engine.function_task
        async def inside(x):
            return x

        await inside(1)
        await asyncio.sleep(0.05)

    @engine.function_task
    async def outside(x):
        return x

    await outside(2)
    await asyncio.sleep(0.05)

    comps = list(engine.components.values())
    assert len(comps) == 2
    wids = {c["description"]["name"]: c["description"]["workflow_id"] for c in comps}
    assert wids["inside"] == "wf-C"
    assert wids["outside"] is None

    await engine.shutdown()

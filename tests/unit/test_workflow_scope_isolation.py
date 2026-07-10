"""Unit tests for workflow_scope() ContextVar isolation and workflow_id propagation."""

import asyncio

import pytest

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


# ---------------------------------------------------------------------------
# Explicit workflow_id kwarg (M6)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_explicit_workflow_id_is_recorded():
    """A task called with workflow_id='my-wf' must record that id in comp_desc."""
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    @engine.function_task
    async def tagged_task(x):
        return x

    tagged_task(1, workflow_id="my-wf")
    await asyncio.sleep(0.05)

    comp = next(iter(engine.components.values()))
    assert comp["description"]["workflow_id"] == "my-wf"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_explicit_workflow_id_not_passed_to_function():
    """The workflow_id kwarg must be stripped before being forwarded to the function."""
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())
    received_kwargs = {}

    @engine.function_task
    async def capturing_task(**kwargs):
        received_kwargs.update(kwargs)
        return "ok"

    capturing_task(workflow_id="wf-invisible")
    await asyncio.sleep(0.05)

    assert "workflow_id" not in received_kwargs

    await engine.shutdown()


@pytest.mark.asyncio
async def test_explicit_workflow_id_overrides_context_var():
    """An explicit workflow_id kwarg must take precedence over the active
    workflow_scope."""
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async with engine.workflow_scope("ctx-wf"):

        @engine.function_task
        async def ctx_task(x):
            return x

        ctx_task(1, workflow_id="explicit-wf")
        await asyncio.sleep(0.05)

    comp = next(iter(engine.components.values()))
    assert comp["description"]["workflow_id"] == "explicit-wf"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_workflow_id_falls_back_to_context_var():
    """When no explicit workflow_id is passed, the active workflow_scope value is
    used."""
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async with engine.workflow_scope("scope-wf"):

        @engine.function_task
        async def scoped_task(x):
            return x

        scoped_task(1)
        await asyncio.sleep(0.05)

    comp = next(iter(engine.components.values()))
    assert comp["description"]["workflow_id"] == "scope-wf"

    await engine.shutdown()

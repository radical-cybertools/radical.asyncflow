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

        fut1, fut2 = task1(1), task2(2)
        await fut1
        await fut2

    desc1, desc2 = fut1.task, fut2.task

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

        futs = [inside(1)]
        await futs[0]

    @engine.function_task
    async def outside(x):
        return x

    futs.append(outside(2))
    await futs[1]

    wids = {f.task["name"]: f.task["workflow_id"] for f in futs}
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

    fut = tagged_task(1, workflow_id="my-wf")
    await fut

    assert fut.task["workflow_id"] == "my-wf"

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

        fut = ctx_task(1, workflow_id="explicit-wf")
        await fut

    assert fut.task["workflow_id"] == "explicit-wf"

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

        fut = scoped_task(1)
        await fut

    assert fut.task["workflow_id"] == "scope-wf"

    await engine.shutdown()

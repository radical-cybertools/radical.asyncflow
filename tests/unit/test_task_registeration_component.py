import asyncio

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.workflow_manager import BLOCK, FUNCTION, TASK


@pytest.mark.asyncio
async def test_register_function_task():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    @engine.function_task
    async def dummy_task(x):
        return x + 1

    await dummy_task(1)

    comp_vals = [t["description"]["name"] for t in engine.components.values()]

    assert "dummy_task" in comp_vals

    desc = next(iter(engine.components.values()))["description"]

    assert desc["executable"] is None
    assert desc["function"] is not None


@pytest.mark.asyncio
async def test_handle_flow_component_registration_registers_function():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async def test_func(x):
        return x

    decorated = engine._handle_flow_component_registration(
        func=test_func,
        comp_type=TASK,
        task_type=FUNCTION,
        is_service=False,
        task_backend_specific_kwargs={},
    )

    assert callable(decorated)


@pytest.mark.asyncio
async def test_register_component_adds_task_entry():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async def dummy():
        return "yo"

    comp_desc = {"function": dummy, "args": (), "kwargs": {}, "executable": None}

    engine._register_component(
        comp_fut=asyncio.Future(), comp_type=TASK, comp_desc=comp_desc
    )

    assert engine.components is not None

    uid = next(iter(engine.components.keys()))

    assert TASK in uid


@pytest.mark.asyncio
async def test_register_component_adds_block_entry():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async def dummy_task():
        return "dummy return value"

    async def dummy_block():
        comp_desc = {
            "function": dummy_task,
            "args": (),
            "kwargs": {},
            "executable": None,
        }

        task_future = engine._register_component(
            comp_fut=asyncio.Future(), comp_type=TASK, comp_desc=comp_desc
        )

        return await task_future

    comp_desc = {"function": dummy_block, "args": (), "kwargs": {}, "executable": None}

    block_future = engine._register_component(
        comp_fut=asyncio.Future(), comp_type=BLOCK, comp_desc=comp_desc
    )

    # only 1 block that is not unpacked yet
    assert len(engine.components) == 1

    buid = next(iter(engine.components.keys()))

    # block gets registered first
    assert BLOCK in buid

    await block_future

    # now the block is unpacked test if
    # the task is registered
    assert len(engine.components) == 2


@pytest.mark.asyncio
async def test_dynamic_task_backend_specific_kwargs():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    task_resources = {"ranks": 8}

    @engine.function_task
    async def dummy_task(task_description=task_resources):
        return "dummy return value"

    await dummy_task(task_description={"gpus_per_rank": 2})

    first_value_desc = next(iter(engine.components.values()))["description"]

    assert first_value_desc["task_backend_specific_kwargs"] == {
        "ranks": 8,
        "gpus_per_rank": 2,
    }

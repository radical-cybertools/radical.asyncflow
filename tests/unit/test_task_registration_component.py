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

    fut = dummy_task(1)
    await fut
    desc = fut.task

    assert desc["name"] == "dummy_task"

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

    comp_desc = {
        "uid": engine._assign_uid(prefix=TASK),
        "function": dummy,
        "args": (),
        "kwargs": {},
        "executable": None,
    }

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
            "uid": engine._assign_uid(prefix=TASK),
            "function": dummy_task,
            "args": (),
            "kwargs": {},
            "executable": None,
        }

        task_future = engine._register_component(
            comp_fut=asyncio.Future(), comp_type=TASK, comp_desc=comp_desc
        )

        await task_future
        return comp_desc["uid"]

    comp_desc = {
        "uid": engine._assign_uid(prefix=BLOCK),
        "function": dummy_block,
        "args": (),
        "kwargs": {},
        "executable": None,
    }

    block_future = engine._register_component(
        comp_fut=asyncio.Future(), comp_type=BLOCK, comp_desc=comp_desc
    )

    # only 1 block that is not unpacked yet
    assert len(engine.components) == 1

    block_uid = next(iter(engine.components.keys()))

    # block gets registered first
    assert BLOCK in block_uid

    task_uid = await block_future

    # now the block is unpacked test if
    # the task was registered (it may be released already)
    assert task_uid in engine.components or engine._is_released(task_uid)


@pytest.mark.asyncio
async def test_dynamic_task_backend_specific_kwargs():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    task_resources = {"ranks": 8}

    @engine.function_task
    async def dummy_task(task_description=task_resources):
        return "dummy return value"

    fut = dummy_task(task_description={"gpus_per_rank": 2})
    await fut
    first_value_desc = fut.task

    assert first_value_desc["task_backend_specific_kwargs"] == {
        "ranks": 8,
        "gpus_per_rank": 2,
    }

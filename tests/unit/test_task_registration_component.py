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
async def test_create_component_registers_function():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    async def test_func(x):
        return x

    fut = engine._create_component(
        test_func,
        (1,),
        {},
        is_service=False,
        comp_type=TASK,
        task_type=FUNCTION,
        task_backend_specific_kwargs={},
    )

    assert isinstance(fut, asyncio.Future)
    await fut
    assert fut.task["function"] is test_func
    await engine.shutdown()


@pytest.mark.asyncio
async def test_decorated_sync_function_rejected_at_call():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    def test_func(x):
        return x

    try:
        decorated = engine.function_task(test_func)
        with pytest.raises(
            TypeError, match="Error in decorated function 'test_func'.*must be async"
        ):
            decorated(1)
    finally:
        await engine.shutdown()


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

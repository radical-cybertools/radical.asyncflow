import pytest

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import NoopExecutionBackend
from radical.asyncflow.workflow_manager import TASK, FUNCTION, BLOCK

@pytest.mark.asyncio
async def test_register_function_task():
    engine = WorkflowEngine(backend=NoopExecutionBackend())

    @engine.function_task
    async def dummy_task(x):
        return x + 1
    
    await dummy_task(1)

    assert 'dummy_task' in [t['description']['name'] for t in engine.components.values()]

    desc = next(iter(engine.components.values()))['description']

    assert desc['executable'] is None
    assert desc['function'] is not None


@pytest.mark.asyncio
async def test_handle_flow_component_registration_registers_function():
    engine = WorkflowEngine(backend=NoopExecutionBackend())

    async def test_func(x): return x

    decorated = engine._handle_flow_component_registration(
        func=test_func, 
        comp_type=TASK, 
        task_type=FUNCTION,
        is_service=False,
        task_backend_specific_kwargs={}
    )

    assert callable(decorated)


def test_register_component_adds_task_entry():
    engine = WorkflowEngine(backend=NoopExecutionBackend())

    async def dummy():
        return "yo"
    
    comp_desc = {'function': dummy,
                 'args': (), 'kwargs': {}, 'executable': None}

    engine._register_component(
        comp_fut=dummy,
        comp_type=TASK,
        comp_desc=comp_desc
    )

    assert engine.components is not None

    uid = next(iter(engine.components.keys()))

    assert TASK in uid

@pytest.mark.asyncio
async def test_register_component_adds_block_entry():
    engine = WorkflowEngine(backend=NoopExecutionBackend())

    async def dummy_task():
        return "dummy return value"

    async def dummy_block():
        comp_desc = {'function': dummy_task,
                    'args': (), 'kwargs': {}, 'executable': None}    
        
        task_future = engine._register_component(
            comp_fut=dummy_task,
            comp_type=TASK,
            comp_desc=comp_desc)
            
        return await task_future()

    comp_desc = {'function': dummy_block,
                 'args': (), 'kwargs': {}, 'executable': None}    

    block_future = engine._register_component(
        comp_fut=dummy_block,
        comp_type=BLOCK,
        comp_desc=comp_desc)

    # only 1 block that is not unpacked yet
    assert len(engine.components) == 1

    buid = next(iter(engine.components.keys()))

    # block gets registered first
    assert BLOCK in buid

    await block_future()

    # now the block is unpacked test if
    # the task is registered
    assert len(engine.components) == 2


@pytest.mark.asyncio
async def test_dynamic_task_backend_specific_kwargs():
    engine = WorkflowEngine(backend=NoopExecutionBackend())

    @engine.function_task
    async def dummy_task(task_description={'ranks': 8}):
        return "dummy return value"
    

    await dummy_task(task_description={'gpus_per_rank': 2})

    first_value_desc = next(iter(engine.components.values()))['description']

    assert first_value_desc['task_backend_specific_kwargs'] == {'ranks': 8,
                                                                'gpus_per_rank': 2}


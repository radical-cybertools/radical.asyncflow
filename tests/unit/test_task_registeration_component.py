import pytest

from radical.flow import WorkflowEngine
from radical.flow import NoopExecutionBackend
from radical.flow.workflow_manager import TASK, FUNCTION

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


def test_register_component_adds_entry():
    engine = WorkflowEngine(backend=NoopExecutionBackend())

    async def dummy(): return "yo"
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
    assert 'task.0001' == uid

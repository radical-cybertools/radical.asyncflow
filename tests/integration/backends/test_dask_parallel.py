# tests/integration/test_dask_workflow.py

import pytest
import asyncio
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import DaskExecutionBackend


def test_funnel_dag_with_dask_backend():

    backend = DaskExecutionBackend({'n_workers': 2,
                                    'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    def task1(*args):
        return 1 * 1

    @flow.function_task
    def task2(*args):
        return 2 * 2

    @flow.function_task
    def task3(t1, t2):
        return 3 * 3 * t1 * t2

    t1 = task1()
    t2 = task2()
    
    result = [t.result() for t in [t1, t2]]
    t3 = task3(result[0], result[1]).result()

    assert isinstance(t3, int)
    assert t3 == 36

    flow.shutdown()

@pytest.mark.asyncio
async def test_async_funnel_dag_with_dask_backend():
    backend = DaskExecutionBackend({'n_workers': 2,
                                    'threads_per_worker': 1})
    flow = WorkflowEngine(backend=backend)

    @flow.function_task
    async def task1():
        return 1 * 1

    @flow.function_task
    async def task2():
        return 2 * 2

    @flow.function_task
    async def task3(t1_result, t2_result):
        return 3 * 3 * t1_result * t2_result

    # Launch task1 and task2 in parallel
    t1_future = task1()
    t2_future = task2()

    # Await both in parallel
    t1_result, t2_result = await asyncio.gather(t1_future, t2_future)

    # Call task3 with the gathered results
    t3_future = task3(t1_result, t2_result)

    # Await result of task3
    t3_result = await t3_future

    assert isinstance(t3_result, int)
    assert t3_result == 36

    await flow.shutdown()

def test_dask_backend_rejects_executable_task():
    backend = DaskExecutionBackend({'n_workers': 2,
                                    'threads_per_worker': 1})

    flow = WorkflowEngine(backend=backend)

    with pytest.raises(ValueError, match="DaskExecutionBackend does not support executable tasks"):
        @flow.executable_task
        def bad_task1():
            return '/bin/date'

        not_supported_task1 = bad_task1()
        not_supported_task1.result()

    flow.shutdown()

def test_dask_backend_regular_task_failure():
    backend = DaskExecutionBackend({'n_workers': 2,
                                    'threads_per_worker': 1})

    flow = WorkflowEngine(backend=backend)

    with pytest.raises(RuntimeError, match="Some error"):
        @flow.function_task
        def bad_task2():
            raise RuntimeError('Some error')
    

        not_supported_task2 = bad_task2()
        not_supported_task2.result()
    
    flow.shutdown()

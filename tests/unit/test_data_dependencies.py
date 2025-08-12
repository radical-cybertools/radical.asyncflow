from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from radical.asyncflow import (
    ConcurrentExecutionBackend,
    InputFile,
    OutputFile,
    WorkflowEngine,
)


@pytest.mark.asyncio
async def test_implicit_data_dependencies_trigger():

    backend= await ConcurrentExecutionBackend(ThreadPoolExecutor())
    flow = await WorkflowEngine.create(backend)
    flow.backend.link_implicit_data_deps = MagicMock()

    @flow.function_task
    async def task1(*args):
        return "task result"

    @flow.function_task
    async def task2(*args):
        return "task result"

    t1 = task1()
    t2 = task2(t1)
    print(await t2)

    flow.backend.link_implicit_data_deps.assert_called_once()

@pytest.mark.asyncio
async def test_explicit_data_dependencies_trigger():
    backend= await ConcurrentExecutionBackend(ThreadPoolExecutor())
    flow = await WorkflowEngine.create(backend)
    flow.backend.link_explicit_data_deps = MagicMock()

    @flow.function_task
    async def task1(*args):
        return "task result"

    @flow.function_task
    async def task2(*args):
        return "task result"

    t1 = task1(OutputFile('joshua.txt'))
    t2 = task2(t1, InputFile('joshua.txt'))
    print(await t2)

    flow.backend.link_explicit_data_deps.assert_called_once()

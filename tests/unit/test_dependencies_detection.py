import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.data import InputFile, OutputFile


@pytest.mark.asyncio
async def test_detect_data_dependencies():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())
    a = InputFile("a.txt")
    b = OutputFile("b.txt")

    _, input_deps, output_deps = engine._detect_dependencies([a, b, 42, "string"])

    assert a.filename in input_deps
    assert b.filename in output_deps
    assert 42 not in input_deps
    assert "string" not in input_deps


@pytest.mark.asyncio
async def test_detect_task_dependencies():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    @engine.function_task
    async def task1():
        return 1

    @engine.function_task
    async def task2():
        return 2

    task = task2(task1)
    await task

    task_deps, _, _ = engine._detect_dependencies([task])

    assert len(task_deps) == 1
    assert task1 in task_deps[0]["args"]


@pytest.mark.asyncio
async def test_detect_duplicate_task_dependency():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    @engine.function_task
    async def task1():
        return 1

    task = task1()
    await task

    task_deps, _, _ = engine._detect_dependencies([task, task])

    assert len(task_deps) == 1
    assert task_deps[0]["uid"] == task.task["uid"]


@pytest.mark.asyncio
async def test_detect_kwarg_task_dependencies():
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    @engine.function_task
    async def task1():
        return 1

    @engine.function_task
    async def task2():
        return 2

    fa = task1()
    fb = task2()
    await fa
    await fb

    task_deps, _, _ = engine._detect_dependencies([fa], {"x": fb})
    assert [d["uid"] for d in task_deps] == [fa.task["uid"], fb.task["uid"]]

    task_deps, _, _ = engine._detect_dependencies([fa], {"x": fa})
    assert [d["uid"] for d in task_deps] == [fa.task["uid"]]

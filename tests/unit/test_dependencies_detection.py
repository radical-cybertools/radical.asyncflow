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
    # Dependencies store only uid and name (not the full task dict)
    assert "uid"  in task_deps[0]
    assert "name" in task_deps[0]
    assert set(task_deps[0].keys()) == {"uid", "name"}


@pytest.mark.asyncio
async def test_dependency_does_not_embed_full_task():
    """Dependency entries must be lightweight {uid, name} dicts,
    not recursive copies of the full task description."""
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())

    @engine.function_task
    async def step1():
        return 1

    @engine.function_task
    async def step2(prev):
        return 2

    @engine.function_task
    async def step3(prev):
        return 3

    s1 = step1()
    s2 = step2(s1)
    s3 = step3(s2)
    await s3

    # Each dependency entry should be {uid, name} only
    for uid, deps in engine.dependencies.items():
        for dep in deps:
            assert set(dep.keys()) == {"uid", "name"}, \
                f"dependency of {uid} has unexpected keys: {dep.keys()}"
            assert isinstance(dep["uid"], str)
            assert isinstance(dep["name"], str)

import pytest
import asyncio
import pytest_asyncio
import time

from concurrent.futures import Future as SyncFuture

from radical.asyncflow.errors import DependencyFailure
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ThreadExecutionBackend


@pytest.fixture
def engine():
    backend = ThreadExecutionBackend({})
    engine = WorkflowEngine(backend=backend)
    yield engine
    engine.shutdown(skip_execution_backend=True)

@pytest_asyncio.fixture
async def async_engine():
    backend = ThreadExecutionBackend({})
    engine = WorkflowEngine(backend=backend)
    yield engine
    await asyncio.sleep(0)  # allow any pending tasks to finish
    await engine.shutdown(skip_execution_backend=True)


def test_dependency_failure_exception_creation(engine):
    @engine.function_task
    def failing_task():
        raise ValueError("Original task failure")

    @engine.function_task  
    def dependent_task(dep):
        return f"Result from {dep}"

    t1 = failing_task()
    time.sleep(0.1)

    assert t1.exception() is not None
    assert isinstance(t1.exception(), ValueError)
    assert str(t1.exception()) == "Original task failure"

    t2 = dependent_task(t1)
    time.sleep(0.2)

    assert t2.exception() is not None
    assert isinstance(t2.exception(), DependencyFailure)

    dep_failure = t2.exception()
    assert "Cannot execute 'dependent_task' due to dependency failure" in str(dep_failure)
    assert "failing_task" in dep_failure.failed_dependencies
    assert isinstance(dep_failure.root_cause, ValueError)
    assert str(dep_failure.root_cause) == "Original task failure"


def test_multiple_dependency_failures(engine):
    @engine.function_task
    def failing_task1():
        raise ValueError("Task 1 failed")

    @engine.function_task
    def failing_task2():
        raise RuntimeError("Task 2 failed") 

    @engine.function_task
    def dependent_task(dep1, dep2):
        return f"Results: {dep1}, {dep2}"

    t1 = failing_task1()
    t2 = failing_task2()
    time.sleep(0.1)

    t3 = dependent_task(t1, t2)
    time.sleep(0.2)

    assert t3.exception() is not None
    assert isinstance(t3.exception(), DependencyFailure)

    dep_failure = t3.exception()
    assert len(dep_failure.failed_dependencies) == 2
    assert "failing_task1" in dep_failure.failed_dependencies
    assert "failing_task2" in dep_failure.failed_dependencies
    assert isinstance(dep_failure.root_cause, (ValueError, RuntimeError))


def test_chain_of_dependency_failures(engine):
    @engine.function_task
    def task1():
        raise ValueError("Root failure")

    @engine.function_task
    def task2(dep):
        return f"Task2 result: {dep}"

    @engine.function_task
    def task3(dep):
        return f"Task3 result: {dep}"

    @engine.function_task
    def task4(dep):
        return f"Task4 result: {dep}"

    t1 = task1()
    t2 = task2(t1)
    t3 = task3(t2) 
    t4 = task4(t3)

    time.sleep(0.3)

    for task_future in [t2, t3, t4]:
        assert task_future.exception() is not None
        assert isinstance(task_future.exception(), DependencyFailure)

    for task_future in [t2, t3, t4]:
        dep_failure = task_future.exception()
        root_cause = dep_failure.root_cause
        while isinstance(root_cause, DependencyFailure) and root_cause.root_cause:
            root_cause = root_cause.root_cause
        assert isinstance(root_cause, ValueError)
        assert str(root_cause) == "Root failure"


def test_partial_dependency_failure(engine):
    @engine.function_task
    def successful_task():
        return "Success!"

    @engine.function_task
    def failing_task():
        raise ValueError("This task failed")

    @engine.function_task
    def dependent_task(good_dep, bad_dep):
        return f"Results: {good_dep}, {bad_dep}"

    t1 = successful_task()
    t2 = failing_task()
    time.sleep(0.1)

    assert t1.result() == "Success!"
    assert isinstance(t2.exception(), ValueError)

    t3 = dependent_task(t1, t2)
    time.sleep(0.2)
    
    assert isinstance(t3.exception(), DependencyFailure)
    dep_failure = t3.exception()
    assert "failing_task" in dep_failure.failed_dependencies
    assert "successful_task" not in dep_failure.failed_dependencies


def test_block_dependency_failure(engine):
    @engine.function_task
    def failing_task():
        raise RuntimeError("Task failure")

    @engine.block
    def dependent_block(dep):
        return f"Block result: {dep}"

    t1 = failing_task()
    time.sleep(0.1)

    b1 = dependent_block(t1)
    time.sleep(0.2)

    assert isinstance(b1.exception(), DependencyFailure)
    dep_failure = b1.exception()
    assert isinstance(dep_failure.root_cause, RuntimeError)
    assert str(dep_failure.root_cause) == "Task failure"


@pytest.mark.asyncio
async def test_async_dependency_failure_propagation(async_engine):
    @async_engine.function_task
    async def async_failing_task():
        await asyncio.sleep(0.01)
        raise ValueError("Async task failed")

    @async_engine.function_task
    async def async_dependent_task(dep):
        await asyncio.sleep(0.01)
        return f"Async result: {dep}"

    t1 = async_failing_task()
    await asyncio.sleep(0.1)

    t2 = async_dependent_task(t1)
    await asyncio.sleep(0.2)

    assert isinstance(t2.exception(), DependencyFailure)
    dep_failure = t2.exception()
    assert isinstance(dep_failure.root_cause, ValueError)
    assert str(dep_failure.root_cause) == "Async task failed"


def test_handle_task_failure_with_dependency_failure(engine):
    mock_task = {
        'uid': 'test.000001',
        'name': 'test_task',
        'exception': ValueError("Original error")
    }

    mock_future = SyncFuture()

    dep_failure = DependencyFailure(
        message="Test dependency failure",
        failed_dependencies=["dep1", "dep2"],
        root_cause=ValueError("Root cause")
    )

    engine.components[mock_task['uid']] = {
        'future': mock_future,
        'description': {'function': lambda: None}
    }

    engine.handle_task_failure(mock_task, mock_future, dep_failure)

    assert mock_future.exception() is dep_failure
    assert isinstance(mock_future.exception(), DependencyFailure)


def test_exception_chaining_in_dependency_failure(engine):
    @engine.function_task
    def original_failing_task():
        raise ValueError("Original error")

    @engine.function_task
    def dependent_task(dep):
        return f"Result: {dep}"

    t1 = original_failing_task()
    time.sleep(0.1)

    t2 = dependent_task(t1)
    time.sleep(0.2)

    dep_failure = t2.exception()
    assert isinstance(dep_failure, DependencyFailure)
    assert dep_failure.__cause__ is not None
    assert isinstance(dep_failure.__cause__, ValueError)
    assert str(dep_failure.__cause__) == "Original error"


def test_dependency_failure_string_representation():
    root_cause = ValueError("Root error")
    dep_failure = DependencyFailure(
        message="Cannot execute task",
        failed_dependencies=["task1", "task2"],
        root_cause=root_cause
    )

    str_repr = str(dep_failure)
    assert "Cannot execute task" in str_repr
    assert "task1" in dep_failure.failed_dependencies
    assert "task2" in dep_failure.failed_dependencies
    assert isinstance(dep_failure.root_cause, ValueError)
    assert "Root error" in str(dep_failure.root_cause)

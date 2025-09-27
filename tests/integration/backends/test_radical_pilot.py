import asyncio

import pytest
import pytest_asyncio

from radical.asyncflow import (
    InputFile,
    OutputFile,
    RadicalExecutionBackend,
    WorkflowEngine,
)


@pytest_asyncio.fixture(scope="function")
async def backend():
    """Initialize RadicalExecutionBackend once for each test."""
    be = await RadicalExecutionBackend({"resource": "local.localhost"})
    yield be
    await be.shutdown()


@pytest.mark.asyncio
async def test_async_bag_of_tasks(backend):
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def echo_task(i):
        return f'/bin/echo "Task {i} executed at" && /bin/date'

    bag_size = 10
    tasks = [echo_task(i) for i in range(bag_size)]
    results = await asyncio.gather(*tasks)

    assert len(results) == bag_size
    for _, result in enumerate(results):
        assert result is not None
        assert isinstance(result, (str, bytes))

    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_radical_backend_reject_service_task_function(backend):
    flow = await WorkflowEngine.create(backend=backend)

    with pytest.raises(
        ValueError,
        match="RadicalExecutionBackend does not support function service tasks",
    ):

        @flow.function_task(service=True)
        async def bad_task2():
            return True

        await bad_task2()

    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_radical_backend_reject_function_task_with_raptor_off(backend):
    flow = await WorkflowEngine.create(backend=backend)

    with pytest.raises(RuntimeError):

        @flow.function_task
        async def bad_task3():
            return True

        await bad_task3()

    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_radical_backend_implicit_data(backend):
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task1(*args):
        return 'echo "This is a file from task1" > t1_output.txt'

    @flow.executable_task
    async def task2(*args):
        return "/bin/cat t1_output.txt"

    t1 = task1()
    t2 = task2(t1)

    assert await t2 == "This is a file from task1\n"
    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_radical_backend_explicit_data(backend):
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task1(*args):
        return 'echo "This is a file from task1" > t1_output.txt'

    @flow.executable_task
    async def task2(*args):
        return "/bin/cat t1_output.txt"

    t1 = task1(OutputFile("t1_output.txt"))
    t2 = task2(t1, InputFile("t1_output.txt"))

    assert await t2 == "This is a file from task1\n"
    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_radical_backend_input_data_staging(backend):
    flow = await WorkflowEngine.create(backend=backend)

    with open("t1_input.txt", "w") as f:
        f.write("This is a file staged in to task1\n")

    @flow.executable_task
    async def task1(*args):
        return "/bin/cat t1_input.txt"

    t1 = task1(InputFile("t1_input.txt"))

    assert await t1 == "This is a file staged in to task1\n"
    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_async_cancel_tasks(backend):
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task():
        return "/bin/sleep 10"

    t1 = task()
    t2 = task()

    await asyncio.sleep(1)

    t1.cancel()
    t2.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1
    with pytest.raises(asyncio.CancelledError):
        await t2

    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_async_cancel_before_start(backend):
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def slow_task():
        return "/bin/sleep 5"

    @flow.executable_task
    async def fast_task():
        return '/bin/echo "done"'

    t1 = slow_task()
    t2 = slow_task()

    await asyncio.sleep(1)
    t2.cancel()

    await t1
    with pytest.raises(asyncio.CancelledError):
        await t2

    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_async_cancel_after_completion(backend):
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def quick_task():
        return "/bin/echo 'done'"

    t = quick_task()
    result = await t
    assert result.strip() == "done"

    t.cancel()
    t_result = await t
    assert t_result.strip() == "done"

    await flow.shutdown(skip_execution_backend=True)


@pytest.mark.asyncio
async def test_async_cancel_one_of_many(backend):
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task(n):
        return '/bin/sleep 5 && /bin/echo "2"'

    t1 = task(1)
    t2 = task(2)

    await asyncio.sleep(1)
    t1.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1

    str_result = await t2
    assert int(str_result) == 2

    await flow.shutdown()

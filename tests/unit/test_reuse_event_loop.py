"""LocalExecutionBackend(reuse_event_loop=True): one event loop per worker thread.

By default every async function task runs with ``asyncio.run``, which creates and
closes an event loop per task. Its clean-up calls ``asyncio.all_tasks()``, which walks
every asyncio Task of the process, so a task costs more the more Tasks are alive. With
``reuse_event_loop=True`` each worker thread keeps one loop, and the backend does the
clean-up that needs no such walk: it cancels the tasks a call left running.
"""

import asyncio
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine


async def _loop_id():
    return id(asyncio.get_running_loop())


async def _engine(**kwargs):
    backend = await LocalExecutionBackend(ThreadPoolExecutor(max_workers=1), **kwargs)
    return backend, await WorkflowEngine.create(backend=backend)


async def test_default_runs_each_task_on_a_new_loop():
    backend, flow = await _engine()
    try:
        task = flow.function_task(_loop_id)
        first, second = await task(), await task()
        assert first != second  # asyncio.run: a loop per task
    finally:
        await flow.shutdown()


async def test_reuse_runs_a_thread_s_tasks_on_one_loop():
    backend, flow = await _engine(reuse_event_loop=True)
    try:
        task = flow.function_task(_loop_id)
        first, second = await task(), await task()
        assert first == second  # one worker thread, one loop
    finally:
        await flow.shutdown()


async def test_reuse_cancels_what_a_task_left_running():
    cancelled = threading.Event()

    async def leaves_a_task():
        async def forever():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        asyncio.get_running_loop().create_task(forever())
        await asyncio.sleep(0)  # let it start
        return "done"

    backend, flow = await _engine(reuse_event_loop=True)
    try:
        assert await flow.function_task(leaves_a_task)() == "done"
        # cancelled when the call returned, as asyncio.run does, not during a later task
        assert cancelled.is_set()
    finally:
        await flow.shutdown()


async def test_reuse_reports_an_error_a_left_task_raised():
    seen = []

    async def leaves_a_failing_task():
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, ctx: seen.append(ctx.get("exception")))

        async def fails_on_cancel():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise ValueError("raised while cancelled") from None

        loop.create_task(fails_on_cancel())
        await asyncio.sleep(0)
        return "done"

    backend, flow = await _engine(reuse_event_loop=True)
    try:
        assert await flow.function_task(leaves_a_failing_task)() == "done"
        assert any(isinstance(e, ValueError) for e in seen)
    finally:
        await flow.shutdown()


async def test_reuse_survives_a_task_that_closes_its_loop():
    async def closes_its_loop():
        loop = asyncio.get_running_loop()
        loop.call_soon(loop.stop)  # stops the loop under run_until_complete
        await asyncio.sleep(0.01)
        return "unreached"

    backend, flow = await _engine(reuse_event_loop=True)
    try:
        with pytest.raises(RuntimeError):
            await flow.function_task(closes_its_loop)()
        assert isinstance(await flow.function_task(_loop_id)(), int)  # a new loop
    finally:
        await flow.shutdown()


async def test_reuse_leaves_no_loop_set_between_tasks():
    def current_loop_in_thread(executor):
        def probe():
            try:
                return asyncio.get_event_loop_policy().get_event_loop()
            except RuntimeError:
                return None

        return executor.submit(probe).result()

    executor = ThreadPoolExecutor(max_workers=1)
    backend = await LocalExecutionBackend(executor, reuse_event_loop=True)
    flow = await WorkflowEngine.create(backend=backend)
    try:
        await flow.function_task(_loop_id)()
        loop = current_loop_in_thread(executor)
        assert loop is None or not loop.is_running()
    finally:
        await flow.shutdown()


async def test_shutdown_closes_the_threads_loops():
    backend, flow = await _engine(reuse_event_loop=True)
    await flow.function_task(_loop_id)()
    loops = backend._thread_loops.loops()
    assert loops and not any(lp.is_closed() for lp in loops)
    await flow.shutdown()
    assert all(lp.is_closed() for lp in loops)


async def test_reuse_with_processes_keeps_asyncio_run():
    backend = await LocalExecutionBackend(
        ProcessPoolExecutor(max_workers=1), reuse_event_loop=True
    )
    flow = await WorkflowEngine.create(backend=backend)
    try:
        assert isinstance(await flow.function_task(_loop_id)(), int)
        assert backend._thread_loops is None  # processes hold few Tasks: unchanged
    finally:
        await flow.shutdown()


async def test_reuse_is_not_slowed_by_live_tasks():
    """With many Tasks alive in the process, a task costs about the same as without."""
    import time

    async def noop():
        return None

    stop = asyncio.Event()
    holders = [asyncio.ensure_future(stop.wait()) for _ in range(20_000)]
    try:
        timings = {}
        for reuse in (False, True):
            backend, flow = await _engine(reuse_event_loop=reuse)
            try:
                task = flow.function_task(noop)
                await task()
                t = time.perf_counter()
                for _ in range(50):
                    await task()
                timings[reuse] = time.perf_counter() - t
            finally:
                await flow.shutdown()
        assert timings[True] < timings[False] / 2, timings
    finally:
        stop.set()
        await asyncio.gather(*holders)

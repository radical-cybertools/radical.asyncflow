"""LocalExecutionBackend(reuse_event_loop=True): one event loop per worker thread.

By default every async function task runs with ``asyncio.run``, which creates and
closes an event loop per task. Its clean-up calls ``asyncio.all_tasks()``, which walks
every asyncio Task of the process, so a task costs more the more Tasks are alive. With
``reuse_event_loop=True`` each worker thread keeps one loop, and the backend does the
clean-up that needs no such walk: it cancels the tasks a call left running.
"""

import asyncio
import contextvars
import gc
import threading
import weakref
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine


async def _loop_id():
    return id(asyncio.get_running_loop())


async def _engine(**kwargs):
    backend = await LocalExecutionBackend(ThreadPoolExecutor(max_workers=1), **kwargs)
    return backend, await WorkflowEngine.create(backend=backend)


def _loop_recorder():
    """A task that records its running loop, and the list it records into.

    The list keeps every loop alive, so loops compare by identity: comparing their
    ``id()`` would not do, as CPython gives a freed loop's id to the next object.
    """
    seen = []

    async def record_loop():
        seen.append(asyncio.get_running_loop())
        return len(seen)

    return seen, record_loop


async def test_default_runs_each_task_on_a_new_loop():
    seen, record_loop = _loop_recorder()
    backend, flow = await _engine()
    try:
        task = flow.function_task(record_loop)
        await task()
        await task()
        assert len(seen) == 2
        assert seen[0] is not seen[1]  # asyncio.run: a loop per task
    finally:
        await flow.shutdown()


async def test_reuse_runs_a_thread_s_tasks_on_one_loop():
    seen, record_loop = _loop_recorder()
    backend, flow = await _engine(reuse_event_loop=True)
    try:
        task = flow.function_task(record_loop)
        await task()
        await task()
        assert len(seen) == 2
        assert seen[0] is seen[1]  # one worker thread, one loop
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


async def test_reuse_survives_a_task_that_stops_its_loop():
    async def stops_its_loop():
        loop = asyncio.get_running_loop()
        loop.call_soon(loop.stop)  # stops the loop under run_until_complete
        await asyncio.sleep(0.01)
        return "unreached"

    backend, flow = await _engine(reuse_event_loop=True)
    try:
        with pytest.raises(RuntimeError):
            await flow.function_task(stops_its_loop)()
        assert isinstance(
            await flow.function_task(_loop_id)(), int
        )  # the next task runs
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
    """With many Tasks alive in the process, a worker's call on its own loop costs about
    what it costs without them; ``asyncio.run`` walks them all."""
    import time

    async def noop():
        return None

    def timed(run, n=200):
        t = time.perf_counter()
        for _ in range(n):
            run(noop, (), {})
        return time.perf_counter() - t

    stop = asyncio.Event()
    holders = [asyncio.ensure_future(stop.wait()) for _ in range(20_000)]
    backend = await LocalExecutionBackend(
        ThreadPoolExecutor(max_workers=1), reuse_event_loop=True
    )
    try:
        await asyncio.sleep(0)
        loop = asyncio.get_running_loop()
        per_task = await loop.run_in_executor(
            backend.executor, timed, LocalExecutionBackend._run_in_thread
        )
        reused = await loop.run_in_executor(
            backend.executor, timed, backend._run_on_thread_loop
        )
        assert reused < per_task / 3, (reused, per_task)
    finally:
        stop.set()
        await asyncio.gather(*holders)
        await backend.shutdown()


_var = contextvars.ContextVar("_var", default="default")


async def test_reuse_keeps_context_variables_to_their_task():
    async def sets():
        _var.set("set by an earlier task")
        return _var.get()

    async def reads():
        return _var.get()

    backend, flow = await _engine(reuse_event_loop=True)
    try:
        assert await flow.function_task(sets)() == "set by an earlier task"
        assert (
            await flow.function_task(reads)() == "default"
        )  # each task copies the context
    finally:
        await flow.shutdown()


async def test_reuse_keeps_timeouts_and_cancellation_working():
    seen, record_loop = _loop_recorder()

    async def times_out():
        try:
            async with asyncio.timeout(0.01):
                await asyncio.sleep(10)
        except TimeoutError:
            pass
        try:
            await asyncio.wait_for(asyncio.sleep(10), 0.01)
        except TimeoutError:
            return await record_loop()

    async def slow():
        await asyncio.sleep(0.2)
        return await record_loop()

    backend, flow = await _engine(reuse_event_loop=True)
    try:
        assert await flow.function_task(times_out)() == 1
        # a function task cancelled by the engine while its thread runs it: the thread
        # finishes it, as with asyncio.run, and the next task runs on the same loop
        running = flow.function_task(slow)()
        await asyncio.sleep(0.05)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert await flow.function_task(record_loop)() == 3
        assert seen[0] is seen[1] is seen[2]
    finally:
        await flow.shutdown()


async def test_reuse_gives_each_worker_thread_one_loop_of_its_own():
    pairs = []

    async def record():
        await asyncio.sleep(0.001)
        pairs.append((threading.get_ident(), asyncio.get_running_loop()))

    backend = await LocalExecutionBackend(
        ThreadPoolExecutor(max_workers=4), reuse_event_loop=True
    )
    flow = await WorkflowEngine.create(backend=backend)
    try:
        task = flow.function_task(record)
        await asyncio.gather(*[task() for _ in range(200)])
        loops_of = {}
        threads_of = {}
        for thread, loop in pairs:
            loops_of.setdefault(thread, set()).add(id(loop))
            threads_of.setdefault(id(loop), set()).add(thread)
        assert len(pairs) == 200
        assert all(len(loops) == 1 for loops in loops_of.values())
        assert all(len(threads) == 1 for threads in threads_of.values())
        assert len(backend._thread_loops.loops()) == len(loops_of)
    finally:
        await flow.shutdown()


async def test_reuse_does_not_keep_a_task_s_finished_tasks_alive():
    async def creates_many():
        loop = asyncio.get_running_loop()
        refs = [weakref.ref(loop.create_task(asyncio.sleep(0))) for _ in range(1000)]
        await asyncio.sleep(0.01)  # they all finish
        gc.collect()
        return sum(ref() is not None for ref in refs)

    backend, flow = await _engine(reuse_event_loop=True)
    try:
        # asyncio.run does not keep them either: a long task may create many
        assert await flow.function_task(creates_many)() == 0
    finally:
        await flow.shutdown()


class _TaggingPolicy(asyncio.DefaultEventLoopPolicy):
    """Creates loops whose task factory tags the tasks it makes."""

    def new_event_loop(self):
        loop = super().new_event_loop()

        def tag(loop, coro, **kwargs):
            task = asyncio.Task(coro, loop=loop, **kwargs)
            task.tagged = True
            return task

        loop.set_task_factory(tag)
        return loop


async def test_reuse_keeps_the_task_factory_a_loop_is_created_with():
    cancelled = threading.Event()

    async def child_is_tagged():
        async def forever():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        child = asyncio.get_running_loop().create_task(forever())
        await asyncio.sleep(0)
        return getattr(child, "tagged", False)

    previous = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(_TaggingPolicy())
    try:
        backend, flow = await _engine(reuse_event_loop=True)
        try:
            # as with asyncio.run, whose loop comes from the policy (eager tasks, say)
            assert await flow.function_task(child_is_tagged)() is True
            assert cancelled.is_set()  # and what it left running is still cancelled
        finally:
            await flow.shutdown()
    finally:
        asyncio.set_event_loop_policy(previous)


async def test_reuse_resets_loop_settings_a_task_changed():
    cancelled = threading.Event()

    async def changes_its_loop():
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)
        loop.set_exception_handler(lambda _loop, _ctx: None)

    async def leaves_a_task():
        async def forever():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        asyncio.get_running_loop().create_task(forever())
        await asyncio.sleep(0)
        return asyncio.get_running_loop().get_exception_handler()

    backend, flow = await _engine(reuse_event_loop=True)
    try:
        await flow.function_task(changes_its_loop)()
        # a later task starts as on a new loop: what it leaves is still cancelled, and
        # the earlier task's exception handler is gone
        assert await flow.function_task(leaves_a_task)() is None
        assert cancelled.is_set()
    finally:
        await flow.shutdown()


async def test_shutdown_can_be_called_again():
    backend, flow = await _engine(reuse_event_loop=True)
    await flow.function_task(_loop_id)()
    loops = backend._thread_loops.loops()
    await flow.shutdown()
    await backend.shutdown()  # an engine retries a failed shutdown on every backend
    assert loops and all(lp.is_closed() for lp in loops)


async def test_the_loops_cannot_be_run_or_closed_on_an_event_loop():
    backend, flow = await _engine(reuse_event_loop=True)
    try:
        await flow.function_task(_loop_id)()
        coro = _loop_id()
        with pytest.raises(RuntimeError):
            backend._thread_loops.run(coro)  # as asyncio.run refuses
        coro.close()
        with pytest.raises(RuntimeError):
            backend._thread_loops.close()  # it would block, and skip the clean-up
        assert not any(lp.is_closed() for lp in backend._thread_loops.loops())
    finally:
        await flow.shutdown()

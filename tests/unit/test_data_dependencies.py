import asyncio
from unittest.mock import MagicMock

import pytest

from radical.asyncflow import (
    InputFile,
    LocalExecutionBackend,
    NoopExecutionBackend,
    OutputFile,
    WorkflowEngine,
)


async def _linking_backend(implicit=False, explicit=False):
    """LocalExecutionBackend declaring the given data-linking capabilities.

    The capabilities are read when the backend is attached, so they must be set before
    WorkflowEngine.create().
    """
    backend = await LocalExecutionBackend()
    backend.supports_implicit_data_linking = implicit
    backend.supports_explicit_data_linking = explicit
    return backend


@pytest.mark.asyncio
async def test_implicit_data_dependencies_trigger():
    backend = await _linking_backend(implicit=True)
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
async def test_implicit_data_dependencies_single_link_for_duplicates():
    backend = await _linking_backend(implicit=True)
    flow = await WorkflowEngine.create(backend)
    flow.backend.link_implicit_data_deps = MagicMock()

    @flow.function_task
    async def task1(*args):
        await asyncio.sleep(0.3)  # still pending when task2 registers
        return "task result"

    @flow.function_task
    async def task2(*args):
        return "task result"

    t1 = task1()
    t2 = task2(t1, t1)
    await asyncio.wait_for(t2, timeout=5)

    flow.backend.link_implicit_data_deps.assert_called_once()


@pytest.mark.asyncio
async def test_explicit_data_dependencies_trigger():
    backend = await _linking_backend(explicit=True)
    flow = await WorkflowEngine.create(backend)
    flow.backend.link_explicit_data_deps = MagicMock(
        return_value={"target": "joshua.txt"}
    )

    @flow.function_task
    async def task1(*args):
        return "task result"

    @flow.function_task
    async def task2(*args):
        return "task result"

    t1 = task1(OutputFile("joshua.txt"))
    t2 = task2(t1, InputFile("joshua.txt"))
    print(await t2)

    flow.backend.link_explicit_data_deps.assert_called_once()


@pytest.mark.asyncio
async def test_explicit_file_dependency_runs_on_non_linking_backend(tmp_path):
    """An OutputFile -> InputFile dependency on a backend that does not link data runs
    normally and does not shut the engine down."""
    flow = await WorkflowEngine.create(
        backend=await LocalExecutionBackend(), work_dir=str(tmp_path)
    )

    @flow.executable_task
    async def produce(*args):
        return "echo produced"

    @flow.executable_task
    async def consume(*args):
        return "echo consumed"

    @flow.function_task
    async def unrelated():
        return "ok"

    try:
        p = produce(OutputFile("out.txt"))
        c = consume(p, InputFile("out.txt"))

        assert (await asyncio.wait_for(c, timeout=5)).strip() == "consumed"
        assert not flow._shutdown_event.is_set()
        assert await asyncio.wait_for(unrelated(), timeout=5) == "ok"
    finally:
        await flow.shutdown()


async def _run_mixed_dependencies(flow):
    """Run a consumer with one plain dependency (implicit linking candidate) and one
    dependency producing the file the consumer reads (explicit linking)."""

    @flow.function_task
    async def plain():
        return "plain"

    @flow.function_task
    async def producer(*args):
        return "file"

    @flow.function_task
    async def consumer(*args):
        return "done"

    d_plain = plain()
    d_file = producer(OutputFile("a.txt"))
    result = await asyncio.wait_for(
        consumer(d_plain, d_file, InputFile("a.txt")), timeout=5
    )
    return d_file, result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "implicit,explicit",
    [(True, False), (False, True), (True, True), (False, False)],
    ids=["implicit-only", "explicit-only", "both", "neither"],
)
async def test_capabilities_gate_hooks_independently(implicit, explicit, tmp_path):
    backend = await _linking_backend(implicit=implicit, explicit=explicit)
    flow = await WorkflowEngine.create(
        backend=backend, implicit_data=True, work_dir=str(tmp_path)
    )
    flow.backend.link_implicit_data_deps = MagicMock()
    flow.backend.link_explicit_data_deps = MagicMock(return_value={"target": "a.txt"})

    try:
        _, result = await _run_mixed_dependencies(flow)
    finally:
        await flow.shutdown()

    assert result == "done"
    assert flow.backend.link_implicit_data_deps.call_count == (1 if implicit else 0)
    assert flow.backend.link_explicit_data_deps.call_count == (1 if explicit else 0)


@pytest.mark.asyncio
async def test_both_capabilities_stage_input_once(tmp_path):
    """With explicit linking, an input file produced by a dependency is linked once from
    that dependency and not staged again from an external path."""
    backend = await _linking_backend(implicit=True, explicit=True)
    flow = await WorkflowEngine.create(backend=backend, work_dir=str(tmp_path))
    flow.backend.link_implicit_data_deps = MagicMock()
    flow.backend.link_explicit_data_deps = MagicMock(return_value={"target": "a.txt"})

    try:
        d_file, result = await _run_mixed_dependencies(flow)
    finally:
        await flow.shutdown()

    assert result == "done"
    flow.backend.link_explicit_data_deps.assert_called_once()
    call = flow.backend.link_explicit_data_deps.call_args.kwargs
    assert call["src_task"]["uid"] == d_file.task["uid"]
    assert call["file_name"] == "a.txt"
    assert "file_path" not in call


class _LegacyBackend:
    """A backend predating the data-linking capability attributes."""

    name = "legacy"

    def __init__(self):
        self._inner = NoopExecutionBackend()
        self.attached_to = []
        self.link_implicit_data_deps = MagicMock()
        self.link_explicit_data_deps = MagicMock()

    def register_callback(self, func):
        self._inner.register_callback(func)

    def get_task_states_map(self):
        return self._inner.get_task_states_map()

    async def submit_tasks(self, tasks):
        await self._inner.submit_tasks(tasks)

    async def cancel_task(self, uid):
        return await self._inner.cancel_task(uid)

    async def shutdown(self):
        await self._inner.shutdown()


@pytest.mark.asyncio
async def test_backend_without_capability_attributes_is_non_linking(tmp_path):
    backend = _LegacyBackend()
    assert not hasattr(backend, "supports_implicit_data_linking")
    assert not hasattr(backend, "supports_explicit_data_linking")

    flow = await WorkflowEngine.create(
        backend=backend, implicit_data=True, work_dir=str(tmp_path)
    )
    try:
        _, result = await _run_mixed_dependencies(flow)
    finally:
        await flow.shutdown()

    assert result == "Dummy Output"  # NoopExecutionBackend's placeholder result
    backend.link_implicit_data_deps.assert_not_called()
    backend.link_explicit_data_deps.assert_not_called()

"""The checks of tests/harness.py that tests/conftest.py runs around every test."""

import asyncio
import logging

import pytest

from radical.asyncflow import WorkflowEngine
from tests.harness import (
    EAGER_TASKS_SUPPORTED,
    eager_task_failure,
    engine_violations,
    log_violations,
)


async def test_test_loop_has_the_chosen_task_factory(pytestconfig):
    factory = asyncio.get_running_loop().get_task_factory()
    if pytestconfig.getoption("eager_tasks"):
        assert factory is asyncio.eager_task_factory
    else:
        assert factory is None


def test_asyncio_run_has_the_chosen_task_factory(pytestconfig):
    async def factory():
        return asyncio.get_running_loop().get_task_factory()

    expected = (
        asyncio.eager_task_factory if pytestconfig.getoption("eager_tasks") else None
    )
    assert asyncio.run(factory()) is expected


@pytest.mark.skipif(not EAGER_TASKS_SUPPORTED, reason="needs Python 3.12")
def test_eager_task_failures_match_parametrized_ids():
    base = (
        "tests/unit/test_foreign_engine_future.py"
        "::test_foreign_engine_future_is_rejected"
    )
    assert eager_task_failure(f"{base}[keyword-pending-block-block]")
    assert not eager_task_failure(f"{base}[keyword-registered-block-block]")
    assert not eager_task_failure("tests/unit/test_async.py::test_async_flow")


async def test_records_after_shutdown_are_violations(tmp_path):
    engine = await WorkflowEngine.create(dry_run=True, work_dir=str(tmp_path))
    await engine.shutdown()
    assert engine_violations([engine], {}) == []

    engine.components["task.late"] = {}
    engine._dependents_map["task.000001"].add("task.late")
    found = engine_violations([engine], {})
    assert [line.split(":")[1] for line in found] == [
        " components keeps 1 after shutdown",
        " _dependents_map keeps 1 after shutdown",
    ]
    # Kept by design after shutdown(skip_execution_backend=True)
    assert engine_violations([engine], {}, kept={engine}) == []
    engine.components.clear()
    engine._dependents_map.clear()


async def test_unfinished_futures_are_violations(tmp_path):
    engine = await WorkflowEngine.create(dry_run=True, work_dir=str(tmp_path))
    await engine.shutdown()
    fut = asyncio.get_running_loop().create_future()
    fut.uid = "task.000042"

    found = engine_violations([engine], {engine: [fut]})
    assert found == [
        f"engine {engine.uid}: 1 futures not done after shutdown: task.000042"
    ]
    fut.cancel()


def test_log_records_on_one_line():
    record = logging.LogRecord(
        "asyncio", logging.ERROR, __file__, 1, "Future exception\nfuture: <F>", (), None
    )
    assert log_violations([record]) == [
        "ERROR from asyncio: Future exception future: <F>"
    ]

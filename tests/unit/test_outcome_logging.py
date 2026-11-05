"""Failed and cancelled components show at INFO and above; successful ones do not."""

import asyncio
import logging

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine

LOGGER = "radical.asyncflow.workflow_manager"


class _Backend(NoopExecutionBackend):
    """Reports tasks named fail* as FAILED and parks tasks named park*."""

    def __init__(self):
        super().__init__()
        self.parked = {}

    async def submit_tasks(self, tasks):
        rest = []
        for t in tasks:
            if t["name"].startswith("fail"):
                t["exception"] = RuntimeError("boom")
                self._callback_func(t, "FAILED")
            elif t["name"].startswith("park"):
                self.parked[t["uid"]] = t
                self._callback_func(t, "RUNNING")
            else:
                rest.append(t)
        await super().submit_tasks(rest)

    async def cancel_task(self, uid):
        task = self.parked.pop(uid, None)
        if task is None:
            return False
        self._callback_func(task, "CANCELED")
        return True


def _messages(caplog, level):
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == LOGGER and r.levelno >= level
    ]


@pytest.mark.asyncio
async def test_failed_and_cancelled_tasks_log_at_info_or_above(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    flow = await WorkflowEngine.create(backend=_Backend())
    try:

        @flow.function_task
        async def ok():
            return 1

        @flow.function_task
        async def fail_one():
            return 1

        @flow.function_task
        async def park_one():
            return 1

        good, bad, parked = ok(), fail_one(), park_one()
        await asyncio.wait_for(good, timeout=10)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(bad, timeout=10)
        for _ in range(500):
            if hasattr(parked, "task") and parked.uid in flow.running:
                break
            await asyncio.sleep(0.01)
        parked.cancel()
        await asyncio.wait_for(asyncio.wait({parked}), timeout=10)

        assert f"{bad.uid} failed" in _messages(caplog, logging.WARNING)
        assert f"{parked.uid} canceled" in _messages(caplog, logging.INFO)
        assert not [m for m in _messages(caplog, logging.INFO) if good.uid in m]
    finally:
        await flow.shutdown()


@pytest.mark.asyncio
async def test_dependency_failure_logs_the_dependent(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    flow = await WorkflowEngine.create(backend=_Backend())
    try:

        @flow.function_task
        async def fail_one():
            return 1

        @flow.function_task
        async def after(x):
            return 2

        bad = fail_one()
        dep = after(bad)
        await asyncio.wait_for(asyncio.wait({bad, dep}), timeout=10)
        assert isinstance(bad.exception(), RuntimeError)
        assert dep.exception() is not None

        warnings = _messages(caplog, logging.WARNING)
        assert f"{bad.uid} failed" in warnings
        assert f"{dep.uid} failed" in warnings
    finally:
        await flow.shutdown()

"""Debug mode: runtime type checks on per-task methods, logger level."""

import logging

import pytest
import typeguard

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine, workflow_manager
from radical.asyncflow.logging import init_default_logger
from radical.asyncflow.workflow_manager import _debug_enabled


@pytest.mark.parametrize(
    "environ, expected",
    [
        ({}, False),
        ({"RADICAL_ASYNCFLOW_DEBUG": "1"}, True),
        ({"RADICAL_ASYNCFLOW_DEBUG": "TRUE"}, True),
        ({"RADICAL_ASYNCFLOW_DEBUG": "yes"}, True),
        ({"RADICAL_ASYNCFLOW_DEBUG": "on"}, True),
        ({"RADICAL_ASYNCFLOW_DEBUG": "0"}, False),
        ({"RADICAL_ASYNCFLOW_DEBUG": ""}, False),
        ({"RADICAL_DEBUG": "TRUE"}, True),
        ({"RADICAL_DEBUG": "false"}, False),
        # the specific variable decides when set
        ({"RADICAL_ASYNCFLOW_DEBUG": "0", "RADICAL_DEBUG": "TRUE"}, False),
        ({"RADICAL_ASYNCFLOW_DEBUG": "1", "RADICAL_DEBUG": "0"}, True),
    ],
)
def test_debug_enabled(environ, expected):
    assert _debug_enabled(environ) is expected


@pytest.mark.asyncio
async def test_task_callbacks_typechecked_in_debug_mode():
    """The suite runs in debug mode (tests/conftest.py): task_callbacks checks its
    argument types."""
    if workflow_manager._debug_typechecked is not typeguard.typechecked:
        pytest.skip("debug mode disabled when radical.asyncflow was imported")
    engine = await WorkflowEngine.create(backend=NoopExecutionBackend())
    try:
        with pytest.raises(typeguard.TypeCheckError):
            engine.task_callbacks({"uid": "task.000001"}, 42)
    finally:
        await engine.shutdown()


def test_logger_level_follows_handlers():
    """Records below every handler's level are dropped at the logger."""
    name = "radical.asyncflow.test_debug_mode"
    logger = init_default_logger(logging.WARNING, logger_name=name, clear_handlers=True)
    try:
        assert logger.level == logging.WARNING
        assert not logger.isEnabledFor(logging.DEBUG)

        # the lowest handler level wins
        logger.addHandler(logging.NullHandler(level=logging.DEBUG))
        init_default_logger(logging.INFO, logger_name=name)
        assert logger.level == logging.DEBUG
    finally:
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)

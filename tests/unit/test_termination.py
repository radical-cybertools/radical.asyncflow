import pytest
from unittest.mock import MagicMock

from radical.asyncflow import WorkflowEngine, ThreadExecutionBackend

@pytest.mark.asyncio
async def test_async_shutdown():
    """
    Unit test: ensures `flow.shutdown()` (inside the context manager) completes without error in async context,
    and backend.shutdown() is called once.
    """
    backend = ThreadExecutionBackend({})
    backend.shutdown = MagicMock()
    async with WorkflowEngine(backend=backend) as flow:
        pass

    try:
        backend.shutdown.assert_called_once()
    except Exception as e:
        pytest.fail(f"Async shutdown raised unexpected error: {e}")


def test_sync_shutdown():
    """
    Unit test: ensures `flow.shutdown()` (inside the context manager) works in sync context,
    and backend.shutdown() is called once.
    """
    backend = ThreadExecutionBackend({})
    backend.shutdown = MagicMock()
    with WorkflowEngine(backend=backend) as flow:
        pass

    try:
        backend.shutdown.assert_called_once()
    except Exception as e:
        pytest.fail(f"Sync shutdown raised unexpected error: {e}")

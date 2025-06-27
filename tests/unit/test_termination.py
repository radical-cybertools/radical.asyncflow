import pytest
from unittest.mock import MagicMock

from radical.asyncflow import WorkflowEngine, ThreadExecutionBackend

@pytest.mark.asyncio
async def test_async_shutdown():
    """
    Unit test: ensures `flow.shutdown()` completes without error in async context,
    and backend.shutdown() is called once.
    """
    backend = ThreadExecutionBackend({})
    backend.shutdown = MagicMock()
    flow = WorkflowEngine(backend=backend)

    try:
        await flow.shutdown()
        backend.shutdown.assert_called_once()
    except Exception as e:
        pytest.fail(f"Async shutdown raised unexpected error: {e}")


def test_sync_shutdown():
    """
    Unit test: ensures `flow.shutdown()` works in sync context,
    and backend.shutdown() is called once.
    """
    backend = ThreadExecutionBackend({})
    backend.shutdown = MagicMock()
    flow = WorkflowEngine(backend=backend)

    try:
        flow.shutdown()
        backend.shutdown.assert_called_once()
    except Exception as e:
        pytest.fail(f"Sync shutdown raised unexpected error: {e}")

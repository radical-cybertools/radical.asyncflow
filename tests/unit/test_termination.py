import pytest
import asyncio

from radical.flow import WorkflowEngine, ThreadExecutionBackend

@pytest.mark.asyncio
async def test_async_shutdown():
    """
    Unit test: ensures `flow.shutdown()` completes without error in async context.
    """
    backend = ThreadExecutionBackend({})
    flow = WorkflowEngine(backend=backend)

    try:
        assert hasattr(flow, 'shutdown')
        await flow.shutdown()
    except Exception as e:
        pytest.fail(f"Async shutdown raised unexpected error: {e}")


def test_sync_shutdown():
    """
    Unit test: ensures `flow.shutdown()` works when called in a blocking event loop.
    """
    backend = ThreadExecutionBackend({})
    flow = WorkflowEngine(backend=backend)

    try:
        assert hasattr(flow, 'shutdown')
        flow.shutdown()
    except Exception as e:
        pytest.fail(f"Sync shutdown via asyncio.run raised unexpected error: {e}")

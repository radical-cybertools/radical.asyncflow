import pytest
from unittest.mock import AsyncMock
from concurrent.futures import ThreadPoolExecutor
from radical.asyncflow import WorkflowEngine, ConcurrentExecutionBackend

@pytest.mark.asyncio
async def test_async_shutdown():
    """
    Unit test: ensures `flow.shutdown()` completes without error in async context,
    and backend.shutdown() is called once.
    """
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor())
    backend.shutdown = AsyncMock()
    flow = await WorkflowEngine.create(backend=backend)

    try:
        await flow.shutdown()
        backend.shutdown.assert_called_once()
    except Exception as e:
        pytest.fail(f"Async shutdown raised unexpected error: {e}")

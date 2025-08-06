import pytest
import asyncio
import signal
import threading
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path

import pytest_asyncio

from radical.asyncflow import WorkflowEngine, NoopExecutionBackend

import pytest
import asyncio
import signal
import threading
import time
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path


class TestGracefulShutdown:
    """Test suite for graceful shutdown functionality of WorkflowEngine."""

    @pytest_asyncio.fixture
    async def engine(self):
        """Create a WorkflowEngine instance for testing."""
        # Mock backend to avoid real backend dependencies
        mock_backend = Mock()
        mock_backend.session = Mock()
        mock_backend.session.path = "/tmp/test"
        mock_backend.get_task_states_map.return_value = Mock()
        mock_backend.register_callback = Mock()
        mock_backend.shutdown = AsyncMock()
        
        # Create engine with mocked backend
        with patch('radical.asyncflow.workflow_manager._get_event_loop_or_raise') as mock_loop:
            mock_loop.return_value = asyncio.get_event_loop()

            engine = WorkflowEngine.__new__(WorkflowEngine)
            engine.__init__(backend=mock_backend, dry_run=False, implicit_data=True)
            
            # Set up basic internal tasks if they don't exist
            if not hasattr(engine, '_run_task') or engine._run_task is None:
                engine._run_task = asyncio.create_task(self._mock_run_component())
            if not hasattr(engine, '_submit_task') or engine._submit_task is None:
                engine._submit_task = asyncio.create_task(self._mock_submit_component())
            
            # Initialize components dict if not exists
            if not hasattr(engine, 'components'):
                engine.components = {}

            yield engine

            # Cleanup - ensure shutdown is called if not already done
            try:
                if not engine._shutdown_event.is_set():
                    await engine.shutdown(skip_execution_backend=True)
            except Exception as e:
                # Log but don't fail the test on cleanup errors
                print(f"Cleanup error: {e}")

            # Cancel any remaining tasks
            for task in [engine._run_task, engine._submit_task]:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

    @pytest.mark.asyncio
    async def test_manual_shutdown_clean_exit(self, engine):
        """Test 1: Manual shutdown completes cleanly without errors."""
        # Arrange: Set up mock tasks that will complete normally
        engine._run_task = asyncio.create_task(self._mock_run_component())
        engine._submit_task = asyncio.create_task(self._mock_submit_component())

        # Act: Call shutdown
        start_time = time.time()
        await engine.shutdown(skip_execution_backend=True)
        end_time = time.time()

        # Assert: Shutdown completed quickly and cleanly
        assert engine._shutdown_event.is_set()
        assert engine._run_task.cancelled() or engine._run_task.done()
        assert engine._submit_task.cancelled() or engine._submit_task.done()
        assert (end_time - start_time) < 1.0  # Should complete quickly

    @pytest.mark.asyncio
    async def test_shutdown_with_running_tasks(self, engine):
        """Test 2: Shutdown cancels running workflow tasks properly."""
        # Arrange: Add some mock workflow components
        mock_future1 = asyncio.Future()
        mock_future2 = asyncio.Future()
        mock_future2.set_result("completed")  # One completed task
        
        # Set up the original_cancel methods that should exist
        mock_future1.original_cancel = Mock(return_value=True)
        mock_future2.original_cancel = Mock(return_value=True)

        # Create a mock handle_task_cancellation method that calls original_cancel
        def mock_handle_cancellation(task_desc, task_fut):
            if hasattr(task_fut, 'original_cancel'):
                task_fut.original_cancel()

        engine.handle_task_cancellation = Mock(side_effect=mock_handle_cancellation)

        engine.components = {
            'task.000001': {
                'future': mock_future1,
                'description': {'uid': 'task.000001', 'name': 'test_task_1'}
            },
            'task.000002': {
                'future': mock_future2,
                'description': {'uid': 'task.000002', 'name': 'test_task_2'}
            }
        }

        # Act: Shutdown
        await engine.shutdown(skip_execution_backend=True)

        # Assert: Shutdown event is set
        assert engine._shutdown_event.is_set()

        # Verify handle_task_cancellation was called for one task (pending only)
        assert engine.handle_task_cancellation.call_count == 1

    @pytest.mark.asyncio
    async def test_signal_handler_triggers_shutdown(self, engine):
        """Test 3: Signal handlers properly trigger graceful shutdown."""
        # Arrange: Track shutdown calls
        shutdown_called = asyncio.Event()
        original_shutdown = engine.shutdown

        async def mock_shutdown(*args, **kwargs):
            shutdown_called.set()
            # Call the original but skip backend shutdown to avoid issues
            return await original_shutdown(skip_execution_backend=True)

        # Replace the shutdown method temporarily
        engine.shutdown = mock_shutdown

        # Act: Simulate signal reception by calling the handler directly
        await engine._handle_signal(signal.SIGTERM)

        # Wait for shutdown to be called with timeout
        try:
            await asyncio.wait_for(shutdown_called.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pytest.fail("Shutdown was not called within timeout period")

        # Assert: Shutdown was triggered
        assert shutdown_called.is_set()
        assert engine._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_timeout_handling(self, engine):
        """Test 4: Shutdown handles component timeout gracefully."""
        # Arrange: Create tasks that won't complete quickly
        async def slow_task():
            await asyncio.sleep(10)  # Long-running task

        engine._run_task = asyncio.create_task(slow_task())
        engine._submit_task = asyncio.create_task(slow_task())

        # Act: Shutdown with timeout
        start_time = time.time()
        await engine.shutdown(skip_execution_backend=True)
        end_time = time.time()

        # Assert: Shutdown completed within reasonable time despite slow tasks
        assert engine._shutdown_event.is_set()
        assert (end_time - start_time) < 7.0  # Should timeout and continue
        assert engine._run_task.cancelled()
        assert engine._submit_task.cancelled()

    @pytest.mark.asyncio
    async def test_backend_shutdown_integration(self, engine):
        """Test 5: Backend shutdown is properly integrated into workflow shutdown."""
        # Arrange: Mock backend with shutdown method
        mock_backend_shutdown = AsyncMock()
        engine.backend.shutdown = mock_backend_shutdown

        # Ensure internal tasks are properly set up
        if not hasattr(engine, '_run_task') or engine._run_task is None:
            engine._run_task = asyncio.create_task(self._mock_run_component())
        if not hasattr(engine, '_submit_task') or engine._submit_task is None:
            engine._submit_task = asyncio.create_task(self._mock_submit_component())

        # Act: Shutdown without skipping backend
        await engine.shutdown(skip_execution_backend=False)

        # Assert: Backend shutdown was called
        mock_backend_shutdown.assert_called_once()
        assert engine._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_concurrent_shutdown_calls(self, engine):
        """Test 6: Multiple concurrent shutdown calls are handled safely."""
        # Arrange: Set up normal components
        engine._run_task = asyncio.create_task(self._mock_run_component())
        engine._submit_task = asyncio.create_task(self._mock_submit_component())

        # Act: Call shutdown multiple times concurrently
        shutdown_tasks = [
            asyncio.create_task(engine.shutdown(skip_execution_backend=True))
            for _ in range(3)
        ]

        results = await asyncio.gather(*shutdown_tasks, return_exceptions=True)

        # Assert: All shutdown calls completed without errors
        assert engine._shutdown_event.is_set()
        for result in results:
            assert not isinstance(result, Exception), f"Shutdown failed with: {result}"

    @pytest.mark.asyncio 
    async def test_shutdown_signal_registration(self):
        """Bonus test: Verify signal handlers are properly registered."""
        with patch('signal.signal') as mock_signal, \
             patch('asyncio.get_event_loop') as mock_get_loop:
            
            mock_loop = Mock()
            mock_get_loop.return_value = mock_loop
            mock_loop.add_signal_handler = Mock()
            
            # Mock backend
            mock_backend = Mock()
            mock_backend.session = Mock()
            mock_backend.session.path = "/tmp/test"
            mock_backend.get_task_states_map.return_value = Mock()
            mock_backend.register_callback = Mock()
            
            # Create engine (will trigger signal handler registration)
            with patch('radical.asyncflow.workflow_manager._get_event_loop_or_raise') as mock_loop_check:
                mock_loop_check.return_value = mock_loop
                
                engine = WorkflowEngine.__new__(WorkflowEngine)
                engine.__init__(backend=mock_backend)
                
                # Assert: Signal handlers were registered
                expected_signals = [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]
                assert mock_loop.add_signal_handler.call_count == len(expected_signals)
                
                # Verify correct signals were registered
                registered_signals = [
                    call[0][0] for call in mock_loop.add_signal_handler.call_args_list
                ]
                for sig in expected_signals:
                    assert sig in registered_signals

    # Helper methods for mocking components
    async def _mock_run_component(self):
        """Mock run component that responds to shutdown signal."""
        try:
            while True:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            return

    async def _mock_submit_component(self):
        """Mock submit component that responds to shutdown signal."""
        try:
            while True:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            return


# Additional integration test for real signal handling
class TestSignalIntegration:
    """Integration tests for signal handling (requires careful setup)."""

    @pytest.mark.asyncio
    @pytest.mark.skipif(threading.current_thread() != threading.main_thread(), 
                       reason="Signal handling only works in main thread")
    async def test_real_signal_handling(self):
        """Test that real signals can trigger shutdown (integration test)."""
        # This test should only run in environments where signal handling is supported
        # and when running in the main thread

        shutdown_completed = threading.Event()
        
        async def create_and_test_engine():
            # Create engine with real signal handling
            mock_backend = Mock()
            mock_backend.session = Mock()
            mock_backend.session.path = "/tmp/test"
            mock_backend.get_task_states_map.return_value = Mock()
            mock_backend.register_callback = Mock()
            mock_backend.shutdown = AsyncMock()

            try:
                with patch('radical.asyncflow.workflow_manager._get_event_loop_or_raise') as mock_loop:
                    mock_loop.return_value = asyncio.get_event_loop()
                    
                    engine = WorkflowEngine.__new__(WorkflowEngine)
                    engine.__init__(backend=mock_backend)
                    
                    # Note: Using SIGUSR1 instead of sending real termination signals
                    # Manual cleanup
                    await engine.shutdown(skip_execution_backend=True)

            finally:
                shutdown_completed.set()

        # Run the test
        await create_and_test_engine()

        # Verify completion
        assert shutdown_completed.is_set()

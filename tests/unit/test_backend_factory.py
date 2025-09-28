"""Unit tests for BackendFactory."""

import concurrent.futures
import logging
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio

from radical.asyncflow.backends.execution.base import BaseExecutionBackend
from radical.asyncflow.backends.factory import BackendFactory


# Mock backend classes for testing
class MockAsyncBackend(BaseExecutionBackend):
    """Mock backend with async initialization."""

    def __init__(self, config=None, **kwargs):
        self.config = config or {}
        self.kwargs = kwargs
        self.initialized = False

    async def __aenter__(self):
        self.initialized = True
        return self

    async def submit_tasks(self, tasks):
        pass

    async def shutdown(self):
        pass

    def state(self):
        return "RUNNING"

    def task_state_cb(self, task, state):
        pass

    def register_callback(self, func):
        pass

    def get_task_states_map(self):
        pass

    def build_task(self, uid, task_desc, task_specific_kwargs):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    def link_explicit_data_deps(
        self, src_task=None, dst_task=None, file_name=None, file_path=None
    ):
        pass

    async def cancel_task(self, uid: str) -> bool:
        return True


class MockAwaitableBackend(BaseExecutionBackend):
    """Mock backend that is awaitable for initialization."""

    def __init__(self, config=None, **kwargs):
        self.config = config or {}
        self.kwargs = kwargs
        self.initialized = False

    def __await__(self):
        async def _init():
            self.initialized = True
            return self

        return _init().__await__()

    async def submit_tasks(self, tasks):
        pass

    async def shutdown(self):
        pass

    def state(self):
        return "RUNNING"

    def task_state_cb(self, task, state):
        pass

    def register_callback(self, func):
        pass

    def get_task_states_map(self):
        pass

    def build_task(self, uid, task_desc, task_specific_kwargs):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    def link_explicit_data_deps(
        self, src_task=None, dst_task=None, file_name=None, file_path=None
    ):
        pass

    async def cancel_task(self, uid: str) -> bool:
        return True


class MockInitializeBackend(BaseExecutionBackend):
    """Mock backend with explicit initialize method."""

    def __init__(self, config=None, **kwargs):
        self.config = config or {}
        self.kwargs = kwargs
        self.initialized = False

    async def initialize(self):
        self.initialized = True

    async def submit_tasks(self, tasks):
        pass

    async def shutdown(self):
        pass

    def state(self):
        return "RUNNING"

    def task_state_cb(self, task, state):
        pass

    def register_callback(self, func):
        pass

    def get_task_states_map(self):
        pass

    def build_task(self, uid, task_desc, task_specific_kwargs):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    def link_explicit_data_deps(
        self, src_task=None, dst_task=None, file_name=None, file_path=None
    ):
        pass

    async def cancel_task(self, uid: str) -> bool:
        return True


class TestBackendFactory:
    """Test suite for BackendFactory functionality."""

    @pytest_asyncio.fixture
    def factory(self):
        """Create a BackendFactory instance for testing."""
        return BackendFactory()

    def test_suggest_installation_known_backend(self):
        """Test installation suggestions for known backends."""
        suggestions = {
            "dask": "pip install 'radical.asyncflow[dask]'",
            "radical_pilot": "pip install 'radical.asyncflow[radicalpilot]'",
        }

        for backend_type, expected in suggestions.items():
            result = BackendFactory._suggest_installation(backend_type)
            assert expected in result

    def test_suggest_installation_unknown_backend(self):
        """Test installation suggestion for unknown backend."""
        result = BackendFactory._suggest_installation("unknown")
        assert "pip install 'radical.asyncflow[hpc]'" in result

    @pytest.mark.asyncio
    async def test_create_noop_backend_success(self, factory):
        """Test successful creation of noop backend."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            # Mock successful backend retrieval
            from radical.asyncflow.backends.execution.noop import NoopExecutionBackend

            mock_registry.get_backend.return_value = NoopExecutionBackend

            # Act
            backend = await factory.create_backend("noop")

            # Assert
            assert isinstance(backend, NoopExecutionBackend)
            mock_registry.get_backend.assert_called_once_with("noop")

    @pytest.mark.asyncio
    async def test_create_concurrent_backend_success(self, factory):
        """Test successful creation of concurrent backend with default config."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            # Mock concurrent backend class that accepts executor
            mock_backend_class = Mock()
            mock_backend_instance = Mock()
            mock_backend_class.return_value = mock_backend_instance
            mock_registry.get_backend.return_value = mock_backend_class

            # Act
            backend = await factory.create_backend("concurrent")

            # Assert
            assert backend is mock_backend_instance
            mock_registry.get_backend.assert_called_once_with("concurrent")
            # Verify that backend was created with an executor
            args = mock_backend_class.call_args[0]
            assert len(args) == 1
            assert isinstance(args[0], concurrent.futures.ThreadPoolExecutor)

    @pytest.mark.asyncio
    async def test_create_concurrent_backend_with_process_executor(self, factory):
        """Test creation of concurrent backend with process executor."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_backend_class = Mock()
            mock_backend_instance = Mock()
            mock_backend_class.return_value = mock_backend_instance
            mock_registry.get_backend.return_value = mock_backend_class

            # Act
            config = {"executor_type": "process", "max_workers": 2}
            await factory.create_backend("concurrent", config=config)

            # Assert
            args = mock_backend_class.call_args[0]
            assert isinstance(args[0], concurrent.futures.ProcessPoolExecutor)

    @pytest.mark.asyncio
    async def test_create_backend_not_available(self, factory):
        """Test creation of unavailable backend raises ValueError."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_registry.get_backend.return_value = None
            mock_registry.list_available.return_value = {
                "noop": True,
                "concurrent": True,
                "unavailable": False,
            }
            mock_registry.get_failure_reason.return_value = "Import error"

            # Act & Assert
            with pytest.raises(
                ValueError, match="Backend 'unavailable' is not available"
            ):
                await factory.create_backend("unavailable")

    @pytest.mark.asyncio
    async def test_create_backend_initialization_failure(self, factory):
        """Test handling of backend initialization failure."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_backend_class = Mock(side_effect=RuntimeError("Init failed"))
            mock_registry.get_backend.return_value = mock_backend_class

            # Act & Assert
            with pytest.raises(
                RuntimeError, match="Failed to initialize backend 'test'"
            ):
                await factory.create_backend("test")

    @pytest.mark.asyncio
    async def test_create_backend_async_context_manager(self, factory):
        """Test creation of backend that is an async context manager."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_registry.get_backend.return_value = MockAsyncBackend

            # Act
            backend = await factory.create_backend("test_async")

            # Assert
            assert isinstance(backend, MockAsyncBackend)
            assert backend.initialized is True

    @pytest.mark.asyncio
    async def test_create_backend_awaitable(self, factory):
        """Test creation of backend that is awaitable."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_registry.get_backend.return_value = MockAwaitableBackend

            # Act
            backend = await factory.create_backend("test_awaitable")

            # Assert
            assert isinstance(backend, MockAwaitableBackend)
            assert backend.initialized is True

    @pytest.mark.asyncio
    async def test_create_backend_with_initialize_method(self, factory):
        """Test creation of backend with explicit initialize method."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_registry.get_backend.return_value = MockInitializeBackend

            # Act
            backend = await factory.create_backend("test_initialize")

            # Assert
            assert isinstance(backend, MockInitializeBackend)
            assert backend.initialized is True

    @pytest.mark.asyncio
    async def test_create_backend_with_config_and_kwargs(self, factory):
        """Test creation of backend with config and keyword arguments."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_backend_class = Mock()
            mock_backend_instance = Mock()
            # Make sure the mock doesn't have initialize method to avoid the await issue
            del mock_backend_instance.initialize
            mock_backend_class.return_value = mock_backend_instance
            mock_registry.get_backend.return_value = mock_backend_class

            # Act
            config = {"setting1": "value1"}
            await factory.create_backend(
                "test_config", config=config, extra_param="extra_value"
            )

            # Assert
            mock_backend_class.assert_called_once_with(
                config, extra_param="extra_value"
            )

    def test_list_available_backends(self, factory):
        """Test listing available backends with detailed information."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_backend_class = Mock()
            mock_backend_class.__name__ = "MockBackend"

            mock_registry._backend_specs = {
                "available": "test:Available",
                "unavailable": "test:Unavailable",
            }
            mock_registry.get_backend.side_effect = lambda name: (
                mock_backend_class if name == "available" else None
            )
            mock_registry.get_failure_reason.side_effect = lambda name: (
                None if name == "available" else "Import failed"
            )

            # Act
            info = factory.list_available_backends()

            # Assert
            assert "available" in info
            assert "unavailable" in info

            available_info = info["available"]
            assert available_info["available"] is True
            assert available_info["class"] == "MockBackend"
            assert available_info["failure_reason"] is None
            assert available_info["installation_hint"] is None

            unavailable_info = info["unavailable"]
            assert unavailable_info["available"] is False
            assert unavailable_info["class"] is None
            assert unavailable_info["failure_reason"] == "Import failed"
            assert unavailable_info["installation_hint"] is not None

    def test_create_backend_sync_success(self, factory):
        """Test synchronous backend creation."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_backend_class = Mock()
            mock_backend_instance = Mock()
            mock_backend_class.return_value = mock_backend_instance
            mock_registry.get_backend.return_value = mock_backend_class

            # Act
            config = {"test": "value"}
            backend = factory.create_backend_sync("test", config=config)

            # Assert
            assert backend is mock_backend_instance
            mock_backend_class.assert_called_once_with(config)

    def test_create_backend_sync_not_available(self, factory):
        """Test synchronous creation of unavailable backend."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_registry.get_backend.return_value = None
            mock_registry.list_available.return_value = {"noop": True}

            # Act & Assert
            with pytest.raises(
                ValueError, match="Backend 'unavailable' is not available"
            ):
                factory.create_backend_sync("unavailable")

    def test_create_backend_sync_initialization_failure(self, factory):
        """Test handling of sync backend creation failure."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_backend_class = Mock(side_effect=RuntimeError("Creation failed"))
            mock_registry.get_backend.return_value = mock_backend_class

            # Act & Assert
            with pytest.raises(RuntimeError, match="Failed to create backend 'test'"):
                factory.create_backend_sync("test")

    def test_logging_during_operations(self, factory, caplog):
        """Test that appropriate log messages are generated."""
        with patch("radical.asyncflow.backends.factory.registry") as mock_registry:
            mock_backend_class = Mock()
            mock_backend_instance = Mock()
            mock_backend_class.return_value = mock_backend_instance
            mock_registry.get_backend.return_value = mock_backend_class

            with caplog.at_level(logging.DEBUG):
                # Test sync creation
                factory.create_backend_sync("test_logging")

            # Check that debug messages were logged
            debug_messages = [
                record.message
                for record in caplog.records
                if record.levelno == logging.DEBUG
            ]
            assert any(
                "Creating backend 'test_logging'" in msg for msg in debug_messages
            )

            with caplog.at_level(logging.INFO):
                # Test sync creation
                factory.create_backend_sync("test_logging_info")

            # Check that info messages were logged
            info_messages = [
                record.message
                for record in caplog.records
                if record.levelno == logging.INFO
            ]
            assert any(
                "Successfully created 'test_logging_info' backend" in msg
                for msg in info_messages
            )

    def test_global_factory_instance(self):
        """Test that the global factory instance is available."""
        from radical.asyncflow.backends.factory import factory

        assert isinstance(factory, BackendFactory)

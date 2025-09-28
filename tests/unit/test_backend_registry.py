"""Unit tests for BackendRegistry."""

import logging
from unittest.mock import Mock, patch

import pytest

from radical.asyncflow.backends.execution.base import BaseExecutionBackend
from radical.asyncflow.backends.registry import BackendRegistry


# Mock backend classes for testing
class MockValidBackend(BaseExecutionBackend):
    """Valid mock backend for testing."""

    async def submit_tasks(self, tasks):
        pass

    async def shutdown(self):
        pass

    def state(self):
        return "IDLE"

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


class MockInvalidBackend:
    """Invalid mock backend that doesn't inherit from BaseExecutionBackend."""

    def some_method(self):
        pass


class TestBackendRegistry:
    """Test suite for BackendRegistry functionality."""

    def test_registry_initialization(self):
        """Test that registry initializes with correct backend specifications."""
        registry = BackendRegistry()

        # Check that registry has expected internal state
        assert hasattr(registry, "_backends")
        assert hasattr(registry, "_failed_backends")
        assert hasattr(registry, "_backend_specs")

        # Check that expected backend specifications are present
        expected_backends = [
            "noop",
            "concurrent",
            "dask",
            "radical_pilot",
        ]
        for backend_name in expected_backends:
            assert backend_name in registry._backend_specs

    @patch("importlib.import_module")
    def test_successful_backend_loading(self, mock_import_module):
        """Test successful loading of a valid backend."""
        # Arrange
        registry = BackendRegistry()
        mock_module = Mock()
        mock_module.MockBackend = MockValidBackend
        mock_import_module.return_value = mock_module

        # Add a test backend spec
        registry.add_backend_spec("test_valid", "test.module:MockBackend")

        # Act
        backend_class = registry.get_backend("test_valid")

        # Assert
        assert backend_class is MockValidBackend
        assert "test_valid" in registry._backends
        assert "test_valid" not in registry._failed_backends
        mock_import_module.assert_called_once_with("test.module")

    @patch("importlib.import_module")
    def test_backend_loading_import_error(self, mock_import_module):
        """Test handling of import errors during backend loading."""
        # Arrange
        registry = BackendRegistry()
        mock_import_module.side_effect = ImportError("Module not found")

        registry.add_backend_spec("test_missing", "missing.module:MissingBackend")

        # Act
        backend_class = registry.get_backend("test_missing")

        # Assert
        assert backend_class is None
        assert "test_missing" in registry._failed_backends
        assert "Import error" in registry._failed_backends["test_missing"]
        assert "test_missing" not in registry._backends

    @patch("importlib.import_module")
    def test_backend_loading_attribute_error(self, mock_import_module):
        """Test handling of missing class in module."""
        # Arrange
        registry = BackendRegistry()
        mock_module = Mock()
        # Remove the expected class from the module
        del mock_module.MissingClass
        mock_import_module.return_value = mock_module

        registry.add_backend_spec("test_missing_class", "test.module:MissingClass")

        # Act
        backend_class = registry.get_backend("test_missing_class")

        # Assert
        assert backend_class is None
        assert "test_missing_class" in registry._failed_backends
        assert "Class not found" in registry._failed_backends["test_missing_class"]

    @patch("importlib.import_module")
    def test_invalid_backend_class_validation(self, mock_import_module):
        """Test validation that backend class inherits from BaseExecutionBackend."""
        # Arrange
        registry = BackendRegistry()
        mock_module = Mock()
        mock_module.InvalidBackend = MockInvalidBackend
        mock_import_module.return_value = mock_module

        registry.add_backend_spec("test_invalid", "test.module:InvalidBackend")

        # Act
        backend_class = registry.get_backend("test_invalid")

        # Assert
        assert backend_class is None
        assert "test_invalid" in registry._failed_backends
        assert "missing required methods" in registry._failed_backends["test_invalid"]

    def test_cached_backend_retrieval(self):
        """Test that successfully loaded backends are cached."""
        registry = BackendRegistry()

        # Add backend directly to cache
        registry._backends["test_cached"] = MockValidBackend

        # Act
        backend_class = registry.get_backend("test_cached")

        # Assert
        assert backend_class is MockValidBackend

    def test_cached_failure_retrieval(self):
        """Test that failed backends are not retried."""
        registry = BackendRegistry()

        # Add failure to cache
        registry._failed_backends["test_failed"] = "Previously failed"

        # Act
        backend_class = registry.get_backend("test_failed")

        # Assert
        assert backend_class is None

    def test_unknown_backend_request(self):
        """Test request for backend that doesn't exist in specifications."""
        registry = BackendRegistry()

        # Act
        backend_class = registry.get_backend("nonexistent_backend")

        # Assert
        assert backend_class is None

    def test_list_available_backends(self):
        """Test listing of all available backends."""
        registry = BackendRegistry()

        # Add some test results
        registry._backends["available_backend"] = MockValidBackend
        registry._failed_backends["failed_backend"] = "Import error"
        registry._backend_specs["available_backend"] = "test:Backend"
        registry._backend_specs["failed_backend"] = "test:Backend"

        # Act
        available = registry.list_available()

        # Assert
        assert isinstance(available, dict)
        # Note: The test will trigger actual backend loading for specs,
        # so we mainly test the structure
        assert "noop" in available  # This should be available
        assert "concurrent" in available  # This should be available

    def test_list_loaded_backends(self):
        """Test listing of currently loaded backends."""
        registry = BackendRegistry()

        # Add loaded backends
        registry._backends["loaded1"] = MockValidBackend
        registry._backends["loaded2"] = MockValidBackend

        # Act
        loaded = registry.list_loaded()

        # Assert
        assert loaded == {"loaded1": MockValidBackend, "loaded2": MockValidBackend}
        # Ensure it returns a copy, not the original dict
        loaded["new_item"] = "test"
        assert "new_item" not in registry._backends

    def test_get_failure_reason(self):
        """Test retrieving failure reasons for backends."""
        registry = BackendRegistry()

        # Add failure reason
        test_reason = "Import error: Module not found"
        registry._failed_backends["failed_backend"] = test_reason

        # Act & Assert
        assert registry.get_failure_reason("failed_backend") == test_reason
        assert registry.get_failure_reason("nonexistent_backend") is None

    def test_register_backend_direct(self):
        """Test direct registration of a backend class."""
        registry = BackendRegistry()

        # Act
        registry.register_backend("custom_backend", MockValidBackend)

        # Assert
        assert registry._backends["custom_backend"] is MockValidBackend
        assert "custom_backend" not in registry._failed_backends

    def test_register_backend_invalid_class(self):
        """Test direct registration with invalid backend class."""
        registry = BackendRegistry()

        # Act & Assert
        with pytest.raises(TypeError, match="must be a BaseExecutionBackend subclass"):
            registry.register_backend("invalid_backend", MockInvalidBackend)

    def test_register_backend_clears_failure(self):
        """Test that direct registration clears any previous failure."""
        registry = BackendRegistry()

        # Add failure first
        registry._failed_backends["test_backend"] = "Previous failure"

        # Act
        registry.register_backend("test_backend", MockValidBackend)

        # Assert
        assert "test_backend" not in registry._failed_backends
        assert registry._backends["test_backend"] is MockValidBackend

    def test_add_backend_spec(self):
        """Test adding new backend specification."""
        registry = BackendRegistry()

        # Act
        registry.add_backend_spec("new_backend", "new.module:NewBackend")

        # Assert
        assert registry._backend_specs["new_backend"] == "new.module:NewBackend"

    def test_add_backend_spec_clears_cache(self):
        """Test that adding spec clears cached results."""
        registry = BackendRegistry()

        # Add cached results
        registry._backends["test_spec"] = MockValidBackend
        registry._failed_backends["test_spec"] = "Old failure"

        # Act
        registry.add_backend_spec("test_spec", "updated.module:UpdatedBackend")

        # Assert
        assert "test_spec" not in registry._backends
        assert "test_spec" not in registry._failed_backends

    @patch("importlib.import_module")
    def test_unexpected_error_during_loading(self, mock_import_module):
        """Test handling of unexpected errors during backend loading."""
        # Arrange
        registry = BackendRegistry()
        mock_import_module.side_effect = RuntimeError("Unexpected error")

        registry.add_backend_spec("test_error", "test.module:ErrorBackend")

        # Act
        backend_class = registry.get_backend("test_error")

        # Assert
        assert backend_class is None
        assert "test_error" in registry._failed_backends
        assert "Unexpected error" in registry._failed_backends["test_error"]

    def test_logging_during_operations(self, caplog):
        """Test that appropriate log messages are generated."""
        registry = BackendRegistry()

        with caplog.at_level(logging.DEBUG):
            # Test successful registration
            registry.register_backend("log_test", MockValidBackend)

            # Test spec addition
            registry.add_backend_spec("log_spec", "test:Backend")

        # Check that debug messages were logged
        assert any(
            "Registered backend 'log_test'" in record.message
            for record in caplog.records
        )
        assert any(
            "Added backend spec 'log_spec'" in record.message
            for record in caplog.records
        )

    def test_global_registry_instance(self):
        """Test that the global registry instance is available."""
        from radical.asyncflow.backends.registry import registry

        assert isinstance(registry, BackendRegistry)
        assert hasattr(registry, "_backend_specs")

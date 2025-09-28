"""Tests for the complete plugin architecture integration.

Tests the full flow: factory -> registry -> backend loading, including
optional dependency behavior and error handling.
"""

from unittest.mock import Mock, patch

import pytest

from radical.asyncflow.backends.factory import BackendFactory
from radical.asyncflow.backends.registry import BackendRegistry


class TestPluginArchitecture:
    """Test the complete plugin architecture integration."""

    def test_factory_registry_integration(self):
        """Test that factory properly uses registry for backend discovery."""
        # Create fresh instances to avoid cached state
        registry = BackendRegistry()

        # Registry should have core backends available
        core_backends = ["noop", "concurrent"]
        available = registry.list_available()

        for backend_name in core_backends:
            assert backend_name in available
            assert available[backend_name] is True  # Should be loadable

    @pytest.mark.asyncio
    async def test_end_to_end_core_backend_creation(self):
        """Test complete flow for core backend creation."""
        # Test noop backend
        noop_backend = await BackendFactory.create_backend("noop")
        assert noop_backend is not None
        assert hasattr(noop_backend, "submit_tasks")
        assert hasattr(noop_backend, "shutdown")

        # Test concurrent backend
        concurrent_backend = await BackendFactory.create_backend("concurrent")
        assert concurrent_backend is not None
        assert hasattr(concurrent_backend, "submit_tasks")
        assert hasattr(concurrent_backend, "shutdown")

    @pytest.mark.asyncio
    async def test_optional_backend_helpful_error_messages(self):
        """Test that optional backends provide helpful error messages."""
        # Use truly unavailable backends instead of dask/radical_pilot
        # which might be available now
        unavailable_backends = ["nonexistent_backend", "fake_backend"]

        for backend_name in unavailable_backends:
            with pytest.raises(ValueError) as exc_info:
                await BackendFactory.create_backend(backend_name)

            error_msg = str(exc_info.value)

            # Should contain helpful information
            assert f"Backend '{backend_name}' is not available" in error_msg
            assert "Available backends:" in error_msg
            assert "Installation hint:" in error_msg
            assert "radical.asyncflow[" in error_msg  # Should suggest optional install

            # Should list actual available backends
            assert "noop" in error_msg
            assert "concurrent" in error_msg

    def test_optional_backend_installation_hints(self):
        """Test that installation hints are correct for each backend."""
        factory = BackendFactory()

        # Test specific backend hints
        dask_hint = factory._suggest_installation("dask")
        assert "radical.asyncflow[dask]" in dask_hint

        radical_pilot_hint = factory._suggest_installation("radical_pilot")
        assert "radical.asyncflow[radicalpilot]" in radical_pilot_hint

        # Test fallback for unknown backends
        unknown_hint = factory._suggest_installation("unknown_backend")
        assert "radical.asyncflow[hpc]" in unknown_hint

    @patch("importlib.import_module")
    def test_backend_validation_failure_handling(self, mock_import):
        """Test handling of backends that load but fail validation."""
        # Simulate rhapsody backend that loads but has wrong base class
        mock_module = Mock()
        mock_backend_class = Mock()
        mock_backend_class.__name__ = "MockRhapsodyBackend"
        mock_module.MockRhapsodyBackend = mock_backend_class
        mock_import.return_value = mock_module

        registry = BackendRegistry()

        # Add a mock rhapsody-style backend that will fail validation
        registry.add_backend_spec("mock_rhapsody", "mock.module:MockRhapsodyBackend")

        # Should return None due to validation failure
        backend_class = registry.get_backend("mock_rhapsody")
        assert backend_class is None

        # Should have failure reason
        failure_reason = registry.get_failure_reason("mock_rhapsody")
        assert failure_reason is not None
        assert "unexpected error" in failure_reason.lower()

    def test_registry_caching_behavior_with_optional_backends(self):
        """Test that registry properly caches both successes and failures."""
        registry = BackendRegistry()

        # Test with dask which should be available now
        result1 = registry.get_backend("dask")
        result2 = registry.get_backend("dask")

        # Both should be the same backend class and consistent
        assert result1 is not None
        assert result2 is not None
        assert result1 is result2  # Should be cached

        # Test with unknown backend (not in _backend_specs)
        result3 = registry.get_backend("nonexistent_backend")
        result4 = registry.get_backend("nonexistent_backend")

        # Both should be None (not available) but consistent
        assert result3 is None
        assert result4 is None

        # For unknown backends, no failure reason is recorded
        # (they just return None without attempting to load)
        failure_reason = registry.get_failure_reason("nonexistent_backend")
        assert failure_reason is None  # No failure reason for unknown backends

    def test_backend_specs_updated_for_phase2(self):
        """Test that backend specs reflect Phase 2 changes."""
        registry = BackendRegistry()
        specs = registry._backend_specs

        # Should have core backends
        assert "noop" in specs
        assert "concurrent" in specs
        assert "radical.asyncflow.backends.execution" in specs["noop"]
        assert "radical.asyncflow.backends.execution" in specs["concurrent"]

        # Should have rhapsody backends with correct module paths
        assert "dask" in specs
        assert "radical_pilot" in specs
        assert "rhapsody.backends.execution.dask_parallel" in specs["dask"]
        assert "rhapsody.backends.execution.radical_pilot" in specs["radical_pilot"]

        # Should NOT have removed backends
        assert "dragon" not in specs
        assert "flux" not in specs

    def test_list_available_includes_optional_backends(self):
        """Test that registry lists optional backends even when not loadable."""
        registry = BackendRegistry()
        available = registry.list_available()

        # Should include all backend specs
        expected_backends = ["noop", "concurrent", "dask", "radical_pilot"]
        for backend in expected_backends:
            assert backend in available

        # Core backends should be available
        assert available["noop"] is True
        assert available["concurrent"] is True

        # Optional backends will be listed but may not be loadable
        # (depends on whether rhapsody is installed in test environment)
        assert "dask" in available
        assert "radical_pilot" in available

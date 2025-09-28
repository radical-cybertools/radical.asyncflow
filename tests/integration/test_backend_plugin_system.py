"""Integration tests for backend plugin system.

Tests the complete plugin architecture in realistic scenarios, including workflow
execution through the plugin system.
"""

import pytest

import radical.asyncflow as asyncflow


class TestBackendPluginIntegration:
    """Test backend plugin system integration with workflows."""

    @pytest.mark.asyncio
    async def test_workflow_with_noop_backend_via_factory(self):
        """Test complete workflow execution using noop backend through factory."""
        # Create backend via plugin system
        backend = await asyncflow.factory.create_backend("noop")
        flow = await asyncflow.WorkflowEngine.create(backend=backend)

        @flow.function_task
        async def simple_task(x: int) -> int:
            return x * 2

        # Submit task through the workflow (no await on task call)
        future = simple_task(5)
        result = await future

        # Noop backend returns dummy output, not actual computation
        assert result == "Dummy Output"

        # Clean shutdown
        await flow.shutdown()

    @pytest.mark.asyncio
    async def test_workflow_with_concurrent_backend_via_factory(self):
        """Test complete workflow execution using concurrent backend through factory."""
        # Create backend via plugin system with custom config
        backend = await asyncflow.factory.create_backend(
            "concurrent", config={"max_workers": 2}
        )
        flow = await asyncflow.WorkflowEngine.create(backend=backend)

        @flow.function_task
        async def compute_task(x: int) -> int:
            return x**2

        # Submit multiple tasks (no await on task calls)
        tasks = []
        for i in range(3):
            future = compute_task(i + 1)
            tasks.append(future)

        # Wait for all results
        results = []
        for task in tasks:
            result = await task
            results.append(result)

        assert results == [1, 4, 9]

        # Clean shutdown
        await flow.shutdown()

    def test_backend_availability_discovery(self):
        """Test that plugin system correctly reports backend availability."""
        available = asyncflow.registry.list_available()

        # Core backends should be available
        assert "noop" in available
        assert "concurrent" in available
        assert available["noop"] is True
        assert available["concurrent"] is True

        # Optional backends should be listed (may or may not be loadable)
        assert "dask" in available
        assert "radical_pilot" in available

        # Removed backends should not be present
        assert "dragon" not in available
        assert "flux" not in available

    @pytest.mark.asyncio
    async def test_helpful_error_for_unavailable_backend(self):
        """Test that attempting to use unavailable backend provides helpful guidance."""
        with pytest.raises(ValueError) as exc_info:
            await asyncflow.factory.create_backend("dask")

        error_msg = str(exc_info.value)

        # Should provide comprehensive error information
        assert "Backend 'dask' is not available" in error_msg
        assert "Installation hint:" in error_msg
        assert "radical.asyncflow[dask]" in error_msg
        assert "Available backends:" in error_msg

        # Should list working backends
        assert "concurrent" in error_msg
        assert "noop" in error_msg

    def test_backend_specs_accessibility(self):
        """Test that backend specifications are properly configured."""
        specs = asyncflow.registry._backend_specs

        # Verify core backend specs
        assert "noop" in specs
        assert "concurrent" in specs
        assert "radical.asyncflow.backends.execution.noop" in specs["noop"]
        assert "radical.asyncflow.backends.execution.concurrent" in specs["concurrent"]

        # Verify rhapsody backend specs point to correct modules
        assert "dask" in specs
        assert "radical_pilot" in specs
        assert "rhapsody.backends.execution.dask_parallel" in specs["dask"]
        assert "rhapsody.backends.execution.radical_pilot" in specs["radical_pilot"]

    @pytest.mark.asyncio
    async def test_factory_backend_lifecycle(self):
        """Test complete backend lifecycle through factory."""
        # Create backend
        backend = await asyncflow.factory.create_backend("concurrent")

        # Verify backend is properly initialized
        assert hasattr(backend, "submit_tasks")
        assert hasattr(backend, "shutdown")
        assert hasattr(backend, "register_callback")

        # Backend should be in proper state
        state = backend.state()
        valid_states = ["CONNECTED", "READY", "INITIALIZED", "DISCONNECTED", "RUNNING"]
        assert state in valid_states

        # Clean shutdown
        await backend.shutdown()

    def test_optional_dependency_installation_hints(self):
        """Test that installation hints match pyproject.toml configuration."""
        from radical.asyncflow.backends.factory import BackendFactory

        # Test specific backend installation hints
        dask_hint = BackendFactory._suggest_installation("dask")
        assert "radical.asyncflow[dask]" in dask_hint

        radicalpilot_hint = BackendFactory._suggest_installation("radical_pilot")
        assert "radical.asyncflow[radicalpilot]" in radicalpilot_hint

        # Test generic HPC hint
        unknown_hint = BackendFactory._suggest_installation("unknown_hpc_backend")
        assert "radical.asyncflow[hpc]" in unknown_hint

    @pytest.mark.asyncio
    async def test_multiple_backend_instances(self):
        """Test that plugin system can create multiple backend instances."""
        # Create multiple instances of same backend type
        backend1 = await asyncflow.factory.create_backend("noop")
        backend2 = await asyncflow.factory.create_backend("noop")

        # Should be different instances
        assert backend1 is not backend2
        assert id(backend1) != id(backend2)

        # Both should be functional
        assert hasattr(backend1, "submit_tasks")
        assert hasattr(backend2, "submit_tasks")

        # Clean shutdown both
        await backend1.shutdown()
        await backend2.shutdown()

    def test_registry_caching_behavior(self):
        """Test that registry properly caches backend class loading."""
        registry = asyncflow.registry

        # First access - should load and cache
        noop_class1 = registry.get_backend("noop")
        assert noop_class1 is not None

        # Second access - should use cache
        noop_class2 = registry.get_backend("noop")
        assert noop_class2 is not None
        assert noop_class1 is noop_class2  # Should be same cached class

        # Verify cache state
        assert "noop" in registry._backends
        assert registry._backends["noop"] is noop_class1

    @pytest.mark.asyncio
    async def test_backend_configuration_passing(self):
        """Test that backend configuration is properly passed through factory."""
        # Test with config dict
        config = {"max_workers": 3, "executor_type": "thread"}
        backend = await asyncflow.factory.create_backend("concurrent", config=config)

        # Backend should be created successfully with config
        assert backend is not None
        await backend.shutdown()

        # Test with kwargs
        backend2 = await asyncflow.factory.create_backend(
            "concurrent", max_workers=2, executor_type="process"
        )

        assert backend2 is not None
        await backend2.shutdown()

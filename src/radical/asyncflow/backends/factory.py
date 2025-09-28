"""Factory for creating execution backends with proper configuration.

This module provides a factory pattern for creating and initializing execution backends,
handling both synchronous and asynchronous backend initialization.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

from .registry import registry

if TYPE_CHECKING:
    from .execution.base import BaseExecutionBackend

logger = logging.getLogger(__name__)


class BackendFactory:
    """Factory for creating and initializing execution backends.

    This factory handles the creation of backend instances, configuration management,
    and proper initialization of both synchronous and asynchronous backends.
    """

    @staticmethod
    def _suggest_installation(backend_type: str) -> str:
        """Provide helpful installation suggestions for missing backends.

        Args:
            backend_type: Backend identifier

        Returns:
            Installation suggestion string
        """
        suggestions = {
            "dask": "pip install 'radical.asyncflow[dask]'",
            "radical_pilot": "pip install 'radical.asyncflow[radical-pilot]'",
            "dragon": "pip install 'radical.asyncflow[dragon]'",
            "flux": "pip install 'radical.asyncflow[flux]'",
        }

        if backend_type in suggestions:
            return f"Try: {suggestions[backend_type]}"
        return "pip install 'radical.asyncflow[hpc]' for HPC backends"

    @staticmethod
    async def create_backend(
        backend_type: str,
        config: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> BaseExecutionBackend:
        """Create and initialize a backend.

        Args:
            backend_type: Backend identifier ('concurrent', 'dask',
                'radical_pilot', etc.)
            config: Backend-specific configuration dictionary
            **kwargs: Additional arguments passed to backend constructor

        Returns:
            Initialized backend instance

        Raises:
            ValueError: If backend is not available
            RuntimeError: If backend initialization fails
        """
        backend_class = registry.get_backend(backend_type)
        if backend_class is None:
            available = list(registry.list_available().keys())
            available_str = ", ".join(sorted(available))
            suggestion = BackendFactory._suggest_installation(backend_type)

            # Get failure reason if available
            failure_reason = registry.get_failure_reason(backend_type)
            failure_info = f"\nReason: {failure_reason}" if failure_reason else ""

            raise ValueError(
                f"Backend '{backend_type}' is not available.\n"
                f"Available backends: {available_str}\n"
                f"Installation hint: {suggestion}{failure_info}"
            )

        try:
            # Handle different backend initialization patterns
            if backend_type == "noop":
                # Noop backend takes no arguments
                logger.debug("Creating noop backend")
                backend = backend_class()

            elif backend_type == "concurrent":
                # Concurrent backend requires an executor
                import concurrent.futures

                # Use config or kwargs to get executor parameters
                max_workers = (config or {}).get("max_workers") or kwargs.get(
                    "max_workers", 4
                )
                executor_type = (config or {}).get("executor_type") or kwargs.get(
                    "executor_type", "thread"
                )

                if executor_type == "process":
                    executor = concurrent.futures.ProcessPoolExecutor(
                        max_workers=max_workers
                    )
                else:
                    executor = concurrent.futures.ThreadPoolExecutor(
                        max_workers=max_workers
                    )

                logger.debug(
                    f"Creating concurrent backend with {executor_type} executor "
                    f"(max_workers={max_workers})"
                )
                backend = backend_class(executor)

                # Concurrent backend is awaitable for async initialization
                if hasattr(backend, "__await__"):
                    backend = await backend

            else:
                # For other backends, try the generic approach
                backend_config = config or {}
                logger.debug(
                    f"Creating backend '{backend_type}' with config: {backend_config}"
                )
                backend = backend_class(backend_config, **kwargs)

                # Handle async initialization if needed
                if hasattr(backend, "__aenter__"):
                    # Backend is an async context manager
                    backend = await backend.__aenter__()
                elif hasattr(backend, "initialize") and callable(backend.initialize):
                    # Backend has an explicit initialize method
                    await backend.initialize()
                elif hasattr(backend, "__await__"):
                    # Backend is awaitable
                    backend = await backend

            logger.info(
                f"Successfully created and initialized '{backend_type}' backend"
            )
            return backend

        except Exception as e:
            logger.error(f"Failed to initialize backend '{backend_type}': {e}")
            raise RuntimeError(
                f"Failed to initialize backend '{backend_type}': {e}"
            ) from e

    @staticmethod
    def list_available_backends() -> dict[str, dict[str, Any]]:
        """List all available backends with detailed information.

        Returns:
            Dictionary with backend info including availability and failure reasons
        """
        info = {}
        for name in registry._backend_specs:
            backend_class = registry.get_backend(name)
            failure_reason = registry.get_failure_reason(name)

            info[name] = {
                "available": backend_class is not None,
                "class": backend_class.__name__ if backend_class else None,
                "failure_reason": failure_reason,
                "installation_hint": (
                    BackendFactory._suggest_installation(name)
                    if backend_class is None
                    else None
                ),
            }

        return info

    @staticmethod
    def create_backend_sync(
        backend_type: str,
        config: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> BaseExecutionBackend:
        """Create a backend synchronously (for backends that don't require async init).

        Args:
            backend_type: Backend identifier
            config: Backend-specific configuration dictionary
            **kwargs: Additional arguments passed to backend constructor

        Returns:
            Backend instance (not initialized if async initialization is required)

        Raises:
            ValueError: If backend is not available
            RuntimeError: If backend creation fails

        Warning:
            This method should only be used for backends that don't require
            asynchronous initialization. Use create_backend() for proper async init.
        """
        backend_class = registry.get_backend(backend_type)
        if backend_class is None:
            available = list(registry.list_available().keys())
            available_str = ", ".join(sorted(available))
            suggestion = BackendFactory._suggest_installation(backend_type)

            raise ValueError(
                f"Backend '{backend_type}' is not available.\n"
                f"Available backends: {available_str}\n"
                f"Installation hint: {suggestion}"
            )

        try:
            backend_config = config or {}
            logger.debug(
                f"Creating backend '{backend_type}' (sync) with config: "
                f"{backend_config}"
            )
            backend = backend_class(backend_config, **kwargs)
            logger.info(f"Successfully created '{backend_type}' backend (sync)")
            return backend

        except Exception as e:
            logger.error(f"Failed to create backend '{backend_type}' (sync): {e}")
            raise RuntimeError(f"Failed to create backend '{backend_type}': {e}") from e


# Convenience factory instance
factory = BackendFactory()

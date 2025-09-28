"""Backend registry with discovery and lazy loading.

This module implements a plugin-style registry for execution backends, allowing
AsyncFlow to discover and load backends on demand without requiring hard dependencies.
"""

from __future__ import annotations

import importlib
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BackendRegistry:
    """Registry for execution backends with lazy loading and discovery.

    This registry manages both core backends (always available) and optional backends
    (loaded on demand from external packages like rhapsody). It provides a clean
    separation between AsyncFlow core functionality and optional HPC/scale-out backends.
    """

    def __init__(self):
        """Initialize the backend registry."""
        self._backends: dict[str, type] = {}
        self._failed_backends: dict[str, str] = {}  # Track failed import reasons

        # Backend specifications: name -> module_path:class_name
        self._backend_specs = {
            # Core backends (should always be available)
            "noop": "radical.asyncflow.backends.execution.noop:NoopExecutionBackend",
            "concurrent": (
                "radical.asyncflow.backends.execution.concurrent:"
                "ConcurrentExecutionBackend"
            ),
            # Optional rhapsody backends (loaded on demand)
            "dask": "rhapsody.backends.execution:DaskExecutionBackend",
            "radical_pilot": "rhapsody.backends.execution:RadicalExecutionBackend",
            "dragon": "rhapsody.backends.execution:DragonExecutionBackend",
            "flux": "rhapsody.backends.execution:FluxExecutionBackend",
        }

    def get_backend(self, name: str) -> Optional[type]:
        """Get backend class by name, loading it if necessary.

        Args:
            name: Backend identifier (e.g., 'dask', 'radical_pilot', 'noop')

        Returns:
            Backend class if available and successfully loaded, None otherwise
        """
        # Return cached backend if already loaded
        if name in self._backends:
            return self._backends[name]

        # Return None if previously failed to load
        if name in self._failed_backends:
            error_msg = self._failed_backends[name]
            logger.debug(f"Backend '{name}' previously failed to load: {error_msg}")
            return None

        # Check if backend specification exists
        if name not in self._backend_specs:
            available = list(self._backend_specs.keys())
            logger.debug(f"Unknown backend '{name}'. Available: {available}")
            return None

        # Attempt to load the backend
        try:
            module_path, class_name = self._backend_specs[name].split(":")
            module = importlib.import_module(module_path)
            backend_class = getattr(module, class_name)

            # Validate that it's a proper backend
            from radical.asyncflow.backends.execution.base import BaseExecutionBackend

            # Check if it's a BaseExecutionBackend subclass
            if not issubclass(backend_class, BaseExecutionBackend):
                error_msg = f"Class {class_name} is not a BaseExecutionBackend subclass"
                self._failed_backends[name] = error_msg
                logger.error(f"Backend '{name}' validation failed: {error_msg}")
                return None

            # Cache successful load
            self._backends[name] = backend_class
            logger.debug(f"Successfully loaded backend '{name}' from {module_path}")
            return backend_class

        except ImportError as e:
            error_msg = f"Import error: {e}"
            self._failed_backends[name] = error_msg
            logger.debug(f"Backend '{name}' not available: {error_msg}")
            return None
        except AttributeError as e:
            error_msg = f"Class not found: {e}"
            self._failed_backends[name] = error_msg
            logger.error(f"Backend '{name}' load failed: {error_msg}")
            return None
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            self._failed_backends[name] = error_msg
            logger.error(f"Backend '{name}' load failed: {error_msg}")
            return None

    def list_available(self) -> dict[str, bool]:
        """List all backends and their availability status.

        Returns:
            Dictionary mapping backend name to availability (True/False)
        """
        available = {}
        for name in self._backend_specs:
            available[name] = self.get_backend(name) is not None
        return available

    def list_loaded(self) -> dict[str, type]:
        """Get all currently loaded backends.

        Returns:
            Dictionary mapping backend name to backend class
        """
        return self._backends.copy()

    def get_failure_reason(self, name: str) -> Optional[str]:
        """Get the reason why a backend failed to load.

        Args:
            name: Backend identifier

        Returns:
            Failure reason string if backend failed to load, None otherwise
        """
        return self._failed_backends.get(name)

    def register_backend(self, name: str, backend_class: type) -> None:
        """Register a backend class directly (for testing or custom backends).

        Args:
            name: Backend identifier
            backend_class: Backend class to register

        Raises:
            TypeError: If backend_class is not a BaseExecutionBackend subclass
        """
        from radical.asyncflow.backends.execution.base import BaseExecutionBackend

        if not issubclass(backend_class, BaseExecutionBackend):
            raise TypeError("Backend class must be a BaseExecutionBackend subclass")

        self._backends[name] = backend_class
        # Remove from failed backends if it was there
        self._failed_backends.pop(name, None)
        logger.debug(f"Registered backend '{name}': {backend_class}")

    def add_backend_spec(self, name: str, module_class_spec: str) -> None:
        """Add a new backend specification for lazy loading.

        Args:
            name: Backend identifier
            module_class_spec: Module and class specification in format
                "module.path:ClassName"
        """
        self._backend_specs[name] = module_class_spec
        # Clear any cached results for this backend
        self._backends.pop(name, None)
        self._failed_backends.pop(name, None)
        logger.debug(f"Added backend spec '{name}': {module_class_spec}")


# Global registry instance
registry = BackendRegistry()

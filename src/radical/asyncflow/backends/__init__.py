"""Backend subsystem for AsyncFlow with plugin-based architecture.

This module provides a plugin-based backend system that supports both local backends
(for development and testing) and optional external backends (for HPC and scale-out
execution) without requiring hard dependencies.
"""

from __future__ import annotations

from .execution import ConcurrentExecutionBackend, NoopExecutionBackend

# Import core components
from .execution.base import BaseExecutionBackend, Session
from .factory import factory
from .registry import registry

__all__ = [
    "BaseExecutionBackend",
    "Session",
    "NoopExecutionBackend",
    "ConcurrentExecutionBackend",
    "factory",
    "registry",
]

"""Execution backends for AsyncFlow with local and optional external backends.

This module provides core local execution backends and optional external backends
through the registry system.
"""

from __future__ import annotations

# Import base class
from .base import BaseExecutionBackend

# Import local core backends (always available)
from .concurrent import ConcurrentExecutionBackend
from .noop import NoopExecutionBackend

__all__ = ["BaseExecutionBackend", "NoopExecutionBackend", "ConcurrentExecutionBackend"]

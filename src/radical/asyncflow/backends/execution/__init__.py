"""Execution backends for AsyncFlow using Rhapsody backends.

This module re-exports all execution backends from rhapsody.backends.execution.
"""

from __future__ import annotations

# Import execution backends from rhapsody
from rhapsody.backends.execution import ConcurrentExecutionBackend, NoopExecutionBackend

# Import base class for compatibility
from ..base import BaseExecutionBackend

__all__ = ["BaseExecutionBackend", "NoopExecutionBackend", "ConcurrentExecutionBackend"]

# Add optional backends
try:
    from rhapsody.backends.execution import DaskExecutionBackend  # noqa: F401

    __all__.append("DaskExecutionBackend")
except ImportError:
    pass

try:
    from rhapsody.backends.execution import RadicalExecutionBackend  # noqa: F401

    __all__.append("RadicalExecutionBackend")
except ImportError:
    pass

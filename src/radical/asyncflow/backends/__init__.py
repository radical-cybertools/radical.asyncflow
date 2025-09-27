"""Backend subsystem for AsyncFlow using Rhapsody backends.

This module provides execution backends from the Rhapsody package for running
scientific workflows on various computing infrastructures.
"""

from __future__ import annotations

# Import execution backends from rhapsody
from rhapsody.backends.execution import ConcurrentExecutionBackend, NoopExecutionBackend

# Import base class and session from our compatibility layer
from .base import BaseExecutionBackend, Session

__all__ = [
    "BaseExecutionBackend",
    "Session",
    "NoopExecutionBackend",
    "ConcurrentExecutionBackend",
]

# Add optional backends that may be available
try:
    from rhapsody.backends.execution import DaskExecutionBackend

    __all__.append("DaskExecutionBackend")
except ImportError:
    pass

try:
    from rhapsody.backends.execution import RadicalExecutionBackend

    __all__.append("RadicalExecutionBackend")
except ImportError:
    pass

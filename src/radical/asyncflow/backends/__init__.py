"""Backend subsystem for AsyncFlow using Rhapsody backends.

This module provides execution backends from the Rhapsody package for running scientific
workflows on various computing infrastructures.
"""

from __future__ import annotations

from rhapsody.backends.execution import ConcurrentExecutionBackend, NoopExecutionBackend

from ..utils import register_optional_backends
from .base import BaseExecutionBackend, Session

__all__ = [
    "BaseExecutionBackend",
    "Session",
    "NoopExecutionBackend",
    "ConcurrentExecutionBackend",
]

# Register optional backends from rhapsody
register_optional_backends(
    globals(),
    __all__,
    "rhapsody.backends.execution",
    ["DaskExecutionBackend", "RadicalExecutionBackend"],
)

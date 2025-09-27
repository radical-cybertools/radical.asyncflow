"""Execution backends for AsyncFlow using Rhapsody backends.

This module re-exports all execution backends from rhapsody.backends.execution.
"""

from __future__ import annotations

from rhapsody.backends.execution import ConcurrentExecutionBackend, NoopExecutionBackend

from ...utils import register_optional_backends
from ..base import BaseExecutionBackend

__all__ = ["BaseExecutionBackend", "NoopExecutionBackend", "ConcurrentExecutionBackend"]

# Register optional backends from rhapsody
register_optional_backends(
    globals(),
    __all__,
    "rhapsody.backends.execution",
    ["DaskExecutionBackend", "RadicalExecutionBackend"],
)

from __future__ import annotations

import importlib.metadata as importlib_metadata

# Import backends from rhapsody through our wrapper
from .backends.execution import ConcurrentExecutionBackend, NoopExecutionBackend
from .data import InputFile, OutputFile
from .utils import register_optional_backends
from .workflow_manager import WorkflowEngine

__version__ = importlib_metadata.version("radical.asyncflow")

__all__ = [
    "WorkflowEngine",
    "InputFile",
    "OutputFile",
    "ConcurrentExecutionBackend",
    "NoopExecutionBackend",
]

# Register optional backends from rhapsody
register_optional_backends(
    globals(),
    __all__,
    "radical.asyncflow.backends.execution",
    ["DaskExecutionBackend", "RadicalExecutionBackend"],
)

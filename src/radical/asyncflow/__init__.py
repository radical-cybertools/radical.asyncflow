from __future__ import annotations

import importlib.metadata as importlib_metadata

# Import backends from rhapsody through our wrapper
from .backends.execution import ConcurrentExecutionBackend, NoopExecutionBackend

# Try to import optional backends
try:
    from .backends.execution import DaskExecutionBackend
except ImportError:
    DaskExecutionBackend = None

try:
    from .backends.execution import RadicalExecutionBackend
except ImportError:
    RadicalExecutionBackend = None

from .data import InputFile, OutputFile
from .workflow_manager import WorkflowEngine

__version__ = importlib_metadata.version("radical.asyncflow")

__all__ = [
    "WorkflowEngine",
    "InputFile",
    "OutputFile",
    "ConcurrentExecutionBackend",
    "NoopExecutionBackend",
]

# Add optional backends to __all__ if they exist
if DaskExecutionBackend is not None:
    __all__.append("DaskExecutionBackend")

if RadicalExecutionBackend is not None:
    __all__.append("RadicalExecutionBackend")

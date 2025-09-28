from __future__ import annotations

import importlib.metadata as importlib_metadata

# Import core components with new plugin architecture
from .backends import factory, registry
from .backends.execution import ConcurrentExecutionBackend, NoopExecutionBackend
from .data import InputFile, OutputFile
from .workflow_manager import WorkflowEngine

__version__ = importlib_metadata.version("radical.asyncflow")

__all__ = [
    "WorkflowEngine",
    "InputFile",
    "OutputFile",
    "ConcurrentExecutionBackend",
    "NoopExecutionBackend",
    "factory",
    "registry",
]

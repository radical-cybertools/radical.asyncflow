from __future__ import annotations

import importlib.metadata as importlib_metadata

from .backends import LocalExecutionBackend, NoopExecutionBackend
from .data import InputFile, OutputFile
from .workflow_manager import WorkflowEngine

__version__ = importlib_metadata.version("radical.asyncflow")

__all__ = [
    "InputFile",
    "OutputFile",
    "WorkflowEngine",
    "NoopExecutionBackend",
    "LocalExecutionBackend",
]

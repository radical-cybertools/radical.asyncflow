from __future__ import annotations

import importlib.metadata as importlib_metadata

from .data import InputFile, OutputFile
from .workflow_manager import WorkflowEngine
from .noop_executor import NoopExecutionBackend

__version__ = importlib_metadata.version("radical.asyncflow")

__all__ = [
    "InputFile",
    "OutputFile",
    "WorkflowEngine",
    "NoopExecutionBackend"
]

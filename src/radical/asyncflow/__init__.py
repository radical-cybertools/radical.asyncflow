from __future__ import annotations

import importlib.metadata as importlib_metadata

from .backends import LocalExecutionBackend, NoopExecutionBackend
from .data import InputFile, OutputFile
from .policies import (
    FifoPolicy,
    MemoryModel,
    NodeShape,
    Pool,
    ResourcePolicy,
)
from .workflow_manager import WorkflowEngine

__version__ = importlib_metadata.version("radical.asyncflow")

__all__ = [
    "InputFile",
    "OutputFile",
    "WorkflowEngine",
    "NoopExecutionBackend",
    "LocalExecutionBackend",
    "FifoPolicy",
    "ResourcePolicy",
    "Pool",
    "NodeShape",
    "MemoryModel",
]

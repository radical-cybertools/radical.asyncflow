from __future__ import annotations

import importlib.metadata as importlib_metadata

from .backends.execution.concurrent import ConcurrentExecutionBackend
from .backends.execution.dask_parallel import DaskExecutionBackend
from .backends.execution.noop import NoopExecutionBackend
from .backends.execution.radical_pilot import RadicalExecutionBackend
from .backends.execution.dragon import DragonExecutionBackend
from .data import InputFile, OutputFile
from .workflow_manager import WorkflowEngine

__version__ = importlib_metadata.version("radical.asyncflow")

__all__ = [
    "DragonExecutionBackend",
    "ConcurrentExecutionBackend",
    "DaskExecutionBackend",
    "NoopExecutionBackend",
    "RadicalExecutionBackend",
    "InputFile",
    "OutputFile",
    "WorkflowEngine",
]

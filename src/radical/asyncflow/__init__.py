from __future__ import annotations

import importlib.metadata as importlib_metadata

from .task import Task
from .data import InputFile
from .data import OutputFile
from .workflow_manager import WorkflowEngine
from .backends.execution.noop import NoopExecutionBackend
from .backends.execution.thread_pool import ThreadExecutionBackend
from .backends.execution.dask_parallel import DaskExecutionBackend
from .backends.execution.radical_pilot import RadicalExecutionBackend

__version__ = importlib_metadata.version('radical.asyncflow')

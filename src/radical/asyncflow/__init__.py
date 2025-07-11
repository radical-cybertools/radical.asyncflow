import os as _os
import radical.utils as _ru

from .task import Task
from .data import InputFile
from .data import OutputFile
from .workflow_manager import WorkflowEngine
from .backends.execution.noop import NoopExecutionBackend
from .backends.execution.dask_parallel import DaskExecutionBackend
from .backends.execution.concurrent import ConcurrentExecutionBackend
from .backends.execution.radical_pilot import RadicalExecutionBackend


# ------------------------------------------------------------------------------
#
# get version info
#
_mod_root = _os.path.dirname (__file__)

version_short, version_base, version_branch, version_tag, version_detail \
             = _ru.get_version(_mod_root)
version      = version_short
__version__  = version_detail

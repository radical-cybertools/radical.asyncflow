import os as _os
import radical.utils as _ru

from radical.asyncflow.task import Task
from radical.asyncflow.data import InputFile
from radical.asyncflow.data import OutputFile
from radical.asyncflow.workflow_manager import WorkflowEngine
from radical.asyncflow.backends.execution.noop import NoopExecutionBackend
from radical.asyncflow.backends.execution.thread_pool import ThreadExecutionBackend
from radical.asyncflow.backends.execution.dask_parallel import DaskExecutionBackend
from radical.asyncflow.backends.execution.radical_pilot import RadicalExecutionBackend


# ------------------------------------------------------------------------------
#
# get version info
#
_mod_root = _os.path.dirname (__file__)

version_short, version_base, version_branch, version_tag, version_detail \
             = _ru.get_version(_mod_root)
version      = version_short
__version__  = version_detail

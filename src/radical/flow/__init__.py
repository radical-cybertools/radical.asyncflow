import os as _os
import radical.utils as _ru

from radical.flow.task import Task
from radical.flow.workflow_manager import WorkflowEngine
from radical.flow.backends.execution.noop import NoopExecutionBackend
from radical.flow.backends.execution.thread_pool import ThreadExecutionBackend
from radical.flow.backends.execution.dask_parallel import DaskExecutionBackend
from radical.flow.backends.execution.radical_pilot import RadicalExecutionBackend


# ------------------------------------------------------------------------------
#
# get version info
#
_mod_root = _os.path.dirname (__file__)

version_short, version_base, version_branch, version_tag, version_detail \
             = _ru.get_version(_mod_root)
version      = version_short
__version__  = version_detail

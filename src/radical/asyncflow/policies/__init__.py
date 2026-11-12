"""Ready-queue policies: which ready task the engine hands to the backend next.

Without a policy the engine submits every ready task at once and the backend's
own queue decides the order. With a policy the engine gives each ready task to
the policy and submits the tasks the policy dispatches, so the policy decides
how many tasks are in flight and which ready task starts next::

    flow = await WorkflowEngine.create(backend=backend, policy=FifoPolicy(64))

Only plain tasks go through a policy. Blocks run inside the engine and services
are long-lived, so both bypass it.

A policy implements the four operations of :class:`Policy`, in the spirit of
sched_ext's enqueue, dispatch and stopping operations. Any object that has them
works. Task descriptions given to a policy carry ``_seq``, the registration
order; ``_lineage``, the ``_seq`` of the oldest ancestor (a dependency or the
enclosing block, transitively); ``_scope``, the ``workflow_id`` the task, or its
outermost enclosing block, was called with; and ``_backend``, the backend it runs
on.

A task may declare a priority, a number where larger runs sooner (default 0)::

    await train(data, task_description={"priority": 10})

The built-in policies order by priority first, unless their documentation says
otherwise. Backends that read the same key, such as Dask's, give it the same
meaning.
"""

from .base import Policy
from .fair import FairPolicy
from .fifo import FifoPolicy

__all__ = ["Policy", "FifoPolicy", "FairPolicy"]

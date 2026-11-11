"""LineagePolicy: finish started workflows before starting new ones."""

from __future__ import annotations

from .base import priority
from .fifo import FifoPolicy


class LineagePolicy(FifoPolicy):
    """Oldest lineage first: finish started workflows before starting new ones.

    Ready tasks start by priority, then by lineage, then in registration order. A
    task's lineage is the registration order of its oldest ancestor: a dependency
    or the enclosing block, transitively. So when a task completes, the dependents
    it releases start before unrelated newer work, much like CFS's "next buddy"
    preference. This lowers the mean workflow completion time and the number of
    half-finished workflows, whose intermediate results stay live, for about the
    same makespan.

    Args:
        capacity: The most tasks in flight at once; set it to the number of
            tasks the backend runs at once.
    """

    def _key(self, desc: dict) -> tuple:
        return (-priority(desc), desc["_lineage"], desc["_seq"])

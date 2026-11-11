"""The operations of a ready-queue policy, and what policies share."""

from __future__ import annotations

import math
from typing import Optional, Protocol


class Policy(Protocol):
    """The operations the engine calls on a ready-queue policy.

    The engine calls them on its event-loop thread, so a policy needs no locking.
    A policy serves one engine at a time. If ``enqueue`` raises, the task fails;
    if ``release`` or ``runnable`` raises, the engine logs it and goes on; if
    ``dispatch`` raises, the engine shuts down, as for any error in its run loop.
    """

    def enqueue(self, uid: str, desc: dict) -> None:
        """Task ``uid`` is ready to run; ``desc`` is its description.

        Raise to reject the task: the engine fails it with that exception.
        """

    def dispatch(self) -> list[dict]:
        """Return the descriptions of the tasks to submit now.

        The engine calls it again after it returned tasks, or after a task left the
        policy.
        """

    def release(
        self, uid: str, state: Optional[str], observed: Optional[dict] = None
    ) -> None:
        """Task ``uid`` leaves the policy.

        ``state`` is how the task ended on the backend, as ``future.state`` spells
        it: ``"DONE"``, ``"FAILED"`` or ``"CANCELLED"``; ``observed`` is the
        resource use the backend reported for it under the ``observed`` key of its
        description (for example ``{"host_mem": 12.5}``, in GiB), or None. ``state``
        is None for a task the backend never got: its future resolved while the
        policy held it (a cancel), or before the engine submitted it; and for the
        tasks of an engine that nothing reports to any more (collected without a
        shutdown, or whose loop closed). When the engine shuts down, a task still
        in flight is released as ``"CANCELLED"``. Ignore uids that are unknown or
        already released.
        """

    def runnable(self) -> bool:
        """Return True if :meth:`dispatch` may return a task now, False if it would not.

        The engine sleeps while this is False, until a task is enqueued or released.
        """


def check_capacity(capacity: int) -> int:
    """Return ``capacity`` if it is a positive int; raise ValueError otherwise."""
    if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
        raise ValueError(f"capacity must be a positive int, not {capacity!r}")
    return capacity


def priority(desc: dict) -> float:
    """The priority the task declares as ``task_description["priority"]``.

    A finite number, larger runs sooner; 0 when none is declared. Raise
    ValueError for anything else, which makes a policy's ``enqueue`` reject the
    task.
    """
    value = (desc.get("task_backend_specific_kwargs") or {}).get("priority", 0)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"priority must be a finite number, not {value!r}")
    return value

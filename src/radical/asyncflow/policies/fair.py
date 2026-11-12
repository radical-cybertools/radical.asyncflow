"""FairPolicy: weighted fair share across workflows, in the style of CFS."""

from __future__ import annotations

import heapq
import itertools
import math
import time
from collections.abc import Callable
from typing import Optional

from .base import check_capacity, priority


class _Group:
    __slots__ = ("vruntime", "weight", "queue", "queued", "inflight")

    def __init__(self, vruntime: float, weight: float):
        self.vruntime = vruntime
        self.weight = weight
        self.queue: list = []  # heap of (-priority, order, uid); dropped uids stay
        self.queued = 0
        self.inflight = 0


class FairPolicy:
    """Weighted fair share across workflows, in the style of CFS.

    A group is a workflow: the scope the task was called in (``workflow_scope``
    or a ``workflow_id``, also through blocks), else its lineage. Each group
    accrues virtual runtime, its service divided by its weight, and the runnable
    group with the least virtual runtime goes next. Service is charged at dispatch
    from a per-function runtime estimate, an exponential average like PELT, and
    corrected when the backend reports the task with the time it was in flight;
    only tasks that end ``DONE`` update the estimate; a correction or refund never
    takes a group below ``min_vruntime``, nor below where it stood. A group with
    nothing queued that gets new work starts at no less than ``min_vruntime``, so
    it banks no credit while it has nothing to run, however long its tasks in
    flight take; one that ran ahead keeps that debt while it is idle, for the last
    ``IDLE_GROUPS`` such groups. Within a group, tasks start by priority, then in
    the order they became ready.

    Args:
        capacity: The most tasks in flight at once; set it to the number of
            tasks the backend runs at once.
        weights: Optional positive weight per scope. A key also matches scopes
            that start with it followed by ``":"``; the longest matching key wins.
            The default weight is 1.
        clock: The clock used to measure runtimes.
    """

    DEFAULT_EST = 1e-3  # seconds charged for a function not seen to finish yet
    ALPHA = 0.2  # weight of the newest runtime in a function's estimate
    IDLE_GROUPS = 1024  # idle groups whose debt is remembered

    def __init__(
        self,
        capacity: int,
        weights: Optional[dict] = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.capacity = check_capacity(capacity)
        self.weights = dict(weights or {})
        for key, w in self.weights.items():
            if (
                isinstance(w, bool)
                or not isinstance(w, (int, float))
                or not math.isfinite(w)
                or w <= 0
            ):
                raise ValueError(f"weight of {key!r} must be positive, not {w!r}")
        self.clock = clock
        self.min_vruntime = 0.0
        self._groups: dict = {}  # gid -> _Group, while it has tasks
        self._idle: dict = {}  # gid -> vruntime of an idle group ahead, oldest first
        self._heap: list = []  # (vruntime, tiebreak, gid); stale entries skipped
        self._tiebreak = itertools.count()
        self._order = itertools.count()
        self._est: dict = {}  # function name -> runtime estimate
        self._waiting: dict = {}  # uid -> (gid, desc), for each queued task
        self._inflight: dict = {}  # uid -> (gid, name, est, t0)

    def _weight(self, gid) -> float:
        w = None
        while isinstance(gid, str):
            w = self.weights.get(gid)
            if w is not None or ":" not in gid:
                break
            gid = gid.rsplit(":", 1)[0]  # the longest key that is a prefix
        return 1.0 if w is None else w

    def _push(self, gid, group: _Group) -> None:
        heapq.heappush(self._heap, (group.vruntime, next(self._tiebreak), gid))

    def enqueue(self, uid: str, desc: dict) -> None:
        key = (-priority(desc), next(self._order), uid)
        gid = desc.get("_scope") or ("lineage", desc["_lineage"])
        group = self._groups.get(gid)
        if group is None:
            vruntime = self._idle.pop(gid, self.min_vruntime)
            group = self._groups[gid] = _Group(vruntime, self._weight(gid))
        if not group.queued:  # it becomes runnable: it banks no credit
            if group.inflight:
                self._settle(gid, group)
            group.vruntime = max(group.vruntime, self.min_vruntime)
            self._push(gid, group)
        heapq.heappush(group.queue, key)
        group.queued += 1
        self._waiting[uid] = (gid, desc)

    def _settle(self, gid, group: _Group) -> None:
        """Charges the group's tasks in flight the time they ran beyond their estimates,
        so far; their release then corrects only the rest."""
        now = self.clock()
        for uid, (g, name, est, t0) in self._inflight.items():
            if g == gid and now - t0 > est:
                group.vruntime += (now - t0 - est) / group.weight
                self._inflight[uid] = (g, name, now - t0, t0)

    def runnable(self) -> bool:
        return bool(self._waiting) and len(self._inflight) < self.capacity

    def dispatch(self) -> list[dict]:
        out = []
        while self._waiting and len(self._inflight) < self.capacity:
            vruntime, _, gid = heapq.heappop(self._heap)
            group = self._groups.get(gid)
            if group is None or not group.queued or vruntime != group.vruntime:
                continue  # stale entry
            uid = heapq.heappop(group.queue)[2]
            while uid not in self._waiting:  # dropped while queued
                uid = heapq.heappop(group.queue)[2]
            desc = self._waiting.pop(uid)[1]
            group.queued -= 1
            group.inflight += 1
            self.min_vruntime = max(self.min_vruntime, vruntime)
            est = self._est.get(desc["name"], self.DEFAULT_EST)
            group.vruntime += est / group.weight
            self._inflight[uid] = (gid, desc["name"], est, self.clock())
            if group.queued:
                self._push(gid, group)
            out.append(desc)
        return out

    def release(
        self, uid: str, state: Optional[str], observed: Optional[dict] = None
    ) -> None:
        queued = self._waiting.pop(uid, None)
        if queued is not None:  # dropped while queued: it was never charged
            gid = queued[0]
            group = self._groups[gid]
            group.queued -= 1
            self._retire_idle(gid, group)
            return
        rec = self._inflight.pop(uid, None)
        if rec is None:
            return
        gid, name, est, t0 = rec
        group = self._groups[gid]
        group.inflight -= 1
        if state is None:
            delta = -est  # refund: the backend never ran it
        else:
            actual = self.clock() - t0
            delta = actual - est
            if state == "DONE":  # a failed or cancelled run says little of the next
                old = self._est.get(name)
                self._est[name] = (
                    actual if old is None else old + self.ALPHA * (actual - old)
                )
        vruntime = group.vruntime + delta / group.weight
        if delta < 0:  # no credit below min_vruntime, nor below where it stood
            vruntime = max(vruntime, min(group.vruntime, self.min_vruntime))
        group.vruntime = vruntime
        if group.queued:
            self._push(gid, group)  # re-key; the old entry is now stale
        self._retire_idle(gid, group)

    def _retire_idle(self, gid, group: _Group) -> None:
        # A group with nothing queued or in flight leaves the active groups. One
        # ahead of min_vruntime keeps its debt for when it comes back, for the
        # last IDLE_GROUPS of them; the others would restart at min_vruntime anyway.
        if group.queued or group.inflight:
            return
        del self._groups[gid]
        if group.vruntime > self.min_vruntime:
            self._idle[gid] = group.vruntime
            if len(self._idle) > self.IDLE_GROUPS:
                del self._idle[next(iter(self._idle))]

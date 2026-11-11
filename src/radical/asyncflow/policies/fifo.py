"""FifoPolicy: ready tasks start by priority, then in the order they became ready."""

from __future__ import annotations

import heapq
import itertools
from typing import Optional

from .base import check_capacity, priority


class FifoPolicy:
    """Ready tasks start by priority, then in the order they became ready.

    At most ``capacity`` tasks are in flight at once. Without declared priorities
    the order is first in, first out.

    Args:
        capacity: The most tasks in flight at once; set it to the number of
            tasks the backend runs at once.
    """

    def __init__(self, capacity: int):
        self.capacity = check_capacity(capacity)
        self._queue: list = []  # heap of (key, uid); entries of dropped uids stay
        self._waiting: dict = {}  # uid -> (key, description), for each queued task
        self._inflight: set = set()
        self._order = itertools.count()

    def _key(self, desc: dict) -> tuple:
        return (-priority(desc), next(self._order))

    def enqueue(self, uid: str, desc: dict) -> None:
        key = self._key(desc)
        heapq.heappush(self._queue, (key, uid))
        self._waiting[uid] = (key, desc)

    def runnable(self) -> bool:
        return bool(self._waiting) and len(self._inflight) < self.capacity

    def dispatch(self) -> list[dict]:
        out = []
        while self._waiting and len(self._inflight) < self.capacity:
            uid = heapq.heappop(self._queue)[1]
            queued = self._waiting.pop(uid, None)
            if queued is None:
                continue  # dropped while queued
            self._inflight.add(uid)
            out.append(queued[1])
        if not self._waiting:
            self._queue.clear()  # only entries of dropped tasks are left
        return out

    def release(
        self, uid: str, state: Optional[str], observed: Optional[dict] = None
    ) -> None:
        if self._waiting.pop(uid, None) is None:
            self._inflight.discard(uid)
        elif len(self._queue) > 2 * len(self._waiting) + 32:
            # a queued task is dropped; rebuild the heap once dropped entries,
            # skipped when popped, outnumber the live ones
            self._queue = [(key, u) for u, (key, _) in self._waiting.items()]
            heapq.heapify(self._queue)

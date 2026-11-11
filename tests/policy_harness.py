"""Helpers for the tests of ready-queue policies driving the engine."""

import asyncio

import pytest

from radical.asyncflow import NoopExecutionBackend


def desc(uid, seq=0, lineage=None, name="f", workflow_id=None, **task_description):
    """A task description as the engine hands it to a policy."""
    return {
        "uid": uid,
        "name": name,
        "_seq": seq,
        "_lineage": seq if lineage is None else lineage,
        "workflow_id": workflow_id,
        "_scope": workflow_id,
        "task_backend_specific_kwargs": task_description,
    }


def uids(descs):
    return [d["uid"] for d in descs]


def leftovers(policy):
    """What a built-in policy still holds, by attribute: queued or in flight tasks,
    active groups, tasks without a pool, reservations."""
    names = ("_waiting", "_inflight", "_groups", "_free", "_reserved")
    found = {name: getattr(policy, name, None) for name in names}
    return {name: list(held) for name, held in found.items() if held}


class ParkingBackend(NoopExecutionBackend):
    """Holds every task until the test finishes it, reporting it RUNNING; services
    become ready at once.

    Records the submission order and the most tasks in flight. With ``gate`` set,
    ``submit_tasks`` waits for it before it takes the tasks, so a test can act
    while the engine awaits the submission.
    """

    def __init__(self):
        super().__init__()
        self.submitted = []  # (name, first arg)
        self.parked = {}  # uid -> task, in submission order
        self.max_inflight = 0
        self.gate = None

    async def submit_tasks(self, tasks):
        if self.gate is not None:
            await self.gate.wait()
        for t in tasks:
            self.submitted.append((t["name"], (t.get("args") or [None])[0]))
            if t.get("is_service"):

                def ready(fut, task, state):
                    if not fut.done():
                        fut.set_result("ready")

                self._callback_func(t, "RUNNING", service_callback=ready)
                continue
            self.parked[t["uid"]] = t
            self.max_inflight = max(self.max_inflight, len(self.parked))
            self._callback_func(t, "RUNNING")

    def finish(self, uid=None, state="DONE"):
        """Reports a parked task, the oldest by default, in ``state``."""
        task = self.parked.pop(uid or next(iter(self.parked)))
        task["return_value"] = task["stdout"] = None
        if state == "FAILED":
            task["exception"] = RuntimeError(f"{task['name']} failed")
        self._callback_func(task, state)
        return task

    async def cancel_task(self, uid):
        if uid not in self.parked:
            return False
        self.finish(uid, "CANCELED")
        return True


async def until(cond, limit=5.0):
    """Waits until ``cond()`` is true; fails the test after ``limit`` seconds."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + limit
    while not cond():
        if loop.time() > deadline:
            pytest.fail("the engine did not reach the expected state")
        await asyncio.sleep(0.001)


async def run_all(backend, futs):
    """Finishes parked tasks, oldest first, until every future is done."""
    while not all(f.done() for f in futs):
        await until(lambda: backend.parked or all(f.done() for f in futs))
        if backend.parked:
            backend.finish()
        await asyncio.sleep(0)

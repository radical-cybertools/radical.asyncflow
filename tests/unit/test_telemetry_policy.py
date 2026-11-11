"""Policy telemetry: the tracing proxy, its events, and joining them by uid."""

import asyncio
import json
import logging
from collections import deque

import pytest

from radical.asyncflow import FifoPolicy, NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.policy_telemetry import (
    POLICY_EVENTS,
    TracedPolicy,
    load_events,
    occupancy,
    timeline,
)
from tests.policy_harness import ParkingBackend, until


class _Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class _Recorder:
    """Stands in for the engine's _emit."""

    def __init__(self):
        self.events = []

    def __call__(self, name, *, task_id, backend=None, workflow_id=None, **fields):
        assert name in POLICY_EVENTS
        self.events.append(
            {
                "event_type": name,
                "task_id": task_id,
                "backend": backend,
                "workflow_id": workflow_id,
                **fields,
            }
        )

    def of(self, name):
        return [e for e in self.events if e["event_type"] == name]


def _desc(uid, seq=0, lineage=None, **extra):
    return {
        "uid": uid,
        "name": "f",
        "_seq": seq,
        "_lineage": seq if lineage is None else lineage,
        **extra,
    }


def _traced(policy, **kwargs):
    rec, clock = _Recorder(), _Clock()
    return TracedPolicy(policy, rec, clock=clock, **kwargs), rec, clock


class _Slots:
    """First in, first out, ``capacity`` at a time.

    A release drops a queued task, as the policy layer asks, and returns what the proxy
    must pass back.
    """

    def __init__(self, capacity):
        self.capacity = capacity
        self.queue = deque()
        self.inflight = set()

    def enqueue(self, uid, desc):
        self.queue.append(desc)

    def runnable(self):
        return bool(self.queue) and len(self.inflight) < self.capacity

    def dispatch(self):
        out = []
        while self.queue and len(self.inflight) < self.capacity:
            desc = self.queue.popleft()
            self.inflight.add(desc["uid"])
            out.append(desc)
        return out

    def release(self, uid, state, observed=None):
        for desc in self.queue:
            if desc["uid"] == uid:
                self.queue.remove(desc)
                return "dropped"
        if uid in self.inflight:
            self.inflight.discard(uid)
            return "freed"
        return None


# ---------------------------------------------------------------------------
# The proxy on its own
# ---------------------------------------------------------------------------


def test_dispatch_reports_held_time_order_and_key():
    traced, rec, clock = _traced(_Slots(2))
    for i in range(3):
        declared = {"priority": 5 - i}  # task_description["priority"]
        traced.enqueue(
            f"t{i}", _desc(f"t{i}", seq=i, task_backend_specific_kwargs=declared)
        )
        clock.t += 1.0
    out = traced.dispatch()

    assert [d["uid"] for d in out] == ["t0", "t1"]
    first, second = rec.of("asyncflow.TaskDispatched")
    assert (first["task_id"], first["held_seconds"], first["order"]) == ("t0", 3.0, 0)
    assert (second["task_id"], second["held_seconds"], second["order"]) == (
        "t1",
        2.0,
        1,
    )
    assert first["key"] == {"priority": 5, "lineage": 0, "seq": 0}
    assert (first["waiting"], first["inflight"]) == (2, 1)
    assert (second["waiting"], second["inflight"]) == (1, 2)
    assert first["policy"] == "_Slots"


def test_release_reports_run_time_and_observed_usage_once():
    traced, rec, clock = _traced(_Slots(1))
    traced.enqueue("a", _desc("a"))
    traced.enqueue("b", _desc("b", seq=1))
    traced.dispatch()
    clock.t += 4.0
    assert traced.release("a", "DONE", {"host_mem": 2.5}) == "freed"  # passed back
    assert traced.release("a", "DONE") is None  # released already: no second event
    assert traced.release("never-seen", None) is None

    (released,) = rec.of("asyncflow.TaskReleased")
    assert released["task_id"] == "a"
    assert released["state"] == "DONE" and released["dispatched"] is True
    assert released["run_seconds"] == 4.0
    assert released["observed"] == {"host_mem": 2.5}
    assert (released["waiting"], released["inflight"]) == (1, 0)


def test_release_without_running_and_of_a_queued_task():
    traced, rec, clock = _traced(_Slots(1))
    traced.enqueue("a", _desc("a"))
    traced.enqueue("b", _desc("b", seq=1))
    traced.dispatch()
    clock.t += 2.0
    traced.release("b", None)  # cancelled while queued: it leaves at once
    traced.release("a", None)  # dispatched, never submitted

    b, a = rec.of("asyncflow.TaskReleased")
    assert (b["task_id"], b["dispatched"], b["state"]) == ("b", False, None)
    assert (b["held_seconds"], b["run_seconds"]) == (2.0, 0.0)
    assert (a["task_id"], a["dispatched"], a["state"]) == ("a", True, None)
    assert traced.inner.queue == deque() and not traced.inner.inflight


def test_rejected_task_is_reported_and_the_error_raised():
    class Picky(_Slots):
        def enqueue(self, uid, desc):
            raise ValueError("too big")

    traced, rec, _ = _traced(Picky(1))
    with pytest.raises(ValueError, match="too big"):
        traced.enqueue("a", _desc("a"))
    (rejected,) = rec.of("asyncflow.TaskRejected")
    assert rejected["error"] == "ValueError: too big"
    assert not traced.runnable()


def test_starving_task_is_reported_once_when_overtaken():
    class Newest(_Slots):
        """Dispatches the youngest task first: the oldest is overtaken."""

        def dispatch(self):
            out = []
            while self.queue and len(self.inflight) < self.capacity:
                desc = self.queue.pop()
                self.inflight.add(desc["uid"])
                out.append(desc)
            return out

    traced, rec, clock = _traced(Newest(1), starve_after=10.0)
    traced.enqueue("old", _desc("old"))
    for i in range(3):
        traced.enqueue(f"y{i}", _desc(f"y{i}", seq=i + 1))
        traced.dispatch()
        clock.t += 6.0
        traced.release(f"y{i}", "DONE")
    traced.dispatch()  # dispatches "old" now
    assert [e["task_id"] for e in rec.of("asyncflow.TaskDispatched")] == [
        "y0",
        "y1",
        "y2",
        "old",
    ]
    (starving,) = rec.of("asyncflow.TaskStarving")
    assert starving["task_id"] == "old"
    assert starving["overtaken"] == 3  # y0, y1, y2 by the check at 12 s
    assert starving["held_seconds"] == 12.0


def test_no_starving_without_overtaking_or_when_disabled():
    traced, rec, clock = _traced(_Slots(1), starve_after=1.0)
    traced.enqueue("a", _desc("a"))
    traced.enqueue("b", _desc("b", seq=1))
    traced.dispatch()
    clock.t += 100.0
    traced.dispatch()  # b waits long, but nothing passed it
    assert rec.of("asyncflow.TaskStarving") == []

    traced, rec, clock = _traced(_Slots(1), starve_after=None)
    traced.enqueue("a", _desc("a"))
    clock.t += 100.0
    traced.dispatch()
    assert rec.of("asyncflow.TaskStarving") == []


def test_blocked_and_unblocked_on_transitions_only():
    class Gated(_Slots):
        def __init__(self):
            super().__init__(4)
            self.reason = None

        def blocked(self):
            return self.reason

    policy = Gated()
    traced, rec, clock = _traced(policy)
    traced.dispatch()
    policy.reason = {"head": "big", "needs": {"cpus": 8}}
    traced.dispatch()
    traced.dispatch()  # still blocked on the same task: nothing new
    clock.t += 3.0
    policy.reason = None
    traced.dispatch()

    (blocked,) = rec.of("asyncflow.PolicyBlocked")
    assert blocked["task_id"] == "big"
    assert blocked["reason"]["needs"] == {"cpus": 8}
    (unblocked,) = rec.of("asyncflow.PolicyUnblocked")
    assert (unblocked["task_id"], unblocked["blocked_seconds"]) == ("big", 3.0)


def test_hooks_add_fields_and_attributes_reach_the_policy():
    class Explained(_Slots):
        def describe_dispatch(self, desc):
            return {"key": {"why": "oldest"}, "charge": {"slots": 1}}

        def occupancy(self):
            return {"slots": len(self.inflight)}

    traced, rec, _ = _traced(Explained(2))
    traced.enqueue("a", _desc("a"))
    traced.dispatch()
    (dispatched,) = rec.of("asyncflow.TaskDispatched")
    assert dispatched["key"] == {"why": "oldest"}
    assert dispatched["charge"] == {"slots": 1}
    assert dispatched["occupancy"] == {"slots": 1}
    assert traced.capacity == 2  # read from the wrapped policy
    assert traced.inflight == {"a"}


def test_layout_falls_back_to_capacity():
    traced, rec, _ = _traced(_Slots(7))
    traced.emit_layout()
    (layout,) = rec.of("asyncflow.PolicyLayout")
    assert layout["layout"] == {"capacity": 7}


def test_events_name_the_backend_and_scope_the_engine_stamped():
    traced, rec, _ = _traced(_Slots(1), default_backend="default")
    declared = {"priority": 3}
    traced.enqueue(
        "a",
        _desc(
            "a",
            _scope="replica-1",
            _backend="dragon",
            workflow_id="replica-1",
            task_backend_specific_kwargs=declared,
        ),
    )
    traced.enqueue("b", _desc("b", seq=1))
    traced.dispatch()
    traced.release("a", "DONE")
    traced.dispatch()

    a, b = rec.of("asyncflow.TaskDispatched")
    assert (a["backend"], a["workflow_id"]) == ("dragon", "replica-1")
    assert a["key"] == {"priority": 3, "lineage": 0, "seq": 0, "scope": "replica-1"}
    assert (b["backend"], b["key"]) == ("default", {"lineage": 1, "seq": 1})
    (released,) = rec.of("asyncflow.TaskReleased")
    assert released["backend"] == "dragon"


def test_the_proxy_hides_none_of_the_policy_attributes():
    policy = FifoPolicy(2)
    traced, _, _ = _traced(policy)
    for name in ("_queue", "_waiting", "_inflight", "_order", "capacity"):
        assert getattr(traced, name) is getattr(policy, name)
    assert repr(traced) == f"TracedPolicy({policy!r})"


@pytest.mark.no_engine_invariants  # the tracing errors are logged at ERROR
def test_tracing_errors_never_change_what_the_policy_decides(caplog):
    class Broken:
        def __call__(self, name, **fields):
            raise RuntimeError("telemetry is gone")

    class Explained(_Slots):
        def describe_dispatch(self, desc):
            raise KeyError(desc["uid"])

    class Picky(_Slots):
        def enqueue(self, uid, desc):
            raise ValueError("too big")

    caplog.set_level(logging.DEBUG, logger="radical.asyncflow.policy_telemetry")
    traced = TracedPolicy(Explained(1), Broken(), clock=_Clock())
    traced.emit_layout()
    traced.enqueue("a", _desc("a"))
    traced.enqueue("b", _desc("b", seq=1))
    assert [d["uid"] for d in traced.dispatch()] == ["a"]
    assert traced.release("a", "DONE") == "freed"
    assert [d["uid"] for d in traced.dispatch()] == ["b"]
    with pytest.raises(ValueError, match="too big"):  # the policy's own error
        TracedPolicy(Picky(1), Broken()).enqueue("c", _desc("c"))

    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 2  # the first failure of each proxy, with its traceback
    assert errors[0].exc_info is not None
    assert "the policy runs on" in errors[0].getMessage()


# ---------------------------------------------------------------------------
# Reading events back
# ---------------------------------------------------------------------------


def test_timeline_prefers_the_worker_side_times():
    events = [
        {"event_type": "TaskCreated", "task_id": "a", "event_time": 1.0},
        {"event_type": "asyncflow.TaskResolved", "task_id": "a", "event_time": 1.5},
        {
            "event_type": "asyncflow.TaskDispatched",
            "task_id": "a",
            "event_time": 2.0,
            "held_seconds": 0.5,
            "node_id": "n0",
            "policy": "FifoPolicy",
            "order": 0,
        },
        {"event_type": "TaskStarted", "task_id": "a", "event_time": 2.1},
        {
            "event_type": "asyncflow.TaskReleased",
            "task_id": "a",
            "event_time": 9.0,
            "run_seconds": 7.0,
            "observed": {"started": 4.0, "ended": 8.5, "host_mem": 1.0},
        },
        {"event_type": "TaskCompleted", "task_id": "a", "event_time": 9.1},
        {"event_type": "ResourceUpdate", "task_id": None, "event_time": 3.0},
    ]
    (row,) = timeline(events)
    assert (row["created"], row["ready"], row["dispatched"]) == (1.0, 1.5, 2.0)
    assert (row["started"], row["exec_start"], row["exec_end"]) == (2.1, 4.0, 8.5)
    assert (row["ended"], row["state"], row["node"]) == (9.1, "DONE", "n0")
    assert row["observed"]["host_mem"] == 1.0


def test_occupancy_is_a_step_function_keeping_the_last_point_per_time():
    events = [
        {
            "event_type": "asyncflow.TaskDispatched",
            "event_time": 1.0,
            "inflight": 1,
            "waiting": 1,
            "occupancy": {"pool": {"cpus": 2}},
        },
        {
            "event_type": "asyncflow.TaskDispatched",
            "event_time": 1.0,
            "inflight": 2,
            "waiting": 0,
            "occupancy": {"pool": {"cpus": 3}},
        },
        {
            "event_type": "asyncflow.TaskReleased",
            "event_time": 4.0,
            "inflight": 1,
            "waiting": 0,
            "occupancy": {"pool": {"cpus": 1}},
        },
    ]
    assert occupancy(events) == [
        {"t": 1.0, "inflight": 2, "waiting": 0, "pool.cpus": 3},
        {"t": 4.0, "inflight": 1, "waiting": 0, "pool.cpus": 1},
    ]


# ---------------------------------------------------------------------------
# Through the engine
# ---------------------------------------------------------------------------


class _Backend(NoopExecutionBackend):
    """Holds every task until the test finishes it."""

    def __init__(self):
        super().__init__()
        self.parked = {}

    async def submit_tasks(self, tasks):
        for t in tasks:
            self.parked[t["uid"]] = t

    def finish_oldest(self, observed=None):
        uid = next(iter(self.parked))
        task = self.parked.pop(uid)
        task["return_value"] = None
        if observed is not None:
            task["observed"] = observed
        self._callback_func(task, "RUNNING")
        self._callback_func(task, "DONE")


async def _until(cond, limit=5.0):
    deadline = asyncio.get_running_loop().time() + limit
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("the engine did not reach the expected state")
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_without_telemetry_the_engine_calls_the_bare_policy():
    policy = FifoPolicy(2)
    flow = await WorkflowEngine.create(backend=_Backend(), policy=policy)
    try:
        assert flow._policy is policy
    finally:
        await flow.shutdown()


@pytest.mark.asyncio
async def test_engine_emits_the_policy_timeline(tmp_path):
    pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")
    backend = _Backend()
    policy = FifoPolicy(1)
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    telemetry = await flow.start_telemetry(checkpoint_path=str(tmp_path))
    events = []
    telemetry.subscribe(events.append)
    assert isinstance(flow._policy, TracedPolicy) and flow._policy.inner is policy
    try:

        @flow.function_task
        async def step(x=None):
            return None

        first = step()
        second = step(first)  # ready only after the first ends
        third = step()
        futs = [first, second, third]
        for k in range(3):
            await _until(lambda: backend.parked)
            backend.finish_oldest(observed={"host_mem": 0.5 + k, "started": 10.0 + k})
        await asyncio.wait_for(asyncio.gather(*futs), 5.0)
        await telemetry.stop()
    finally:
        await flow.shutdown()

    uids = [f.uid for f in futs]
    names = [e.event_type for e in events]
    assert names.count("asyncflow.PolicyLayout") == 1
    assert names.count("asyncflow.TaskDispatched") == 3
    assert names.count("asyncflow.TaskReleased") == 3

    rows = {r["uid"]: r for r in timeline(events)}
    assert set(uids) <= set(rows)
    for uid in uids:
        r = rows[uid]
        assert r["policy"] == "FifoPolicy"
        moments = [r[c] for c in ("created", "queued", "dispatched", "submitted")]
        assert moments == sorted(moments)
        assert r["dispatched"] <= r["started"] <= r["ended"]
        assert r["state"] == "DONE" and r["release_state"] == "DONE"
        assert r["held_seconds"] >= 0 and r["run_seconds"] >= 0
    # one slot: each task waits in the policy for the one before it
    order = sorted(uids, key=lambda u: rows[u]["order"])
    assert order == [uids[0], uids[2], uids[1]]
    assert {rows[u]["exec_start"] for u in uids} == {10.0, 11.0, 12.0}
    assert all(p["inflight"] <= 1 for p in occupancy(events))

    # The checkpoint file holds the same events
    (path,) = tmp_path.glob("*.jsonl")
    from_file = {r["uid"]: r for r in timeline(load_events(str(path)))}
    for uid in uids:
        assert from_file[uid]["dispatched"] == rows[uid]["dispatched"]
        assert from_file[uid]["observed"] == rows[uid]["observed"]
    json.dumps(timeline(load_events(str(path))))  # plain data


@pytest.mark.asyncio
async def test_engine_releases_say_how_each_task_left_the_policy():
    """A task cancelled while held leaves at once; the others leave as the backend
    reports them, or as CANCELLED when the engine shuts down with them in flight."""
    pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")
    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=FifoPolicy(2))
    telemetry = await flow.start_telemetry()
    events = []
    telemetry.subscribe(events.append)

    @flow.function_task
    async def job(i):
        return i

    done, failed, held = job(0), job(1), job(2)
    await until(lambda: len(backend.parked) == 2)
    held.cancel()  # still in the policy: capacity 2
    backend.finish(done.uid)
    backend.finish(failed.uid, "FAILED")
    await asyncio.gather(done, failed, held, return_exceptions=True)
    running = job(3)
    await until(lambda: running.uid in backend.parked)
    await flow.shutdown()
    await telemetry.stop()

    released = {
        e.task_id: e for e in events if e.event_type == "asyncflow.TaskReleased"
    }
    assert {u: (e.state, e.dispatched) for u, e in released.items()} == {
        done.uid: ("DONE", True),
        failed.uid: ("FAILED", True),
        held.uid: (None, False),
        running.uid: ("CANCELLED", True),
    }
    rows = {r["uid"]: r for r in timeline(events)}
    assert rows[held.uid]["release_state"] is None
    assert "dispatched" not in rows[held.uid]


@pytest.mark.asyncio
async def test_a_traced_policy_is_free_for_another_engine_after_shutdown():
    pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")
    policy = FifoPolicy(1)
    flow = await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    telemetry = await flow.start_telemetry()
    await flow.shutdown()
    await telemetry.stop()

    second = await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    await second.shutdown()

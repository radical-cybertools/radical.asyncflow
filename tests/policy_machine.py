"""A Hypothesis state machine that drives a ready-queue policy on its own.

Random enqueues (priorities, workflows, lineages, resource demands), dispatches and
releases (by every end state, twice, of unknown and of queued tasks). It checks what
every policy must do:

- a task is dispatched once, and never after it was released while queued;
- when ``runnable()`` is False, ``dispatch()`` returns nothing;
- a policy with a ``capacity`` never has more in flight, and is runnable exactly when
  it has a task queued and a free slot;
- releasing what runs lets every queued task start, and leaves the policy holding
  nothing.

A subclass makes the policy (``make_policy``) and checks each dispatch against its own
order (``check_dispatch``); ``TestX = XMachine.TestCase`` runs it under the Hypothesis
profile of tests/conftest.py.
"""

import itertools

from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)

from tests.policy_harness import leftovers

demands = st.fixed_dictionaries(
    {},
    optional={
        "cpus": st.sampled_from([0.5, 1, 2, 3]),
        "gpus": st.sampled_from([0.25, 1 / 3, 0.5, 1, 2]),
        "host_mem": st.sampled_from([2, 8, 20]),
        "gpu_mem": st.sampled_from([4, 10, 20]),
    },
)
tasks = st.tuples(
    st.sampled_from([0, 0, 0, 1, -1, 2.5]),  # priority
    st.sampled_from([None, "A", "B"]),  # scope
    st.integers(0, 3),  # lineage
    st.sampled_from(["f", "g"]),  # name
    demands,
)


class Clock:
    now = 0.0

    def __call__(self):
        return self.now


class PolicyMachine(RuleBasedStateMachine):
    counted = True  # the policy has a capacity
    declares = False  # tasks declare their demand as task_description["resources"]

    def make_policy(self, capacity, data, clock):
        """The policy under test; ``data`` draws anything else it needs."""
        raise NotImplementedError

    def check_dispatch(self, out, queued, slots, first):
        """Checks a dispatch: ``queued`` before it, ``slots`` free, ``first`` the
        best-ranked queued task."""

    def check_idle(self):
        """Checks the policy once it holds nothing."""

    def rank(self, d):
        return (-d["task_backend_specific_kwargs"].get("priority", 0), d["_order"])

    @initialize(capacity=st.integers(1, 4), data=st.data())
    def setup(self, capacity, data):
        self.capacity, self.clock = capacity, Clock()
        self.p = self.make_policy(capacity, data, self.clock)
        self.uids = (f"task.{i:06d}" for i in itertools.count())
        self.order = itertools.count()
        self.queued, self.inflight, self.gone = {}, {}, set()

    @rule(batch=st.lists(tasks, min_size=1, max_size=6))
    def enqueue(self, batch):
        for prio, scope, lineage, name, demand in batch:
            uid, order = next(self.uids), next(self.order)
            kwargs = {"priority": prio}
            if self.declares:
                kwargs["resources"] = dict(demand)
            desc = {
                "uid": uid,
                "name": name,
                "_seq": order,
                "_order": order,
                "_lineage": min(lineage, order),
                "_scope": scope,
                "_backend": "x",
                "_declared": dict(demand),
                "task_backend_specific_kwargs": kwargs,
            }
            try:
                self.p.enqueue(uid, desc)
            except ValueError as e:
                assert self.declares and "can never fit" in str(e), e
                continue
            self.queued[uid] = desc

    @precondition(lambda self: self.queued)
    @rule()
    def dispatch(self):
        runnable = self.p.runnable()
        queued = dict(self.queued)
        first = min(queued.values(), key=self.rank)
        slots = self.capacity - len(self.inflight)
        out = self.p.dispatch()
        if not runnable:
            assert out == []
        for d in out:
            assert d["uid"] in self.queued, f"{d['uid']} dispatched, not queued"
            self.inflight[d["uid"]] = self.queued.pop(d["uid"])
        self.check_dispatch(out, queued, slots, first)

    @precondition(lambda self: self.inflight)
    @rule(data=st.data(), state=st.sampled_from(["DONE", "FAILED", "CANCELLED", None]))
    def release_inflight(self, data, state):
        uid = data.draw(st.sampled_from(sorted(self.inflight)))
        self.clock.now += data.draw(st.sampled_from([0.001, 0.5, 2.0]))
        self.p.release(uid, state)
        del self.inflight[uid]
        self.gone.add(uid)

    @precondition(lambda self: self.queued)
    @rule(data=st.data())
    def release_queued(self, data):
        uid = data.draw(st.sampled_from(sorted(self.queued)))
        self.p.release(uid, None)
        del self.queued[uid]
        self.gone.add(uid)

    @rule(data=st.data(), state=st.sampled_from(["DONE", None]))
    def release_again_or_unknown(self, data, state):
        self.p.release(data.draw(st.sampled_from([*sorted(self.gone), "nope"])), state)

    @invariant()
    def runnable_when_a_slot_and_a_task(self):
        if self.counted and hasattr(self, "p"):
            assert len(self.inflight) <= self.capacity
            free = len(self.inflight) < self.capacity
            assert self.p.runnable() == (bool(self.queued) and free)

    def teardown(self):
        if not hasattr(self, "p"):
            return
        for _ in range(4 * (len(self.queued) + len(self.inflight)) + 4):
            for d in self.p.dispatch():
                self.inflight[d["uid"]] = self.queued.pop(d["uid"])
            if not self.inflight:
                break
            uid = next(iter(self.inflight))
            self.p.release(uid, "DONE")
            del self.inflight[uid]
        assert not self.queued, f"never started: {sorted(self.queued)}"
        assert not self.inflight
        assert leftovers(self.p) == {}
        self.check_idle()

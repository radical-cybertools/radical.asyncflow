"""FifoPolicy on its own: capacity, order, priority, release and drop."""

import math

import pytest

from radical.asyncflow import FifoPolicy
from tests.policy_harness import desc, leftovers, uids


@pytest.mark.parametrize("capacity", [0, -1, 1.5, True, "2", None])
def test_capacity_must_be_a_positive_int(capacity):
    with pytest.raises(ValueError, match="capacity"):
        FifoPolicy(capacity)


def test_ready_order_within_capacity():
    p = FifoPolicy(2)
    for i in range(4):
        p.enqueue(f"t{i}", desc(f"t{i}", seq=10 - i))  # ready order, not _seq
    assert p.runnable()
    assert uids(p.dispatch()) == ["t0", "t1"]
    assert not p.runnable() and p.dispatch() == []
    p.release("t0", "DONE")
    assert p.runnable()
    assert uids(p.dispatch()) == ["t2"]


def test_priority_first_then_ready_order():
    p = FifoPolicy(1)
    p.enqueue("low", desc("low", priority=-1))
    p.enqueue("a", desc("a"))
    p.enqueue("high", desc("high", priority=5))
    p.enqueue("b", desc("b"))
    p.enqueue("half", desc("half", priority=0.5))
    order = []
    while p.runnable():
        (d,) = p.dispatch()
        order.append(d["uid"])
        p.release(d["uid"], "DONE")
    assert order == ["high", "half", "a", "b", "low"]


@pytest.mark.parametrize("value", [True, "1", None, math.nan, math.inf, [1]])
def test_priority_must_be_a_finite_number(value):
    p = FifoPolicy(1)
    with pytest.raises(ValueError, match="priority"):
        p.enqueue("t", desc("t", priority=value))
    assert not p.runnable() and leftovers(p) == {}


def test_release_of_unknown_or_released_task_is_ignored():
    p = FifoPolicy(1)
    p.release("nope", "DONE")
    p.enqueue("t0", desc("t0"))
    p.enqueue("t1", desc("t1"))
    p.dispatch()
    p.release("t0", "DONE")
    p.release("t0", "DONE")  # a second report of the same task
    assert uids(p.dispatch()) == ["t1"]
    p.release("t1", None)
    assert leftovers(p) == {}


def test_a_queued_task_released_is_dropped():
    """A task cancelled while queued never dispatches and holds nothing."""
    p = FifoPolicy(1)
    p.enqueue("t0", desc("t0"))
    p.enqueue("t1", desc("t1"))
    p.enqueue("t2", desc("t2"))
    p.release("t0", None)
    p.release("t1", None)
    assert uids(p.dispatch()) == ["t2"]
    assert not p.runnable()
    p.release("t2", "DONE")
    assert leftovers(p) == {}


def test_dropped_entries_do_not_pile_up():
    """Thousands of queued tasks cancelled behind a few live ones leave no garbage."""
    p = FifoPolicy(1)
    p.enqueue("live", desc("live", priority=1))
    for i in range(5000):
        p.enqueue(f"t{i}", desc(f"t{i}"))
        p.release(f"t{i}", None)
    assert len(p._queue) < 100
    assert uids(p.dispatch()) == ["live"]
    assert p._queue == []


def test_not_runnable_when_only_dropped_tasks_remain():
    p = FifoPolicy(4)
    p.enqueue("t0", desc("t0"))
    p.release("t0", None)
    assert not p.runnable()
    assert p.dispatch() == []


pytest.importorskip("hypothesis")

from tests.policy_machine import PolicyMachine  # noqa: E402


class FifoMachine(PolicyMachine):
    """FifoPolicy dispatches exactly the best-ranked queued tasks that fit its slots."""

    def make_policy(self, capacity, data, clock):
        return FifoPolicy(capacity)

    def check_dispatch(self, out, queued, slots, first):
        best = sorted(queued.values(), key=self.rank)[: max(slots, 0)]
        assert uids(out) == uids(best)


TestFifoMachine = FifoMachine.TestCase

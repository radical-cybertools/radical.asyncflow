"""FairPolicy: on its own, and sharing the backend across workflow scopes."""

import asyncio
import heapq
import itertools
import math
import random

import pytest

from radical.asyncflow import FairPolicy, WorkflowEngine
from tests.policy_harness import ParkingBackend, desc, leftovers, run_all, until


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def drain(p, clock, n, runtime=1.0):
    """Runs n tasks one at a time, each for ``runtime``; returns their workflows."""
    order = []
    for _ in range(n):
        (d,) = p.dispatch()
        clock.now += runtime
        p.release(d["uid"], "DONE")
        order.append(d["_scope"])
    return order


@pytest.mark.parametrize("capacity", [0, -1, 1.5, True, "2"])
def test_capacity_must_be_a_positive_int(capacity):
    with pytest.raises(ValueError, match="capacity"):
        FairPolicy(capacity)


@pytest.mark.parametrize("weight", [0, -1, math.inf, math.nan, True, "2"])
def test_weights_must_be_positive_numbers(weight):
    with pytest.raises(ValueError, match="weight"):
        FairPolicy(1, weights={"A": weight})


def test_equal_weights_alternate():
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    for i in range(4):
        p.enqueue(f"a{i}", desc(f"a{i}", i, workflow_id="A"))
    for i in range(4):
        p.enqueue(f"b{i}", desc(f"b{i}", 10 + i, workflow_id="B"))
    order = drain(p, clock, 8)
    assert order.count("A") == order.count("B") == 4
    assert all(x != y for x, y in zip(order, order[1:], strict=False))


def test_weights_share_service():
    clock = Clock()
    p = FairPolicy(1, weights={"A": 2.0}, clock=clock)
    for i in range(30):
        p.enqueue(f"a{i}", desc(f"a{i}", i, workflow_id="A"))
        p.enqueue(f"b{i}", desc(f"b{i}", 100 + i, workflow_id="B"))
    order = drain(p, clock, 30)
    assert 19 <= order.count("A") <= 21  # 2:1


def test_weight_key_matches_id_prefix():
    p = FairPolicy(1, weights={"sim": 3.0, "sim:a": 5.0})
    assert p._weight("sim:0042") == 3.0
    assert p._weight("sim:a:run1") == 5.0  # the longest matching key
    assert p._weight("simulation") == 1.0
    assert p._weight("other") == 1.0


def test_tasks_without_workflow_id_are_grouped_by_lineage():
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    for i in range(3):
        p.enqueue(f"x{i}", desc(f"x{i}", 10 + i, lineage=1))  # one workflow
    p.enqueue("y", desc("y", 20, lineage=2))  # another
    order = []
    for _ in range(4):
        (d,) = p.dispatch()
        clock.now += 1.0
        p.release(d["uid"], "DONE")
        order.append(d["uid"])
    assert order[:2] == ["x0", "y"]


def test_idle_group_banks_no_credit():
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    for i in range(5):
        p.enqueue(f"a{i}", desc(f"a{i}", i, workflow_id="A"))
    p.enqueue("c0", desc("c0", 9, workflow_id="C"))  # keeps min_vruntime moving
    drain(p, clock, 6)
    # B arrives late: it starts at min_vruntime, not at 0, so it does not run
    # alone for five slots to catch up with A
    for i in range(4):
        p.enqueue(f"a{5 + i}", desc(f"a{5 + i}", 5 + i, workflow_id="A"))
        p.enqueue(f"b{i}", desc(f"b{i}", 20 + i, workflow_id="B"))
    order = drain(p, clock, 8)
    assert order.count("A") == order.count("B") == 4
    assert "BBB" not in "".join(order)  # B does not run alone to catch up


def test_a_chain_does_not_starve_a_bag():
    """A workflow with one task at a time is idle between its steps; it keeps its debt,
    so a workflow with many tasks gets its share."""
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    for i in range(10):
        p.enqueue(f"b{i}", desc(f"b{i}", 100 + i, workflow_id="bag"))
    order = []
    for i in range(10):
        p.enqueue(f"a{i}", desc(f"a{i}", i, workflow_id="chain"))  # the next step
        (d,) = p.dispatch()
        clock.now += 1.0
        p.release(d["uid"], "DONE")
        order.append(d["_scope"])
        if d["_scope"] == "bag":
            # the step did not run: it is still queued, take it back
            p.release(f"a{i}", None)
    assert 4 <= order.count("chain") <= 6, order


def test_idle_debt_is_remembered_for_a_bounded_number_of_groups():
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    p.IDLE_GROUPS = 8
    for i in range(50):
        p.enqueue(f"t{i}", desc(f"t{i}", i, workflow_id=f"w{i}"))
    for _ in range(50):
        (d,) = p.dispatch()
        clock.now += 1.0
        p.release(d["uid"], "DONE")
    assert p._groups == {} and len(p._idle) <= 8


def test_priority_orders_tasks_within_a_workflow_only():
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    p.enqueue("a0", desc("a0", 0, workflow_id="A"))
    p.enqueue("a1", desc("a1", 1, workflow_id="A", priority=5))
    p.enqueue("b0", desc("b0", 2, workflow_id="B"))
    p.enqueue("b1", desc("b1", 3, workflow_id="B"))
    order = []
    for _ in range(4):
        (d,) = p.dispatch()
        clock.now += 1.0
        p.release(d["uid"], "DONE")
        order.append(d["uid"])
    assert order == ["a1", "b0", "a0", "b1"]  # A's urgent task first, shares kept


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_only_finished_tasks_teach_the_runtime(state):
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    p._est["f"] = 10.0
    p.enqueue("a0", desc("a0", 0, workflow_id="A"))
    p.enqueue("a1", desc("a1", 1, workflow_id="A"))
    before = p._groups["A"].vruntime
    p.dispatch()
    clock.now += 0.001  # ended at once
    p.release("a0", state)
    assert p._est["f"] == 10.0
    assert p._groups["A"].vruntime == pytest.approx(before + 0.001)  # charged as ran


def test_lineage_groups_do_not_collide_with_scopes():
    p = FairPolicy(2, weights={"lineage": 5.0})
    p.enqueue("x", desc("x", 5, lineage=5))  # no scope: grouped by lineage 5
    p.enqueue("y", desc("y", 6, workflow_id="lineage:5"))
    assert set(p._groups) == {("lineage", 5), "lineage:5"}
    assert p._groups[("lineage", 5)].weight == 1.0


def test_shares_are_of_time_not_of_tasks():
    """A's tasks run 3 s and B's 1 s: B runs about three tasks for each of A's."""
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    for i in range(40):
        p.enqueue(f"a{i}", desc(f"a{i}", i, name="slow", workflow_id="A"))
        p.enqueue(f"b{i}", desc(f"b{i}", 100 + i, name="fast", workflow_id="B"))
    order = []
    for _ in range(40):
        (d,) = p.dispatch()
        clock.now += 3.0 if d["_scope"] == "A" else 1.0
        p.release(d["uid"], "DONE")
        order.append(d["_scope"])
    a, b = order.count("A"), order.count("B")
    assert 2.5 <= b / a <= 3.5, (a, b)


def test_the_runtime_estimate_is_an_average_of_finished_runs():
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    for i, runtime in enumerate([1.0, 2.0, 2.0]):
        p.enqueue(f"t{i}", desc(f"t{i}", i, workflow_id="A"))
        p.dispatch()
        clock.now += runtime
        p.release(f"t{i}", "DONE")
    first = 1.0
    second = first + FairPolicy.ALPHA * (2.0 - first)
    third = second + FairPolicy.ALPHA * (2.0 - second)
    assert p._est["f"] == pytest.approx(third)


def test_two_queued_tasks_of_a_workflow_dropped():
    clock = Clock()
    p = FairPolicy(2, clock=clock)
    for i in range(3):
        p.enqueue(f"a{i}", desc(f"a{i}", i, workflow_id="A"))
    p.release("a0", None)
    p.release("a1", None)
    (d,) = p.dispatch()
    assert d["uid"] == "a2"
    p.release("a2", "DONE")
    assert leftovers(p) == {}


def test_an_early_end_gives_no_credit_below_min_vruntime():
    """A workflow whose long task ends early does not drop below where it stood once
    others have run past it."""
    clock = Clock()
    p = FairPolicy(8, clock=clock)
    p._est["slow"] = 100.0
    p.enqueue("g0", desc("g0", 0, name="slow", workflow_id="G"))
    p.dispatch()  # G is charged 100 s ahead
    order = itertools.count()
    for _ in range(20):  # for 20 s, B runs 1 s tasks on the other 7 slots
        for _ in range(7):
            i = next(order)
            p.enqueue(f"b{i}", desc(f"b{i}", 1 + i, workflow_id="B"))
        batch = p.dispatch()
        clock.now += 1.0
        for d in batch:
            p.release(d["uid"], "DONE")
    assert p.min_vruntime > 100  # B has run past G's charge
    p.enqueue("g1", desc("g1", 500, workflow_id="G"))
    p.release("g0", "DONE")  # ended after 20 s instead of 100
    assert p._groups["G"].vruntime >= p.min_vruntime


def test_a_long_task_in_flight_banks_no_credit_while_its_workflow_has_none_queued():
    """A's first, unseen task runs 2,000 s (charged 1 ms); B's bag runs beside it.

    When A gets a bag of its own at t=500 it shares the slots, not takes them.
    """
    clock = Clock()
    p = FairPolicy(4, clock=clock)
    p.enqueue("long", desc("long", 0, name="long", workflow_id="A"))
    for i in range(5000):
        p.enqueue(f"b{i}", desc(f"b{i}", 1 + i, name="short", workflow_id="B"))
    running = {d["uid"]: clock.now for d in p.dispatch()}
    while clock.now < 500:  # B's 1 s tasks run on the other 3 slots
        clock.now += 1.0
        for uid in [u for u in running if u != "long"]:
            del running[uid]
            p.release(uid, "DONE")
        running.update({d["uid"]: clock.now for d in p.dispatch()})
    for i in range(100):
        p.enqueue(f"a{i}", desc(f"a{i}", 10000 + i, name="short", workflow_id="A"))
    started = []
    for _ in range(20):
        clock.now += 1.0
        for uid in [u for u in running if u != "long"]:
            del running[uid]
            p.release(uid, "DONE")
        out = p.dispatch()
        running.update({d["uid"]: clock.now for d in out})
        started += [d["_scope"] for d in out]
    assert started.count("B") >= len(started) // 4, started


def test_new_work_while_tasks_run_is_not_charged_extra():
    """A has two 1 s tasks in flight and B one; a new task of A does not put A further
    ahead than the two tasks it was charged for."""
    clock = Clock()
    p = FairPolicy(3, clock=clock)
    p._est["f"] = 1.0
    p.enqueue("a0", desc("a0", 0, workflow_id="A"))
    p.enqueue("a1", desc("a1", 1, workflow_id="A"))
    for i in range(10):
        p.enqueue(f"b{i}", desc(f"b{i}", 10 + i, workflow_id="B"))
    assert sorted(d["uid"] for d in p.dispatch()) == ["a0", "a1", "b0"]
    assert p._groups["A"].vruntime == 2.0
    p.enqueue("a2", desc("a2", 2, workflow_id="A"))  # nothing queued, two in flight
    assert p._groups["A"].vruntime == 2.0


def test_a_workflow_streaming_steps_gets_its_share():
    """A: 4 chains, each step ready when the previous ends; B: a big bag; 0.5-1.5 s
    tasks, capacity 8. A asks for half the slots, and gets about half the time."""
    rng = random.Random(1)
    clock = Clock()
    p = FairPolicy(8, clock=clock)
    for i in range(20000):
        p.enqueue(f"b{i}", desc(f"b{i}", 10 + i, name="g", workflow_id="B"))
    for k in range(4):
        p.enqueue(f"a{k}.0", desc(f"a{k}.0", k, workflow_id="A"))
    events, busy, step, seq = [], {"A": 0.0, "B": 0.0}, [0] * 4, 10**6

    def start():
        for d in p.dispatch():
            rt = rng.uniform(0.5, 1.5)
            heapq.heappush(events, (clock.now + rt, d["uid"], d["_scope"], rt))

    start()
    while events and events[0][0] < 1000:
        clock.now, uid, scope, rt = heapq.heappop(events)
        busy[scope] += rt
        p.release(uid, "DONE")
        if scope == "A":
            k = int(uid[1:].split(".")[0])
            step[k] += 1
            seq += 1
            p.enqueue(f"a{k}.{step[k]}", desc(f"a{k}.{step[k]}", seq, workflow_id="A"))
        start()
    share = busy["A"] / (busy["A"] + busy["B"])
    assert share > 0.45, share


def test_a_long_task_is_not_charged_twice_when_new_work_arrives_during_it():
    """A's first, unseen task L runs 1,000 s beside B's bag; at t=500 A gets a bag too.

    From then on both are always backlogged, so they get equal service.
    """
    clock = Clock()
    p = FairPolicy(2, clock=clock)
    p._est["short"] = 1.0
    seq = itertools.count()
    p.enqueue("L", desc("L", next(seq), name="long", workflow_id="A"))
    for i in range(5000):
        p.enqueue(f"b{i}", desc(f"b{i}", next(seq), name="short", workflow_id="B"))
    events, served = [], {"A": 0.0, "B": 0.0}

    def start():
        for d in p.dispatch():
            rt = 1000.0 if d["uid"] == "L" else 1.0
            heapq.heappush(events, (clock.now + rt, d["uid"], d["_scope"], rt))

    start()
    arrived = False
    while events[0][0] <= 3000:
        if not arrived and events[0][0] > 500:
            clock.now, arrived = 500.0, True
            for i in range(5000):
                p.enqueue(
                    f"a{i}", desc(f"a{i}", next(seq), name="short", workflow_id="A")
                )
            start()
            continue
        clock.now, uid, scope, rt = heapq.heappop(events)
        served[scope] += rt
        p.release(uid, "DONE")
        start()
    assert abs(served["A"] - served["B"]) < 50, served


def test_task_that_never_ran_is_refunded():
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    p._est["f"] = 2.0
    p.enqueue("a0", desc("a0", 0, workflow_id="A"))
    p.enqueue("a1", desc("a1", 1, workflow_id="A"))  # keeps the group alive
    before = p._groups["A"].vruntime
    p.dispatch()
    clock.now += 5.0
    p.release("a0", None)
    assert p._groups["A"].vruntime == before
    assert p._est["f"] == 2.0  # the estimate is not touched


def test_a_queued_task_released_is_dropped():
    clock = Clock()
    p = FairPolicy(1, clock=clock)
    p.enqueue("a0", desc("a0", 0, workflow_id="A"))
    p.enqueue("a1", desc("a1", 1, workflow_id="A"))
    p.enqueue("b0", desc("b0", 2, workflow_id="B"))
    p.release("a0", None)
    p.release("b0", None)
    assert drain(p, clock, 1) == ["A"]
    assert not p.runnable() and leftovers(p) == {}


def test_groups_are_forgotten_when_idle():
    """A long run of independent tasks does not keep a group per lineage."""
    clock = Clock()
    p = FairPolicy(2, clock=clock)
    for i in range(100):
        p.enqueue(f"t{i}", desc(f"t{i}", i))
        for d in p.dispatch():
            clock.now += 0.1
            p.release(d["uid"], "DONE")
    for d in p.dispatch():
        p.release(d["uid"], "DONE")
    assert p._groups == {} and leftovers(p) == {}


async def test_a_scope_weight_reaches_the_members_of_its_blocks():
    backend = ParkingBackend()
    policy = FairPolicy(1, weights={"interactive": 4})
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    try:

        @flow.function_task
        async def job(name):
            return name

        @flow.block
        async def blk():
            return await job("member")

        gate = job("gate")
        await until(lambda: backend.parked)
        async with flow.workflow_scope("interactive"):
            block = blk()
        await until(lambda: policy._waiting)
        ((gid, _),) = policy._waiting.values()
        assert gid == "interactive" and policy._groups[gid].weight == 4
        await run_all(backend, [gate, block])
    finally:
        await flow.shutdown()


async def test_fair_share_across_workflow_scopes():
    clock = Clock()
    backend = ParkingBackend()
    finish = backend.finish

    def finish_after_one_second(*args, **kwargs):
        clock.now += 1.0  # every task runs for 1 s on the policy's clock
        return finish(*args, **kwargs)

    backend.finish = finish_after_one_second
    flow = await WorkflowEngine.create(
        backend=backend, policy=FairPolicy(1, clock=clock)
    )
    try:

        @flow.function_task
        async def job(name):
            return name

        gate = job("gate")  # holds the only slot while both workflows queue up
        await until(lambda: backend.parked)
        futs = [gate]
        async with flow.workflow_scope("A"):
            futs += [job(f"a{i}") for i in range(4)]
        async with flow.workflow_scope("B"):
            futs += [job(f"b{i}") for i in range(4)]
        await asyncio.sleep(0.01)
        await run_all(backend, futs)
        order = [arg[0] for _, arg in backend.submitted[1:]]
        assert sorted(order) == list("aaaabbbb")
        assert all(x != y for x, y in zip(order, order[1:], strict=False)), order
    finally:
        await flow.shutdown()


pytest.importorskip("hypothesis")

from hypothesis import strategies as st  # noqa: E402

from tests.policy_machine import PolicyMachine  # noqa: E402


class FairMachine(PolicyMachine):
    """FairPolicy fills its free slots, and keeps priority order within a workflow."""

    def make_policy(self, capacity, data, clock):
        weights = data.draw(st.sampled_from([None, {"A": 2.0}, {"B": 0.5, "A:x": 3}]))
        return FairPolicy(capacity, weights=weights, clock=clock)

    @staticmethod
    def group(d):
        return d["_scope"] or ("lineage", d["_lineage"])

    def check_dispatch(self, out, queued, slots, first):
        assert len(out) == min(len(queued), max(slots, 0))
        for d in out:  # nothing of its workflow ranked before it is left queued
            left = [x for x in self.queued.values() if self.group(x) == self.group(d)]
            assert all(self.rank(x) > self.rank(d) for x in left)


TestFairMachine = FairMachine.TestCase

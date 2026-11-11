"""LineagePolicy: on its own, and finishing started workflows in the engine."""

import asyncio

import pytest

from radical.asyncflow import FifoPolicy, LineagePolicy, WorkflowEngine
from tests.policy_harness import ParkingBackend, desc, leftovers, run_all, until


def drain(p):
    order = []
    while p.runnable():
        for d in p.dispatch():
            order.append(d["uid"])
            p.release(d["uid"], "DONE")
    return order


def test_oldest_lineage_first_then_registration_order():
    p = LineagePolicy(1)
    p.enqueue("new", desc("new", seq=5))
    p.enqueue("old_b", desc("old_b", seq=7, lineage=1))
    p.enqueue("old_a", desc("old_a", seq=6, lineage=1))
    assert drain(p) == ["old_a", "old_b", "new"]


def test_priority_before_lineage():
    p = LineagePolicy(1)
    p.enqueue("old", desc("old", seq=6, lineage=1))
    p.enqueue("urgent", desc("urgent", seq=9, priority=1))
    p.enqueue("older", desc("older", seq=7, lineage=0))
    assert drain(p) == ["urgent", "older", "old"]


def test_a_queued_task_released_is_dropped():
    p = LineagePolicy(2)
    p.enqueue("a", desc("a", seq=1))
    p.enqueue("b", desc("b", seq=2))
    p.release("a", None)
    assert drain(p) == ["b"]
    assert leftovers(p) == {}


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (FifoPolicy, ["a1", "b1", "a2"]),
        (LineagePolicy, ["a1", "a2", "b1"]),
    ],
)
async def test_lineage_finishes_started_workflows_first(policy, expected):
    """Step a2 depends on a1; b1 is registered after a1 but is ready before a2."""
    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=policy(1))
    try:

        @flow.function_task
        async def step(name, *deps):
            return name

        a1 = step("a1")
        await until(lambda: backend.parked)
        b1 = step("b1")
        a2 = step("a2", a1)
        await asyncio.sleep(0.01)  # b1 is held, a2 waits for a1
        await run_all(backend, [a1, b1, a2])
        assert [arg for _, arg in backend.submitted] == expected
    finally:
        await flow.shutdown()


async def test_block_members_inherit_the_block_lineage():
    """Members registered late inside an older block start before newer work."""
    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=LineagePolicy(1))
    try:

        @flow.function_task
        async def step(name):
            return name

        go = asyncio.Event()

        @flow.block
        async def blk():
            await go.wait()
            return await asyncio.gather(step("m1"), step("m2"))

        hog = step("hog")  # holds the only slot
        await until(lambda: backend.parked)
        block = blk()  # registered before the newer tasks, so an older lineage
        await until(lambda: block.uid in flow.components)
        newer = [step(f"n{i}") for i in range(2)]
        await asyncio.sleep(0.01)
        go.set()  # m1 and m2 register now, after n0 and n1
        await asyncio.sleep(0.01)
        await run_all(backend, [hog, block, *newer])
        order = [arg for _, arg in backend.submitted]
        assert order == ["hog", "m1", "m2", "n0", "n1"]
    finally:
        await flow.shutdown()


pytest.importorskip("hypothesis")

from tests.policy_harness import uids  # noqa: E402
from tests.policy_machine import PolicyMachine  # noqa: E402


class LineageMachine(PolicyMachine):
    """LineagePolicy dispatches exactly the best-ranked queued tasks: priority, then
    lineage, then registration order."""

    def make_policy(self, capacity, data, clock):
        return LineagePolicy(capacity)

    def rank(self, d):
        prio = -d["task_backend_specific_kwargs"].get("priority", 0)
        return (prio, d["_lineage"], d["_seq"])

    def check_dispatch(self, out, queued, slots, first):
        best = sorted(queued.values(), key=self.rank)[: max(slots, 0)]
        assert uids(out) == uids(best)


TestLineageMachine = LineageMachine.TestCase

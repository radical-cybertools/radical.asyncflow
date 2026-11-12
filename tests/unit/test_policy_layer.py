"""The engine with a ready-queue policy: what it holds, submits and releases."""

import asyncio

import pytest

from radical.asyncflow import FifoPolicy, WorkflowEngine
from radical.asyncflow.errors import DependencyFailureError
from tests.policy_harness import ParkingBackend, leftovers, run_all, until


class Recording(FifoPolicy):
    """A FifoPolicy that records the releases of its dispatched tasks."""

    def __init__(self, capacity=4):
        super().__init__(capacity)
        self.released = []

    def release(self, uid, state, observed=None):
        if uid in self._inflight:
            self.released.append((uid, state, observed))
        super().release(uid, state, observed)


@pytest.fixture
async def parked():
    """An engine with a FifoPolicy of capacity 1 on a ParkingBackend."""
    backend = ParkingBackend()
    policy = Recording(1)
    flow = await WorkflowEngine.create(backend=backend, policy=policy)

    @flow.function_task
    async def job(name, *deps):
        return name

    yield flow, backend, policy, job
    await flow.shutdown()


async def test_capacity_bounds_tasks_in_flight():
    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=FifoPolicy(3))
    try:

        @flow.function_task
        async def job(i):
            return i

        futs = [job(i) for i in range(20)]
        await until(lambda: len(backend.parked) == 3)
        await asyncio.sleep(0.05)
        assert len(backend.submitted) == 3  # the rest wait in the policy
        await run_all(backend, futs)
        assert backend.max_inflight == 3
        assert len(backend.submitted) == 20
    finally:
        await flow.shutdown()


async def test_higher_priority_starts_first(parked):
    flow, backend, policy, job = parked
    gate = job("gate")  # holds the only slot while the others queue up
    await until(lambda: backend.parked)
    low = job("low", task_description={"priority": -1})
    plain = job("plain")
    high = job("high", task_description={"priority": 9})
    await until(lambda: len(policy._waiting) == 3)
    await run_all(backend, [gate, low, plain, high])
    assert [arg for _, arg in backend.submitted] == ["gate", "high", "plain", "low"]


async def test_task_cancelled_while_held_leaves_the_policy(parked):
    """Its dependents are cancelled at once and the backend never sees it."""
    flow, backend, policy, job = parked
    first = job("first")
    await until(lambda: backend.parked)
    held = job("held")
    dependent = job("dependent", held)
    after = job("after")
    await until(lambda: len(policy._waiting) == 2)  # held and after
    assert held.cancel()
    assert held.uid not in policy._waiting  # dropped at once
    await asyncio.wait_for(asyncio.wait([dependent]), 1.0)
    assert held.cancelled() and dependent.cancelled()
    await run_all(backend, [first, after])
    assert [arg for _, arg in backend.submitted] == ["first", "after"]
    assert leftovers(policy) == {}


async def test_task_cancelled_during_submit_keeps_its_slot(parked):
    """The backend still runs a task cancelled while the engine awaits its submission,
    so its slot comes back only when the backend reports it."""
    flow, backend, policy, job = parked
    backend.gate = asyncio.Event()
    a = job("a")
    await until(lambda: policy._inflight)  # dispatched; submit_tasks waits
    assert a.cancel() and a.cancelled()
    b = job("b")
    backend.gate.set()
    await until(lambda: backend.parked and policy._waiting)
    await asyncio.sleep(0.02)
    assert [arg for _, arg in backend.submitted] == ["a"]  # b waits for a's slot
    backend.finish()  # a ends on the backend
    await run_all(backend, [b])
    assert backend.max_inflight == 1
    assert [uid for uid, _, _ in policy.released] == [a.uid, b.uid]


@pytest.mark.parametrize("state", ["DONE", "FAILED", "CANCELED"])
async def test_release_gets_what_the_backend_observed_once(parked, state):
    flow, backend, policy, job = parked
    fut = job("j")
    await until(lambda: backend.parked)
    task = next(iter(backend.parked.values()))
    task["observed"] = {"host_mem": 12.5}
    backend.finish(state=state)
    backend._callback_func(task, state)  # a second report of the same end
    await asyncio.wait([fut], timeout=1.0)
    assert fut.done()
    if state == "FAILED":
        assert isinstance(fut.exception(), RuntimeError)  # retrieved
    expected = {"DONE": "DONE", "FAILED": "FAILED", "CANCELED": "CANCELLED"}[state]
    assert policy.released == [(task["uid"], expected, {"host_mem": 12.5})]


async def test_task_resolved_outside_the_engine_while_held_never_runs(parked):
    """It is dropped when dispatched, and its dependents go on as without a policy."""
    flow, backend, policy, job = parked
    gate = job("gate")
    await until(lambda: backend.parked)
    held = job("held")
    dependent = job("dependent", held)
    await until(lambda: policy._waiting)
    held.set_result("set by hand")
    await run_all(backend, [gate, dependent])
    assert [arg for _, arg in backend.submitted] == ["gate", "dependent"]
    assert (held.uid, None, None) in policy.released
    assert leftovers(policy) == {}


async def test_a_task_the_policy_rejects_fails_with_its_dependents(parked):
    flow, backend, policy, job = parked
    bad = job("bad", task_description={"priority": "high"})
    dependent = job("dependent", bad)
    await asyncio.wait([bad, dependent], timeout=1.0)
    assert isinstance(bad.exception(), ValueError)
    assert "priority" in str(bad.exception())
    assert isinstance(dependent.exception(), DependencyFailureError)
    assert backend.submitted == [] and leftovers(policy) == {}


async def test_blocks_and_services_bypass_the_policy(parked):
    flow, backend, policy, job = parked

    @flow.function_task(service=True)
    async def svc():
        return None

    @flow.block
    async def blk():
        return "block ran"

    hog = job("hog")  # takes the only slot
    await until(lambda: backend.parked)
    assert await asyncio.wait_for(svc(), 1.0) == "ready"
    assert await asyncio.wait_for(blk(), 1.0) == "block ran"
    await run_all(backend, [hog])
    assert [uid for uid, _, _ in policy.released] == [hog.uid]


async def test_registration_order_and_lineage(parked):
    flow, backend, policy, job = parked

    @flow.block
    async def blk():
        return await job("member")

    a = job("a")
    b = job("b", a)
    c = job("c")
    block = blk()
    await until(lambda: backend.parked)

    def stamps(fut):
        d = flow.components[fut.uid]["description"]
        return d["_seq"], d["_lineage"]

    assert flow.components[a.uid]["description"]["_backend"] == backend.name
    (sa, la), (sb, lb), (sc, lc) = stamps(a), stamps(b), stamps(c)
    assert sa < sb < sc and la == sa
    assert lb == la  # a dependency's lineage
    assert lc == sc  # a new lineage
    await until(lambda: len(flow.components) == 5)
    (member,) = flow._block_members[block.uid]
    member_desc = flow.components[member]["description"]
    assert member_desc["_lineage"] == stamps(block)[1]  # the block's lineage
    assert member_desc["workflow_id"] == block.uid
    assert member_desc["_scope"] is None  # the block was called outside any scope
    await run_all(backend, [a, b, c, block])


async def test_block_members_share_the_scope_the_block_was_called_in(parked):
    flow, backend, policy, job = parked

    @flow.block
    async def blk():
        return await job("member")

    async with flow.workflow_scope("interactive"):
        block = blk()
        direct = job("direct")
    await until(lambda: block.uid in flow._block_members)
    (member,) = flow._block_members[block.uid]
    for uid in (direct.uid, member):
        assert flow.components[uid]["description"]["_scope"] == "interactive"
    await run_all(backend, [block, direct])


async def test_a_task_routed_to_another_backend_names_it():
    default, other = ParkingBackend(), ParkingBackend()
    other.name = "other"
    flow = await WorkflowEngine.create(backend=[default, other], policy=FifoPolicy(4))
    try:

        @flow.function_task(backend="other")
        async def remote():
            return None

        @flow.function_task
        async def local():
            return None

        r, loc = remote(), local()
        await until(lambda: other.parked and default.parked)
        stamped = {
            f.uid: flow.components[f.uid]["description"]["_backend"] for f in (r, loc)
        }
        assert stamped == {r.uid: "other", loc.uid: default.name}
        await run_all(other, [r])
        await run_all(default, [loc])
    finally:
        await flow.shutdown()


async def test_without_a_policy_nothing_is_stamped():
    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend)
    try:

        @flow.function_task
        async def job():
            return None

        fut = job()
        await until(lambda: backend.parked)
        assert "_seq" not in flow.components[fut.uid]["description"]
        await run_all(backend, [fut])
    finally:
        await flow.shutdown()


async def test_custom_policy_object():
    class Lifo:
        def __init__(self):
            self.stack, self.inflight = [], set()

        def enqueue(self, uid, desc):
            self.stack.append(desc)

        def dispatch(self):
            if self.inflight or not self.stack:
                return []
            desc = self.stack.pop()
            self.inflight.add(desc["uid"])
            return [desc]

        def release(self, uid, state, observed=None):
            self.inflight.discard(uid)

        def runnable(self):
            return bool(self.stack) and not self.inflight

    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=Lifo())
    try:

        @flow.function_task
        async def job(i):
            return i

        first = job(0)
        await until(lambda: backend.parked)
        rest = [job(i) for i in range(1, 4)]
        await asyncio.sleep(0.01)
        await run_all(backend, [first, *rest])
        assert [arg for _, arg in backend.submitted] == [0, 3, 2, 1]
    finally:
        await flow.shutdown()


async def test_object_without_policy_operations_is_rejected():
    class NoRelease:
        def enqueue(self, uid, desc): ...
        def dispatch(self): ...
        def runnable(self): ...

    backend = ParkingBackend()
    with pytest.raises(TypeError, match=r"lacks the operations \['release'\]"):
        await WorkflowEngine.create(backend=backend, policy=NoRelease())
    assert backend.attached_to == []  # rejected before the backend is touched


async def test_a_policy_serves_one_running_engine():
    policy = FifoPolicy(2)
    first = await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    with pytest.raises(ValueError, match="give each engine its own"):
        await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    await first.shutdown()
    second = await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    await second.shutdown()


async def test_shutdown_with_tasks_held():
    backend = ParkingBackend()
    policy = Recording(1)
    flow = await WorkflowEngine.create(backend=backend, policy=policy)

    @flow.function_task
    async def job(i):
        return i

    futs = [job(i) for i in range(5)]
    await until(lambda: backend.parked)
    await asyncio.wait_for(flow.shutdown(), 5.0)
    assert len(backend.submitted) == 1
    assert all(f.cancelled() for f in futs)
    assert leftovers(policy) == {}  # the running task's slot too
    assert policy.released == [(futs[0].uid, "CANCELLED", None)]

    # so the policy can serve another engine, with its full capacity
    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    try:

        @flow.function_task
        async def again():
            return None

        fut = again()
        await until(lambda: backend.parked)
        await run_all(backend, [fut])
    finally:
        await flow.shutdown()


async def test_after_shutdown_without_the_backend_a_policy_waits_for_its_reports():
    """With skip_execution_backend the backend still runs the engine's tasks, so their
    slots stay taken until it reports them."""
    backend = ParkingBackend()
    policy = FifoPolicy(1)
    flow = await WorkflowEngine.create(backend=backend, policy=policy)

    @flow.function_task
    async def job():
        return None

    job()
    await until(lambda: backend.parked)
    await flow.shutdown(skip_execution_backend=True)
    assert policy._inflight  # the backend runs it on
    with pytest.raises(ValueError, match="give each engine its own"):
        await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    backend.finish()  # the backend reports it to the engine that sent it
    assert leftovers(policy) == {}
    second = await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    await second.shutdown()
    await backend.shutdown()


@pytest.mark.no_engine_invariants  # the first engine is left without shutdown
def test_a_policy_whose_engine_loop_closed_is_free_again():
    policy = FifoPolicy(1)

    async def abandon():
        backend = ParkingBackend()
        flow = await WorkflowEngine.create(backend=backend, policy=policy)

        @flow.function_task
        async def job():
            return None

        job()
        job()
        await until(lambda: backend.parked)

    asyncio.run(abandon())  # no shutdown: the loop closes with a task in flight
    assert leftovers(policy)

    async def again():
        backend = ParkingBackend()
        flow = await WorkflowEngine.create(backend=backend, policy=policy)
        assert leftovers(policy) == {}  # the closed engine's tasks are gone

        @flow.function_task
        async def job():
            return None

        fut = job()
        await until(lambda: backend.parked)
        await run_all(backend, [fut])
        await flow.shutdown()

    asyncio.run(again())


@pytest.mark.no_engine_invariants  # the failing policy is logged at ERROR
async def test_a_policy_that_fails_to_release_does_not_stop_the_engine(caplog):
    class Failing(FifoPolicy):
        def release(self, uid, state, observed=None):
            super().release(uid, state, observed)
            raise RuntimeError("policy bug")

    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=Failing(2))
    try:

        @flow.function_task
        async def job(*deps):
            return None

        first = job()
        second = job()
        dependent = job(second)
        await until(lambda: len(backend.parked) == 2)
        assert second.cancel()  # the backend reports it; its release raises
        await asyncio.wait_for(asyncio.wait([dependent]), 1.0)
        assert dependent.cancelled()
        backend.finish()  # first's release raises too
        assert await asyncio.wait_for(first, 1.0) is None
        assert sum("failed to release" in r.message for r in caplog.records) == 2
    finally:
        await flow.shutdown()


async def test_a_policy_that_dispatches_nothing_does_not_spin_the_loop():
    class Stuck(FifoPolicy):
        calls = 0

        def dispatch(self):
            Stuck.calls += 1
            return []

        def runnable(self):
            return True  # says it could start a task, and never does

    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=Stuck(1))
    try:

        @flow.function_task
        async def job():
            return None

        fut = job()
        await asyncio.sleep(0.1)
        assert Stuck.calls < 5  # once per change, not once per loop iteration
        fut.cancel()
    finally:
        await flow.shutdown()


async def test_skipping_the_backend_while_it_takes_a_task_frees_the_policy():
    """A task the backend never took is not waited for."""
    backend = ParkingBackend()
    backend.gate = asyncio.Event()
    policy = FifoPolicy(1)
    flow = await WorkflowEngine.create(backend=backend, policy=policy)

    @flow.function_task
    async def job():
        return None

    job()
    await until(lambda: policy._inflight)  # submit_tasks waits at the gate
    await flow.shutdown(skip_execution_backend=True)
    assert leftovers(policy) == {}
    second = await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    await second.shutdown()


@pytest.mark.no_engine_invariants  # the backend's shutdown fails
async def test_a_backend_that_fails_to_shut_down_still_frees_the_policy():
    class Failing(ParkingBackend):
        async def shutdown(self):
            raise RuntimeError("cannot stop")

    policy = FifoPolicy(1)
    flow = await WorkflowEngine.create(backend=Failing(), policy=policy)

    @flow.function_task
    async def job():
        return None

    job()
    await until(lambda: flow.backend.parked)
    with pytest.raises(RuntimeError, match="cannot stop"):
        await flow.shutdown()
    assert leftovers(policy) == {}
    second = await WorkflowEngine.create(backend=ParkingBackend(), policy=policy)
    await second.shutdown()


async def test_the_engine_does_not_keep_an_engine_whose_tasks_were_reported():
    import radical.asyncflow.workflow_manager as wm

    backend = ParkingBackend()
    policy = FifoPolicy(1)
    flow = await WorkflowEngine.create(backend=backend, policy=policy)

    @flow.function_task
    async def job():
        return None

    job()
    await until(lambda: backend.parked)
    await flow.shutdown(skip_execution_backend=True)
    assert wm._POLICY_ENGINES.get(id(policy)) is flow  # the backend still runs it
    backend.finish()
    assert wm._POLICY_ENGINES.get(id(policy)) is None
    await backend.shutdown()


async def test_a_policy_that_dispatches_one_task_at_a_time_fills_its_slots():
    class OneByOne(FifoPolicy):
        def dispatch(self):  # one task per call, while runnable() stays True
            capacity = self.capacity
            self.capacity = min(capacity, len(self._inflight) + 1)
            try:
                return super().dispatch()
            finally:
                self.capacity = capacity

    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=OneByOne(3))
    try:

        @flow.function_task
        async def job(i):
            return i

        futs = [job(i) for i in range(3)]
        await until(lambda: len(backend.parked) == 3)
        await run_all(backend, futs)
    finally:
        await flow.shutdown()


async def test_a_task_dispatched_twice_is_submitted_once(caplog):
    class Twice(FifoPolicy):
        def dispatch(self):
            out = super().dispatch()
            return out + out

    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=Twice(2))
    try:

        @flow.function_task
        async def job():
            return None

        fut = job()
        await until(lambda: backend.parked)
        await asyncio.sleep(0.01)
        assert len(backend.submitted) == 1
        assert any("dispatched" in r.message for r in caplog.records)
        await run_all(backend, [fut])
    finally:
        await flow.shutdown()


async def test_a_policy_that_returns_unknown_tasks_does_not_spin_the_loop():
    class Ghost(FifoPolicy):
        calls = 0

        def dispatch(self):
            Ghost.calls += 1
            return [{"uid": "task.ghost"}]

        def runnable(self):
            return True

    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=Ghost(1))
    try:

        @flow.function_task
        async def job():
            return None

        fut = job()
        await asyncio.sleep(0.1)
        assert Ghost.calls < 5
        fut.cancel()
    finally:
        await flow.shutdown()


@pytest.mark.no_engine_invariants  # the failing policy is logged at ERROR
async def test_a_policy_that_fails_to_say_if_it_is_runnable_leaves_the_engine_idle():
    class Failing(FifoPolicy):
        def runnable(self):
            raise RuntimeError("policy bug")

    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=Failing(1))
    try:

        @flow.function_task
        async def job():
            return None

        fut = job()
        await until(lambda: backend.parked)
        assert not flow._shutdown_event.is_set()
        backend.finish()
        assert await asyncio.wait_for(fut, 1.0) is None
    finally:
        await flow.shutdown()


async def test_a_held_task_resolved_by_hand_frees_its_place_for_the_next(parked):
    """Without dependents to wake the engine, the next task still starts."""
    flow, backend, policy, job = parked
    gate = job("gate")
    await until(lambda: backend.parked)
    held = job("held")
    after = job("after")
    await until(lambda: len(policy._waiting) == 2)
    held.set_result("by hand")
    backend.finish()  # gate ends: held is dispatched and dropped; after starts
    await until(lambda: backend.parked)
    await run_all(backend, [gate, after])
    assert [arg for _, arg in backend.submitted] == ["gate", "after"]


async def test_a_task_in_flight_dispatched_again_keeps_its_slot(caplog):
    class Again(FifoPolicy):
        """Returns its last task once more on the next call (a bug)."""

        last = None

        def dispatch(self):
            out = super().dispatch()
            if not out and self.last is not None:
                out, self.last = [self.last], None
            elif out:
                self.last = out[-1]
            return out

    backend = ParkingBackend()
    policy = Again(1)
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    try:

        @flow.function_task
        async def job(i):
            return i

        a = job(0)
        await until(lambda: backend.parked)
        a.set_result("by hand")  # still on the backend
        b = job(1)  # dispatch() returns a again
        await until(lambda: any("again" in r.message for r in caplog.records))
        assert a.uid in policy._inflight and backend.max_inflight == 1
        backend.finish()  # a ends on the backend: b starts
        await run_all(backend, [b])
        assert backend.max_inflight == 1
    finally:
        await flow.shutdown()


@pytest.mark.no_engine_invariants  # the abandoned engine is never shut down
async def test_an_engine_collected_without_shutdown_gives_its_tasks_back():
    import gc

    policy = FifoPolicy(1)

    async def abandon():
        backend = ParkingBackend()
        flow = await WorkflowEngine.create(backend=backend, policy=policy)

        @flow.function_task
        async def job():
            return None

        job()
        await until(lambda: backend.parked)
        flow._run_task.cancel()  # its loop goes on; the engine is dropped
        await asyncio.sleep(0)

    await abandon()
    other = await WorkflowEngine.create(backend=ParkingBackend())  # takes the signals
    for _ in range(3):
        gc.collect()
    import radical.asyncflow.workflow_manager as wm

    assert wm._POLICY_ENGINES.get(id(policy)) is None  # collected
    assert policy._inflight  # still holds the collected engine's task
    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    try:
        assert leftovers(policy) == {}

        @flow.function_task
        async def job():
            return None

        fut = job()
        await until(lambda: backend.parked)
        await run_all(backend, [fut])
    finally:
        await flow.shutdown()
        await other.shutdown()


@pytest.mark.no_engine_invariants  # the engines are never shut down
def test_policies_of_engines_collected_without_shutdown_are_collected_too():
    import gc
    import weakref

    import radical.asyncflow.workflow_manager as wm

    policies = [FifoPolicy(1) for _ in range(5)]  # outlive their engines
    refs = [weakref.ref(p) for p in policies]

    async def run(policy):
        backend = ParkingBackend()
        flow = await WorkflowEngine.create(backend=backend, policy=policy)

        @flow.function_task
        async def job():
            return None

        job()
        await until(lambda: backend.parked)
        raise RuntimeError("the application fails before shutdown")

    for policy in policies:
        with pytest.raises(RuntimeError):
            asyncio.run(run(policy))
    gc.collect()
    assert all(id(p) in wm._ABANDONED for p in policies)  # their tasks are noted
    del policies, policy
    gc.collect()
    assert all(ref() is None for ref in refs)  # and the policies are not kept


@pytest.mark.no_engine_invariants  # the failing policy is logged at ERROR
def test_a_policy_failing_to_take_back_an_abandoned_task_does_not_stop_the_next():
    class Failing(FifoPolicy):
        fail = False

        def release(self, uid, state, observed=None):
            super().release(uid, state, observed)
            if self.fail:
                raise RuntimeError("policy bug")

    policy = Failing(1)

    async def abandon():
        backend = ParkingBackend()
        flow = await WorkflowEngine.create(backend=backend, policy=policy)

        @flow.function_task
        async def job():
            return None

        job()
        await until(lambda: backend.parked)

    asyncio.run(abandon())  # the loop closes; the engine is left without shutdown
    import gc

    gc.collect()  # the engine is collected: its tasks are noted for the policy
    policy.fail = True

    async def again():
        backend = ParkingBackend()
        flow = await WorkflowEngine.create(backend=backend, policy=policy)
        policy.fail = False
        assert leftovers(policy) == {}

        @flow.function_task
        async def job():
            return None

        fut = job()
        await until(lambda: backend.parked)
        await run_all(backend, [fut])
        await flow.shutdown()

    asyncio.run(again())


async def test_a_policy_that_cannot_be_weakly_referenced_works():
    class Slotted:
        __slots__ = ("inner",)

        def __init__(self):
            self.inner = FifoPolicy(1)

        def enqueue(self, uid, desc):
            self.inner.enqueue(uid, desc)

        def dispatch(self):
            return self.inner.dispatch()

        def release(self, uid, state, observed=None):
            self.inner.release(uid, state, observed)

        def runnable(self):
            return self.inner.runnable()

    backend = ParkingBackend()
    flow = await WorkflowEngine.create(backend=backend, policy=Slotted())
    try:

        @flow.function_task
        async def job():
            return None

        fut = job()
        await until(lambda: backend.parked)
        await run_all(backend, [fut])
    finally:
        await flow.shutdown()


@pytest.mark.no_engine_invariants  # the engines are never shut down
def test_each_policy_keeps_one_finalizer_for_its_abandoned_tasks():
    import gc
    import weakref

    policy = FifoPolicy(1)

    async def abandon():
        backend = ParkingBackend()
        flow = await WorkflowEngine.create(backend=backend, policy=policy)

        @flow.function_task
        async def job():
            return None

        job()
        await until(lambda: backend.parked)

    for _ in range(5):
        asyncio.run(abandon())
        gc.collect()
    finalizers = [
        f for f in weakref.finalize._registry if f.peek() and f.peek()[0] is policy
    ]
    assert len(finalizers) <= 1

"""ResourcePolicy: admission by demand, on its own and driving the engine."""

import asyncio
import itertools
import math

import pytest

from radical.asyncflow import (
    MemoryModel,
    NodeShape,
    Pool,
    ResourcePolicy,
    WorkflowEngine,
)
from radical.asyncflow.errors import DependencyFailureError
from tests.policy_harness import ParkingBackend, leftovers, until

_uid = itertools.count()


def task(name="f", backend=None, priority=None, **resources):
    uid = f"task.{next(_uid):06d}"
    kwargs = {"resources": resources} if resources else {}
    if priority is not None:
        kwargs["priority"] = priority
    return {
        "uid": uid,
        "name": name,
        "_backend": backend,
        "task_backend_specific_kwargs": kwargs,
    }


def enqueue(policy, *descs):
    for d in descs:
        policy.enqueue(d["uid"], d)
    return descs


def uids(descs):
    return [d["uid"] for d in descs]


def is_idle(policy):
    """Nothing held, and every pool's ledger back to its totals."""
    pools = [policy._single] if policy._single else list(policy.pools.values())
    for pool in pools:
        assert (pool.cpu, pool.host, pool.slices, pool.gpu_mem) == pytest.approx(
            pool._total
        )
        assert pool.unknown == 0
    return leftovers(policy) == {}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cpus": -1},
        {"cpus": math.nan},
        {"cpus": math.inf},
        {"cpus": 4, "gpus": 1.5},
        {"cpus": 4, "gpus": True},
        {"cpus": 4, "host_mem": -2},
        {"cpus": 4, "gpu_mem": math.nan},
        {"cpus": 4, "cpu_overcommit": 0},
    ],
)
def test_node_shape_rejects_bad_values(kwargs):
    with pytest.raises(ValueError):
        NodeShape(**kwargs)


@pytest.mark.parametrize(
    "build",
    [
        lambda: Pool([NodeShape(4)], mode="spread"),
        lambda: Pool([]),
        lambda: Pool([NodeShape(4)], unknown_per_node=0),
        lambda: Pool([NodeShape(4)], unknown_per_node=True),
        lambda: ResourcePolicy([NodeShape(4)]),
        lambda: ResourcePolicy(Pool(NodeShape(4)), memory=object()),
        lambda: MemoryModel(headroom=0.5),
        lambda: MemoryModel(headroom=math.nan),
        lambda: MemoryModel(min_samples=0),
    ],
)
def test_bad_configuration_is_rejected(build):
    with pytest.raises(ValueError):
        build()


def test_one_pool_for_two_backends_is_rejected():
    pool = Pool(NodeShape(4))
    with pytest.raises(ValueError, match="Pool of its own"):
        ResourcePolicy({"a": pool, "b": pool})


@pytest.mark.parametrize(
    "resources",
    [{"cpu": 1}, {"gpus": 1.5}, {"host_mem": -1}, {"cpus": math.nan}],
)
def test_bad_demand_is_rejected(resources):
    p = ResourcePolicy(Pool([NodeShape(8, 2, 64, 40)]))
    with pytest.raises(ValueError):
        enqueue(p, task(**resources))
    assert is_idle(p)


def test_bad_priority_is_rejected():
    p = ResourcePolicy(Pool([NodeShape(8)]))
    with pytest.raises(ValueError, match="priority"):
        enqueue(p, task(priority="urgent"))
    assert is_idle(p)


@pytest.mark.parametrize(
    ("mode", "resources"),
    [
        ("node", {"cpus": 5}),  # every node has 4
        ("node", {"gpus": 2}),  # every node has 1 GPU
        ("node", {"host_mem": 40}),  # every node has 32 GiB
        ("aggregate", {"cpus": 9}),  # the pool has 8
        ("aggregate", {"gpus": 3}),  # the pool has 2 GPUs
    ],
)
def test_demand_that_can_never_fit_is_rejected(mode, resources):
    p = ResourcePolicy(Pool([NodeShape(4, 1, 32, 16)] * 2, mode=mode))
    with pytest.raises(ValueError, match="can never fit"):
        enqueue(p, task(**resources))


# ---------------------------------------------------------------------------
# Admission
# ---------------------------------------------------------------------------


def test_aggregate_mode_admits_against_the_summed_capacity():
    p = ResourcePolicy(Pool([NodeShape(4)] * 2, mode="aggregate"))
    a, b, c = enqueue(p, *(task(cpus=3) for _ in range(3)))
    assert uids(p.dispatch()) == [a["uid"], b["uid"]]  # 6 of 8 cores
    assert "placement" not in a
    assert p.dispatch() == [] and not p.runnable()
    p.release(a["uid"], "DONE")
    assert p.runnable()
    assert uids(p.dispatch()) == [c["uid"]]


def test_node_mode_places_each_task_on_a_node_it_fits():
    shapes = [NodeShape(4, name="n0"), NodeShape(4, name="n1")]
    p = ResourcePolicy(Pool(shapes, mode="node"))
    a, b, c = enqueue(p, *(task(cpus=3) for _ in range(3)))
    assert uids(p.dispatch()) == [a["uid"], b["uid"]]
    assert {a["placement"]["host"], b["placement"]["host"]} == {"n0", "n1"}
    assert a["placement"]["gpus"] == []
    assert p.dispatch() == []  # one core left on each node: c fits neither
    p.release(b["uid"], "DONE")
    (d,) = p.dispatch()
    assert d is c and c["placement"]["node"] == b["placement"]["node"]


def test_node_mode_shares_gpus_by_slices_and_memory():
    p = ResourcePolicy(Pool([NodeShape(64, 2, 512, 40)], mode="node"))
    tasks = enqueue(p, *(task(gpus=0.25, gpu_mem=9) for _ in range(9)))
    admitted = p.dispatch()
    # by slices 4 per GPU fit, by memory 4 (36 of 40 GiB): 8 on 2 GPUs
    assert uids(admitted) == uids(tasks[:8])
    per_gpu = [sum(t["placement"]["gpus"] == [g] for t in admitted) for g in (0, 1)]
    assert per_gpu == [4, 4]
    assert p.dispatch() == []


def test_whole_gpus_take_whole_devices():
    p = ResourcePolicy(Pool([NodeShape(8, 4, 64, 40)], mode="node"))
    a, b, c = enqueue(p, task(gpus=2), task(gpus=0.5), task(gpus=2))
    assert uids(p.dispatch()) == [a["uid"], b["uid"]]
    assert len(set(a["placement"]["gpus"]) | set(b["placement"]["gpus"])) == 3
    assert p.dispatch() == []  # one free device left: c needs two
    p.release(b["uid"], "DONE")
    assert uids(p.dispatch()) == [c["uid"]]


def test_aggregate_mode_charges_gpu_memory_on_every_device():
    p = ResourcePolicy(Pool([NodeShape(8, 4, gpu_mem=40)], mode="aggregate"))
    two, *shares = enqueue(
        p, task(gpus=2, gpu_mem=40), *(task(gpus=0.25, gpu_mem=40) for _ in range(3))
    )
    # 160 GiB: 80 for the two devices, 40 for each of two shares; the third waits
    assert uids(p.dispatch()) == uids([two, *shares[:2]])
    p.release(two["uid"], "DONE")
    assert uids(p.dispatch()) == [shares[2]["uid"]]


def test_the_demand_is_taken_out_of_what_the_backend_reads():
    """A backend that has a setting of the same name (Dask) does not read it."""
    pooled = ResourcePolicy({"a": Pool(NodeShape(4))})
    a, other = enqueue(pooled, task(backend="a", cpus=2), task(backend="dask", GPU=1))
    assert "resources" not in a["task_backend_specific_kwargs"]
    assert other["task_backend_specific_kwargs"]["resources"] == {"GPU": 1}
    assert uids(pooled.dispatch()) == [other["uid"], a["uid"]]


def test_node_mode_needs_distinct_host_names():
    with pytest.raises(ValueError, match="names must differ"):
        Pool([NodeShape(4, name="gpu001")] * 2, mode="node")
    Pool([NodeShape(4, name="gpu001")] * 2, mode="aggregate")  # names unused


def test_host_memory_bounds_admission():
    p = ResourcePolicy(Pool([NodeShape(64, host_mem=64)], mode="node"))
    tasks = enqueue(p, *(task(host_mem=20) for _ in range(4)))
    assert uids(p.dispatch()) == uids(tasks[:3])


def test_tasks_of_one_kind_with_different_memory_are_tried_apart():
    """A task that declares less memory is not stuck behind a bigger one of its kind."""
    p = ResourcePolicy(Pool([NodeShape(8, 1, host_mem=64)], mode="node"))
    (gpu,) = enqueue(p, task("gpu", gpus=1, host_mem=30))
    p.dispatch()
    first, big, small = enqueue(
        p, task("gpu", gpus=1), task(host_mem=40), task(host_mem=10)
    )
    # first waits for the GPU; big needs 40 of the 34 GiB left; small fits
    assert uids(p.dispatch()) == [small["uid"]]


def test_tasks_that_declare_many_different_memories_share_few_queues():
    p = ResourcePolicy(Pool([NodeShape(8, host_mem=64)] * 4, mode="node"))
    enqueue(p, *(task(host_mem=1 + i % 63) for i in range(2000)))
    assert len(p._queues[id(p._single)]) <= 7  # 1 to 63 GiB: powers of two
    assert p.dispatch()
    assert all(n.host >= -1e-9 and n.cpu >= -1e-9 for n in p._single.nodes)


def test_a_task_that_declares_no_memory_queues_apart():
    p = ResourcePolicy(Pool([NodeShape(3, host_mem=1)], mode="node"))
    (running,) = enqueue(p, task(host_mem=0.5))
    p.dispatch()
    first, none = enqueue(p, task(host_mem=0.9), task(host_mem=0))
    assert uids(p.dispatch()) == [none["uid"]]  # 0.9 GiB waits; 0 GiB fits beside


def test_fractional_cores_add_up_exactly_enough():
    """Float sums of many small demands do not lose a whole-node task its fit."""
    p = ResourcePolicy(Pool([NodeShape(4)], mode="node"))
    small = enqueue(p, *(task(cpus=0.1) for _ in range(40)))
    assert len(p.dispatch()) == 40
    for t in reversed(small):
        p.release(t["uid"], "DONE")
    (whole,) = enqueue(p, task(cpus=4))
    assert uids(p.dispatch()) == [whole["uid"]]


def test_a_task_that_never_ran_gives_its_demand_back():
    memory = MemoryModel(min_samples=1)
    p = ResourcePolicy(Pool([NodeShape(1)]), memory=memory)
    a, b = enqueue(p, task(), task())
    p.dispatch()
    p.release(a["uid"], None, observed={"host_mem": 5})
    assert uids(p.dispatch()) == [b["uid"]]
    assert memory.estimate("f||1|0", "host") is None  # not learned


def test_release_twice_or_unknown_is_ignored():
    p = ResourcePolicy(Pool([NodeShape(2)], mode="node"))
    a, b, c = enqueue(p, task(), task(), task())
    p.dispatch()
    p.release(a["uid"], "DONE")
    p.release(a["uid"], "DONE")
    p.release("task.nope", "DONE")
    assert uids(p.dispatch()) == [c["uid"]]
    p.release(b["uid"], "DONE")
    p.release(c["uid"], "DONE")
    assert is_idle(p)


def test_pools_per_backend():
    a_pool, b_pool = Pool([NodeShape(1)]), Pool([NodeShape(1)])
    p = ResourcePolicy({"a": a_pool, "b": b_pool})
    t_a1, t_a2, t_b, t_free = enqueue(
        p, task(backend="a"), task(backend="a"), task(backend="b"), task(backend="c")
    )
    out = uids(p.dispatch())
    # one core each: a's second task waits; c has no pool, so no limit
    assert set(out) == {t_a1["uid"], t_b["uid"], t_free["uid"]}
    p.release(t_a1["uid"], "DONE")
    assert uids(p.dispatch()) == [t_a2["uid"]]
    assert p.state()["a"]["running"] == 1 and p.state()["b"]["running"] == 1


# ---------------------------------------------------------------------------
# Order: priority, age and the reservation
# ---------------------------------------------------------------------------


def test_a_whole_node_task_is_not_passed_over_by_smaller_ones():
    """An 8-core task waits only for the tasks that run when it arrives."""
    p = ResourcePolicy(Pool([NodeShape(8)], mode="node"))
    running = enqueue(p, *(task("small", cpus=1) for _ in range(8)))
    assert len(p.dispatch()) == 8
    (big,) = enqueue(p, task("big", cpus=8))
    younger = enqueue(p, *(task("small", cpus=1) for _ in range(20)))
    for t in running[:7]:
        p.release(t["uid"], "DONE")
        assert p.dispatch() == []  # the freed cores are set aside for big
    assert p.state()[None]["reserved_for"] == big["uid"]
    p.release(running[7]["uid"], "DONE")
    assert uids(p.dispatch()) == [big["uid"]]
    p.release(big["uid"], "DONE")
    assert uids(p.dispatch()) == uids(younger[:8])


def test_smaller_tasks_use_what_the_waiting_task_does_not_need():
    """A task waiting for a GPU holds its cores; the other cores stay in use."""
    p = ResourcePolicy(Pool([NodeShape(16, 1)], mode="node"))
    (gpu_hog,) = enqueue(p, task("hog", gpus=1))
    p.dispatch()
    (waiting,) = enqueue(p, task("gpu", cpus=4, gpus=1))
    cpu = enqueue(p, *(task("cpu", cpus=1) for _ in range(14)))
    # 16 cores, 1 for the hog, 4 set aside for the waiting GPU task: 11 start
    assert uids(p.dispatch()) == uids(cpu[:11])
    p.release(gpu_hog["uid"], "DONE")
    assert uids(p.dispatch()) == [waiting["uid"], cpu[11]["uid"]]


def test_the_reservation_takes_one_node_and_others_keep_running():
    nodes = [NodeShape(4, name="n0"), NodeShape(4, name="n1")]
    p = ResourcePolicy(Pool(nodes, mode="node"))
    first = enqueue(p, *(task("small", cpus=1) for _ in range(6)))
    p.dispatch()  # 3 on each node
    on = {h: [t for t in first if t["placement"]["host"] == h] for h in ("n0", "n1")}
    (big,) = enqueue(p, task("big", cpus=4))
    more = enqueue(p, *(task("small", cpus=1) for _ in range(3)))
    # big is held on n0; n1's free core goes to a small task
    assert uids(p.dispatch()) == [more[0]["uid"]]
    assert more[0]["placement"]["host"] == "n1"
    assert p.state()[None]["reserved_for"] == big["uid"]
    p.release(on["n1"][0]["uid"], "DONE")
    assert uids(p.dispatch()) == [more[1]["uid"]]  # n1 keeps running
    for t in on["n0"][:2]:
        p.release(t["uid"], "DONE")
        assert p.dispatch() == []  # n0's cores are set aside for big
    p.release(on["n0"][2]["uid"], "DONE")
    assert uids(p.dispatch()) == [big["uid"]]
    assert big["placement"]["host"] == "n0"


def test_higher_priority_starts_first():
    p = ResourcePolicy(Pool([NodeShape(2)], mode="node"))
    old, low, high = enqueue(p, task(), task(priority=-5), task(priority=3))
    assert uids(p.dispatch()) == [high["uid"], old["uid"]]
    p.release(high["uid"], "DONE")
    assert uids(p.dispatch()) == [low["uid"]]


def test_a_low_priority_big_task_does_not_hold_back_urgent_ones():
    p = ResourcePolicy(Pool([NodeShape(8)], mode="node"))
    running = enqueue(p, *(task(cpus=1) for _ in range(4)))
    p.dispatch()
    (big,) = enqueue(p, task("big", cpus=8, priority=-1))
    urgent = enqueue(p, *(task("ai", cpus=1, priority=1) for _ in range(6)))
    assert uids(p.dispatch()) == uids(urgent[:4])
    assert p.state()[None]["reserved_for"] == urgent[4]["uid"]  # not big
    for t in [*running, *urgent[:4]]:
        p.release(t["uid"], "DONE")
    assert uids(p.dispatch()) == uids(urgent[4:])
    assert p.state()[None]["reserved_for"] == big["uid"]


def test_a_dropped_task_gives_up_its_reservation():
    p = ResourcePolicy(Pool([NodeShape(4)], mode="node"))
    (small,) = enqueue(p, task(cpus=1))
    p.dispatch()
    (big,) = enqueue(p, task("big", cpus=4))
    others = enqueue(p, *(task(cpus=1) for _ in range(3)))
    assert p.dispatch() == []
    assert p.state()[None]["reserved_for"] == big["uid"]
    p.release(big["uid"], None)  # cancelled while queued
    assert p.runnable()
    assert uids(p.dispatch()) == uids(others)
    assert p.state()[None]["reserved_for"] is None


# ---------------------------------------------------------------------------
# Learned memory
# ---------------------------------------------------------------------------


def test_memory_is_learned_from_observed_peaks():
    memory = MemoryModel(min_samples=2, headroom=1.0)
    p = ResourcePolicy(
        Pool([NodeShape(64, host_mem=64)], mode="node", unknown_per_node=2),
        memory=memory,
    )
    tasks = enqueue(p, *(task() for _ in range(10)))
    first = p.dispatch()
    assert uids(first) == uids(tasks[:2])  # memory unknown: 2 per node
    p.release(first[0]["uid"], "DONE", observed={"host_mem": 10})
    assert len(p.dispatch()) == 1  # still unknown after one sample
    p.release(first[1]["uid"], "DONE", observed={"host_mem": 10})
    # learned 10 GiB: the running unknown task is charged 10, and 5 more fit
    assert len(p.dispatch()) == 5
    assert p.state()[None]["nodes"][0]["host_free"] == pytest.approx(4)


@pytest.mark.parametrize("peak", [None, -3, math.nan, math.inf, "12", True])
def test_bad_observed_peaks_are_ignored(peak):
    memory = MemoryModel(min_samples=1)
    p = ResourcePolicy(Pool([NodeShape(4, host_mem=64)]), memory=memory)
    (a,) = enqueue(p, task())
    p.dispatch()
    p.release(a["uid"], "DONE", observed={"host_mem": peak})
    assert memory.estimate("f||1|0", "host") is None
    assert is_idle(p)


def test_a_backend_that_reports_no_peaks_is_named_once(caplog):
    p = ResourcePolicy(Pool([NodeShape(8, host_mem=64)]), memory=MemoryModel())
    tasks = enqueue(p, *(task("fold") for _ in range(4)))
    for _ in range(2):
        for t in p.dispatch():
            p.release(t["uid"], "DONE", observed=None)
    warnings = [r for r in caplog.records if "reported no peak" in r.message]
    assert len(warnings) == 1 and warnings[0].message.startswith("fold:")
    assert is_idle(p) and len(tasks) == 4


def test_learned_gpu_memory_is_capped_per_device_in_aggregate_mode():
    memory = MemoryModel(min_samples=1, headroom=1.0)
    p = ResourcePolicy(Pool([NodeShape(8, 4, gpu_mem=40)]), memory=memory)
    (a,) = enqueue(p, task(gpus=1))
    p.dispatch()
    p.release(a["uid"], "DONE", observed={"gpu_mem": 100})  # more than one device has
    (b,) = enqueue(p, task(gpus=1))
    assert uids(p.dispatch()) == [b["uid"]]
    assert p.state()[None]["gpu_mem_free"] == pytest.approx(120)


def test_learned_peak_above_a_node_is_capped():
    memory = MemoryModel(min_samples=1, headroom=1.0)
    p = ResourcePolicy(Pool([NodeShape(64, host_mem=32)], mode="node"), memory=memory)
    a, b, c = enqueue(p, task(), task(), task())
    p.dispatch()
    p.release(a["uid"], "DONE", observed={"host_mem": 100})  # more than the node has
    p.release(b["uid"], "DONE")
    assert uids(p.dispatch()) == [c["uid"]]  # runs alone, not never


def test_learned_peak_is_capped_by_the_nodes_that_have_the_cores():
    """The cap is the memory of a node the task can run on, not of any node."""
    memory = MemoryModel(min_samples=1, headroom=1.0)
    nodes = [NodeShape(16, host_mem=32), NodeShape(2, host_mem=512)]
    p = ResourcePolicy(Pool(nodes, mode="node"), memory=memory)
    a, b = enqueue(p, task(cpus=8), task(cpus=8))
    assert uids(p.dispatch()) == [a["uid"], b["uid"]]
    p.release(a["uid"], "DONE", observed={"host_mem": 400})
    p.release(b["uid"], "DONE")
    (c,) = enqueue(p, task(cpus=8))
    assert uids(p.dispatch()) == [c["uid"]]  # capped at the 16-core node's 32


def test_uncounted_memory_does_not_limit_unknown_tasks():
    p = ResourcePolicy(Pool([NodeShape(8)], mode="node"), memory=MemoryModel())
    tasks = enqueue(p, *(task() for _ in range(8)))
    assert len(p.dispatch()) == len(tasks)


# ---------------------------------------------------------------------------
# Driving the engine
# ---------------------------------------------------------------------------


class ObservingBackend(ParkingBackend):
    """A ParkingBackend that reports a fixed peak for every task it finishes."""

    def __init__(self, observed=None):
        super().__init__()
        self.observed = observed
        self.descs = []

    async def submit_tasks(self, tasks):
        self.descs.extend(tasks)
        await super().submit_tasks(tasks)

    def finish(self, uid=None, state="DONE"):
        if self.observed is not None:
            self.parked[uid or next(iter(self.parked))]["observed"] = dict(
                self.observed
            )
        return super().finish(uid, state)


async def test_engine_places_tasks_and_learns_memory():
    backend = ObservingBackend(observed={"host_mem": 30})
    policy = ResourcePolicy(
        Pool([NodeShape(8, 2, 64, 40, name="n0")], mode="node", unknown_per_node=1),
        memory=MemoryModel(min_samples=1, headroom=1.0),
    )
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    try:

        @flow.function_task
        async def fold(i):
            return i

        gpu = {"resources": {"gpus": 0.5, "gpu_mem": 15, "host_mem": 1}}
        folds = [fold(i, task_description=gpu) for i in range(4)]
        await until(lambda: len(backend.parked) == 4)
        placed = [t["placement"] for t in backend.descs]
        assert all(p["host"] == "n0" for p in placed)
        assert sorted(g for p in placed for g in p["gpus"]) == [0, 0, 1, 1]

        plain = [fold(10 + i) for i in range(3)]  # memory not declared
        await until(lambda: len(backend.parked) == 5)
        for _ in folds:
            backend.finish()
        await asyncio.wait_for(asyncio.gather(*folds), 2.0)
        await asyncio.sleep(0.02)
        assert len(backend.parked) == 1  # one task of unknown memory per node
        backend.finish()  # it peaked at 30 GiB: the kind is learned
        # two plain tasks of 30 GiB now fit the 64 GiB node at once
        await until(lambda: len(backend.parked) == 2)
        kind = f"fold|{backend.name}|1|0"
        assert policy.memory.estimate(kind, "host") == pytest.approx(30)
        backend.finish()
        backend.finish()
        await asyncio.wait_for(asyncio.gather(*plain), 2.0)
        assert is_idle(policy)
    finally:
        await flow.shutdown()


async def test_engine_fails_a_task_whose_demand_can_never_fit():
    backend = ParkingBackend()
    policy = ResourcePolicy(Pool([NodeShape(4, 1)], mode="node"))
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    try:

        @flow.function_task
        async def job(*deps):
            return None

        too_big = job(task_description={"resources": {"gpus": 2}})
        dependent = job(too_big)
        fine = job()
        await asyncio.wait([too_big, dependent], timeout=2.0)
        assert isinstance(too_big.exception(), ValueError)
        assert isinstance(dependent.exception(), DependencyFailureError)
        await until(lambda: backend.parked)
        backend.finish()
        await asyncio.wait_for(fine, 2.0)
        assert len(backend.submitted) == 1
        assert is_idle(policy)
    finally:
        await flow.shutdown()


async def test_engine_a_pool_named_after_the_default_backend_applies():
    backend = ParkingBackend()
    policy = ResourcePolicy({backend.name: Pool(NodeShape(1))})
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    try:

        @flow.function_task
        async def job(i):
            return i

        futs = [job(i) for i in range(3)]
        await until(lambda: backend.parked)
        await asyncio.sleep(0.02)
        assert len(backend.parked) == 1  # one core
        for _ in futs:
            await until(lambda: backend.parked)
            backend.finish()
        await asyncio.wait_for(asyncio.gather(*futs), 2.0)
        assert backend.max_inflight == 1 and is_idle(policy)
    finally:
        await flow.shutdown()


async def test_engine_cancel_of_a_waiting_task_frees_its_reservation():
    backend = ParkingBackend()
    policy = ResourcePolicy(Pool([NodeShape(4)], mode="node"))
    flow = await WorkflowEngine.create(backend=backend, policy=policy)
    try:

        @flow.function_task
        async def job(name):
            return name

        small = job("small")
        await until(lambda: backend.parked)
        big = job("big", task_description={"resources": {"cpus": 4}})
        others = [job(f"o{i}") for i in range(3)]
        await until(lambda: policy.state()[None]["reserved_for"] is not None)
        assert [arg for _, arg in backend.submitted] == ["small"]
        big.cancel()
        await until(lambda: len(backend.parked) == 4)  # the 3 others start
        backend.finish()
        for _ in others:
            backend.finish()
        await asyncio.wait_for(asyncio.gather(small, *others), 2.0)
        assert is_idle(policy)
    finally:
        await flow.shutdown()


# ---------------------------------------------------------------------------
# Where the reservation goes, and what it holds
# ---------------------------------------------------------------------------


def reserved_host(p):
    """The host name of the node the pool's reservation is on."""
    (_, node, _) = p._reserved[id(p._single)]
    return p._single.nodes[node].shape.name


def test_the_reservation_is_on_a_node_that_can_hold_the_task():
    """Not on a node closer to fitting that is too small for it."""
    big = NodeShape(64, host_mem=256, name="big")
    small = NodeShape(64, host_mem=90, name="small")
    p = ResourcePolicy(Pool([big, small], mode="node"))
    (filler,) = enqueue(p, task("filler", host_mem=240))  # only big can hold it
    p.dispatch()
    (huge,) = enqueue(p, task("huge", host_mem=100))  # small is 10 GiB short, for ever
    tens = enqueue(p, *(task("ten", host_mem=10) for _ in range(20)))
    started = p.dispatch()
    assert reserved_host(p) == "big"
    assert [d["placement"]["host"] for d in started] == ["small"] * 9
    p.release(filler["uid"], "DONE")
    assert p.dispatch()[0] is huge and huge["placement"]["host"] == "big"
    assert len(tens) == 20


def test_the_reservation_is_on_the_node_closest_to_fitting():
    from radical.asyncflow.policies.resources import _Charge

    pool = Pool([NodeShape(8, host_mem=100, name=f"n{i}") for i in range(2)], "node")
    p = ResourcePolicy(pool)
    pool.apply(_Charge(0, (), 0, 7, 0, 0.0, 0.0, False), +1)  # n0: 7 cores busy
    pool.apply(_Charge(1, (), 0, 1, 0, 60.0, 0.0, False), +1)  # n1: 1 core, 60 GiB
    enqueue(p, task("whole", cpus=8))  # n0 misses 7 cores, n1 one
    assert p.dispatch() == []
    assert reserved_host(p) == "n1"


def test_whole_node_tasks_are_not_reserved_behind_each_other():
    """Each waits for a node of its own, not for the one another big task took."""
    p = ResourcePolicy(Pool([NodeShape(8, name=f"n{i}") for i in range(4)], "node"))
    smalls = enqueue(p, *(task("small") for _ in range(32)))
    p.dispatch()
    on = {
        f"n{i}": [t for t in smalls if t["placement"]["host"] == f"n{i}"]
        for i in range(4)
    }
    big0, big1 = enqueue(p, *(task("big", cpus=8) for _ in range(2)))
    enqueue(p, *(task("small") for _ in range(100)))  # a younger stream
    assert p.dispatch() == []
    held0 = reserved_host(p)
    for t in on[held0]:
        p.release(t["uid"], "DONE")
        p.dispatch()
    assert big0["placement"]["host"] == held0
    held1 = reserved_host(p)
    assert held1 != held0  # not behind big0
    for t in on[held1]:
        p.release(t["uid"], "DONE")
        p.dispatch()
    assert big1["placement"]["host"] == held1


def test_the_reservation_follows_a_growing_estimate():
    memory = MemoryModel(min_samples=1, headroom=1.0)
    p = ResourcePolicy(Pool([NodeShape(8, host_mem=64)], mode="node"), memory=memory)
    (t0,) = enqueue(p, task("k", cpus=4))
    p.dispatch()
    p.release(t0["uid"], "DONE", observed={"host_mem": 10})  # learned: 10 GiB
    a, b = enqueue(p, task("k", cpus=4), task("k", cpus=4))
    assert uids(p.dispatch()) == [a["uid"], b["uid"]]
    (w,) = enqueue(p, task("k", cpus=4))
    assert p.dispatch() == []  # held, with 10 GiB
    p.release(a["uid"], "DONE", observed={"host_mem": 40})  # the estimate is 40 now
    hold = p._hold(p._single, p._waiting[w["uid"]])
    assert hold.host == pytest.approx(40)


# ---------------------------------------------------------------------------
# Learned memory that one node can still give
# ---------------------------------------------------------------------------


def hetero_pool():
    a = NodeShape(8, 1, host_mem=512, gpu_mem=16, name="a")  # big host, small GPU
    b = NodeShape(8, 1, host_mem=64, gpu_mem=80, name="b")  # small host, big GPU
    return Pool([a, b], mode="node")


def test_learned_memory_still_fits_the_node_the_kind_ran_on():
    memory = MemoryModel(min_samples=1, headroom=1.1)
    p = ResourcePolicy(hetero_pool(), memory=memory)
    first, waiting = enqueue(
        p, task("k", gpus=1, gpu_mem=70), task("k", gpus=1, gpu_mem=70)
    )
    assert uids(p.dispatch()) == [first["uid"]]
    assert first["placement"]["host"] == "b"  # the only node with 70 GiB GPUs
    p.release(first["uid"], "DONE", observed={"host_mem": 60})  # learned: 66 GiB
    assert uids(p.dispatch()) == [waiting["uid"]]  # capped at b's 64 GiB
    (later,) = enqueue(p, task("k", gpus=1, gpu_mem=70))  # not "can never fit"


def test_early_failures_do_not_teach_a_tiny_estimate():
    memory = MemoryModel()  # 3 samples
    p = ResourcePolicy(Pool([NodeShape(64, host_mem=256)], mode="node"), memory=memory)
    for _ in range(3):  # three tasks fail at once, at a tiny peak
        (t,) = enqueue(p, task("k"))
        p.dispatch()
        p.release(t["uid"], "FAILED", observed={"host_mem": 0.05})
    many = enqueue(p, *(task("k") for _ in range(64)))
    assert len(p.dispatch()) == 2  # still unknown: two per node
    assert memory.estimate("k||1|0", "host") is None
    for t in many[:3]:  # three finish, one of them at a high peak
        p.release(t["uid"], "DONE", observed={"host_mem": 0.05})
        p.dispatch()
    assert memory.estimate("k||1|0", "host") is not None


def test_a_failed_task_peak_still_raises_the_estimate():
    memory = MemoryModel(min_samples=1, headroom=1.0)
    p = ResourcePolicy(Pool([NodeShape(64, host_mem=256)], mode="node"), memory=memory)
    (a,) = enqueue(p, task("k"))
    p.dispatch()
    p.release(a["uid"], "DONE", observed={"host_mem": 10})
    (b,) = enqueue(p, task("k"))
    p.dispatch()
    p.release(b["uid"], "FAILED", observed={"host_mem": 200})  # killed at its peak
    assert memory.estimate("k||1|0", "host") == pytest.approx(200)


# ---------------------------------------------------------------------------
# Small things
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("share", "per_gpu"),
    [(1 / k, k) for k in (2, 3, 5, 7, 8, 9, 11, 13, 16, 20, 32, 64, 100)]
    + [(0.33, 3), (0.2, 5), (0.01, 100)],
)
def test_gpu_shares_pack(share, per_gpu):
    p = ResourcePolicy(Pool([NodeShape(128, 1)], mode="node"))
    enqueue(p, *(task(gpus=share) for _ in range(per_gpu + 1)))
    assert len(p.dispatch()) == per_gpu


def test_a_task_without_a_pool_released_while_queued_is_dropped():
    p = ResourcePolicy({"a": Pool(NodeShape(4))})
    (t,) = enqueue(p, task(backend="other"))
    p.release(t["uid"], None)
    assert p.dispatch() == [] and not p.runnable()


def test_the_documented_example_runs_sixteen_folds_per_node():
    nodes = [NodeShape(cpus=64, gpus=4, host_mem=256, gpu_mem=40, name="gpu000")]
    p = ResourcePolicy(Pool(nodes, mode="node"), memory=MemoryModel())
    fold = {"gpus": 0.25, "gpu_mem": 9, "host_mem": 8}
    enqueue(p, *(task("fold", **fold) for _ in range(17)))
    assert len(p.dispatch()) == 16


def test_a_task_without_a_pool_needs_a_valid_priority_too():
    p = ResourcePolicy({"a": Pool(NodeShape(4))})
    with pytest.raises(ValueError, match="priority"):
        enqueue(p, task(backend="other", priority="high"))


@pytest.mark.parametrize(
    ("mode", "resources"),
    [
        ("node", {"gpus": 0.5, "gpu_mem": 20}),  # 16 GiB per GPU
        ("aggregate", {"gpus": 0.5, "gpu_mem": 20}),  # no GPU has 20
        ("aggregate", {"gpus": 2, "gpu_mem": 10, "cpus": 1}),  # 2 x 10 of 2 x 16: fits
    ],
)
def test_gpu_memory_that_can_never_fit_is_rejected(mode, resources):
    p = ResourcePolicy(Pool([NodeShape(4, 1, 32, 16)] * 2, mode=mode))
    if resources.get("gpus") == 2:
        enqueue(p, task(**resources))
        return
    with pytest.raises(ValueError, match="can never fit"):
        enqueue(p, task(**resources))


def test_node_mode_checks_each_gpu_memory():
    """Slices allow four quarter shares on a GPU, its memory three."""
    p = ResourcePolicy(Pool([NodeShape(64, 1, gpu_mem=30)], mode="node"))
    enqueue(p, *(task(gpus=0.25, gpu_mem=10) for _ in range(4)))
    assert len(p.dispatch()) == 3


def test_aggregate_mode_counts_host_memory():
    p = ResourcePolicy(Pool([NodeShape(8, host_mem=16)] * 2, mode="aggregate"))
    tasks = enqueue(p, *(task(host_mem=10) for _ in range(4)))
    assert uids(p.dispatch()) == uids(tasks[:3])  # 30 of 32 GiB


def test_aggregate_mode_limits_unknown_memory_per_node():
    p = ResourcePolicy(
        Pool([NodeShape(8, host_mem=64)] * 2, unknown_per_node=1), memory=MemoryModel()
    )
    enqueue(p, *(task() for _ in range(4)))
    assert len(p.dispatch()) == 2  # one per node, over two nodes


def test_undeclared_gpu_memory_is_unknown():
    p = ResourcePolicy(
        Pool([NodeShape(64, 2, gpu_mem=40)], mode="node", unknown_per_node=1),
        memory=MemoryModel(),
    )
    enqueue(p, *(task(gpus=0.25) for _ in range(3)))
    assert len(p.dispatch()) == 1


def test_node_mode_spreads_gpu_shares():
    p = ResourcePolicy(Pool([NodeShape(64, 2)], mode="node"))
    a, b = enqueue(p, task(gpus=0.25), task(gpus=0.25))
    p.dispatch()
    assert {a["placement"]["gpus"][0], b["placement"]["gpus"][0]} == {0, 1}


def test_a_task_that_fits_beside_a_reservation_starts_when_it_arrives():
    p = ResourcePolicy(Pool([NodeShape(8)], mode="node"))
    enqueue(p, task(cpus=4))
    p.dispatch()
    enqueue(p, task("big", cpus=8))
    assert p.dispatch() == []  # big waits; 4 cores are set aside for it
    (small,) = enqueue(p, task(cpus=0.5, priority=-1))  # no release in between
    assert p.dispatch() == []  # still within big's reservation
    p2 = ResourcePolicy(Pool([NodeShape(8, 1)], mode="node"))
    enqueue(p2, task(gpus=1))
    p2.dispatch()
    enqueue(p2, task("gpu", cpus=1, gpus=1))
    assert p2.dispatch() == []
    (cpu,) = enqueue(p2, task(cpus=2))  # fits beside it: starts at once
    assert uids(p2.dispatch()) == [cpu["uid"]]


def test_two_dropped_tasks_of_one_demand():
    p = ResourcePolicy(Pool([NodeShape(1)], mode="node"))
    first, a, b, c = enqueue(p, task(), task(), task(), task())
    p.dispatch()
    p.release(a["uid"], None)
    p.release(b["uid"], None)
    p.release(first["uid"], "DONE")
    assert uids(p.dispatch()) == [c["uid"]]


def test_the_estimate_is_the_largest_peak_with_headroom():
    memory = MemoryModel(min_samples=3, headroom=1.1)
    p = ResourcePolicy(Pool([NodeShape(64, host_mem=256)], mode="node"), memory=memory)
    for peak in (4, 10, 6):
        (t,) = enqueue(p, task("k"))
        p.dispatch()
        p.release(t["uid"], "DONE", observed={"host_mem": peak})
    assert memory.estimate("k||1|0", "host") == pytest.approx(11.0)


def test_tasks_that_declare_no_memory_start_on_an_overcommitted_node():
    """A recharge can leave a node's memory below zero; tasks that need none of it still
    start."""
    memory = MemoryModel(min_samples=1, headroom=1.0)
    p = ResourcePolicy(Pool([NodeShape(8, host_mem=16)], mode="node"), memory=memory)
    a, b, c = enqueue(p, task("k"), task("k"), task("k"))
    p.dispatch()  # two of unknown memory
    p.release(a["uid"], "DONE", observed={"host_mem": 20})  # learned: 16, capped
    (free,) = enqueue(p, task("other", host_mem=0, cpus=1))
    assert free["uid"] in uids(p.dispatch())


pytest.importorskip("hypothesis")

from hypothesis import strategies as st  # noqa: E402

from radical.asyncflow.policies.resources import GPU_SLICES  # noqa: E402
from tests.policy_machine import PolicyMachine  # noqa: E402

EPS = 1e-6


def demand_of(d):
    """Cpus, slices, devices, host and per-device GPU memory a task declared."""
    r = d["_declared"]
    gpus = r.get("gpus", 0)
    if gpus <= 0:
        slices = 0
    elif gpus < 1:
        slices = max(1, math.ceil(gpus * GPU_SLICES - 1e-9))
    else:
        slices = int(gpus) * GPU_SLICES
    devices = 0 if gpus <= 0 else max(1, int(gpus))
    return (
        r.get("cpus", 1),
        slices,
        devices,
        r.get("host_mem") or 0,
        (r.get("gpu_mem") or 0) if gpus else 0,
    )


class ResourceMachine(PolicyMachine):
    """No node, device or pool holds more than it has, by a ledger of the declared
    demands; the first-ranked task starts whenever it fits."""

    counted = False
    declares = True

    def make_policy(self, capacity, data, clock):
        shapes = data.draw(
            st.lists(
                st.builds(
                    NodeShape,
                    cpus=st.sampled_from([2, 4, 8]),
                    gpus=st.sampled_from([0, 1, 2]),
                    host_mem=st.sampled_from([None, 16, 32]),
                    gpu_mem=st.sampled_from([None, 16, 24]),
                ),
                min_size=1,
                max_size=3,
            )
        )
        nodes = [
            NodeShape(n.cpus, n.gpus, n.host_mem, n.gpu_mem, name=f"n{i}")
            for i, n in enumerate(shapes)
        ]
        self.pool = Pool(nodes, mode=data.draw(st.sampled_from(["node", "aggregate"])))
        return ResourcePolicy(self.pool)

    def free(self, without=()):
        """What each node has free (node mode), or the pool (aggregate mode), with the
        tasks in flight but those ``without``."""
        nodes = self.pool.nodes
        inf = math.inf
        cpu = [n.shape.cpus * n.shape.cpu_overcommit for n in nodes]
        host = [inf if n.shape.host_mem is None else n.shape.host_mem for n in nodes]
        sl = [[GPU_SLICES] * n.shape.gpus for n in nodes]
        gm = [
            [inf if n.shape.gpu_mem is None else n.shape.gpu_mem] * n.shape.gpus
            for n in nodes
        ]
        if self.pool.mode == "aggregate":
            cpu, host = [sum(cpu)], [sum(host)]
            sl = [[sum(sum(x) for x in sl)]]
            gm = [[sum(sum(x) for x in gm)]]
        for d in self.inflight.values():
            if d["uid"] in without:
                continue
            c, s, devices, h, g = demand_of(d)
            i = d["placement"]["node"] if self.pool.mode == "node" else 0
            cpu[i] -= c
            host[i] -= h
            if self.pool.mode == "aggregate":
                sl[0][0] -= s
                gm[0][0] -= g * devices
            for j in d.get("placement", {}).get("gpus", []):
                sl[i][j] -= min(s, GPU_SLICES)
                gm[i][j] -= g
        return cpu, host, sl, gm

    def fits(self, d, without):
        c, s, devices, h, g = demand_of(d)
        cpu, host, sl, gm = self.free(without)
        if self.pool.mode == "aggregate":
            return (
                c <= cpu[0] + EPS
                and h <= host[0] + EPS
                and s <= sl[0][0]
                and g * devices <= gm[0][0] + EPS
            )
        per_dev = min(s, GPU_SLICES)
        for i in range(len(cpu)):
            if c > cpu[i] + EPS or h > host[i] + EPS:
                continue
            ok = [
                j
                for j in range(len(sl[i]))
                if sl[i][j] >= per_dev and gm[i][j] + EPS >= g
            ]
            if not s or len(ok) >= devices:
                return True
        return False

    def check_dispatch(self, out, queued, slots, first):
        if self.fits(first, without={d["uid"] for d in out}):  # before the dispatch
            assert out and out[0]["uid"] == first["uid"]
        cpu, host, sl, gm = self.free()
        assert all(x >= -EPS for x in cpu + host)
        assert all(x >= -EPS for dev in sl + gm for x in dev)

    def check_idle(self):
        state = self.p.state()[None]
        assert state["cpu_free"] == pytest.approx(self.pool._total[0])
        assert state["gpu_slices_free"] == self.pool._total[2]
        assert state["reserved_for"] is None


TestResourceMachine = ResourceMachine.TestCase

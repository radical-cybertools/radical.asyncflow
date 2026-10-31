"""Scale tests: long chains, thousands of dependents of one slow or failing task, and a
shutdown with hundreds of registrations pending.

They run nightly and on pull requests labelled ``stress``, under both task factories:
``pytest -m stress tests/stress`` and the same with ``--eager-tasks``.
"""

import asyncio

import pytest

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine
from radical.asyncflow.errors import DependencyFailureError

pytestmark = pytest.mark.stress


def kept_uids(flow, uids):
    """The uids among uids that an engine record still holds."""
    stores = (flow.components, flow.dependencies, flow._dependency_count)
    kept = {uid for uid in uids if any(uid in store for store in stores)}
    kept |= set(uids) & set(flow._ready_queue)
    for dependents in flow._dependents_map.values():
        kept |= set(uids) & dependents
    return kept


@pytest.mark.parametrize("chains, length", [(20, 25), (4, 200)])
async def test_executable_to_function_chains(chains, length, tmp_path):
    flow = await WorkflowEngine.create(
        backend=await LocalExecutionBackend(), work_dir=str(tmp_path)
    )

    @flow.executable_task
    async def exe():
        await asyncio.sleep(0.01)  # registers after its first dependent is called
        return "echo 1"

    @flow.function_task
    async def inc(x):
        return int(x) + 1

    tails = []
    for _ in range(chains):
        fut = exe()
        for _ in range(length):
            fut = inc(fut)
        tails.append(fut)

    results = await asyncio.wait_for(asyncio.gather(*tails), timeout=120)
    assert results == [length + 1] * chains
    await flow.shutdown()


async def test_dependents_of_one_slow_task(tmp_path):
    flow = await WorkflowEngine.create(dry_run=True, work_dir=str(tmp_path))

    @flow.executable_task
    async def slow():
        await asyncio.sleep(0.3)
        return "echo hi"

    @flow.function_task
    async def dep(x):
        return x

    root = slow()
    deps = [dep(root) for _ in range(2000)]

    results = await asyncio.wait_for(asyncio.gather(*deps), timeout=60)
    assert results == ["Dummy Output"] * 2000
    assert not getattr(flow, "_pending_registrations", ())
    await flow.shutdown()


async def test_dependents_of_a_failed_registration(tmp_path):
    flow = await WorkflowEngine.create(dry_run=True, work_dir=str(tmp_path))

    @flow.executable_task
    async def bad():
        await asyncio.sleep(0.05)
        return ""  # no command: bad never registers

    @flow.function_task
    async def child(x):
        return x

    @flow.function_task
    async def grandchild(x):
        return x

    root = bad()
    children = [child(root) for _ in range(500)]
    grandchildren = [grandchild(fut) for fut in children]

    results = await asyncio.wait_for(
        asyncio.gather(*children, *grandchildren, return_exceptions=True), timeout=60
    )
    assert all(isinstance(r, DependencyFailureError) for r in results)
    assert {name for r in results[:500] for name in r.failed_dependencies} == {"bad"}
    assert {name for r in results[500:] for name in r.failed_dependencies} == {"child"}
    uids = [fut.uid for fut in (root, *children, *grandchildren)]
    assert not kept_uids(flow, uids)
    await flow.shutdown()


async def test_shutdown_with_pending_registrations(tmp_path):
    flow = await WorkflowEngine.create(dry_run=True, work_dir=str(tmp_path))

    @flow.executable_task
    async def slow():
        await asyncio.sleep(60)  # still running at shutdown, so slow never registers
        return "echo hi"

    @flow.function_task
    async def dep(x):
        return x

    pending = [slow() for _ in range(500)]
    deps = [dep(fut) for fut in pending]
    await asyncio.sleep(0.05)
    await flow.shutdown()

    results = await asyncio.wait_for(
        asyncio.gather(*pending, *deps, return_exceptions=True), timeout=10
    )
    assert all(isinstance(r, asyncio.CancelledError) for r in results)
    await asyncio.sleep(0.05)
    assert not getattr(flow, "_pending_registrations", ())

    late = dep(1)  # the run loop has stopped, so a registered late would never finish
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(late, timeout=5)
    assert late.uid not in flow.components

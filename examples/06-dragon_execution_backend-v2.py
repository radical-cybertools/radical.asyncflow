"""
Dragon V2 Example: Worker Configuration, Scheduling, and DataReferenceV2

Demonstrates:
- DragonExecutionBackendV2 with multi-node worker configuration
- Worker groups with placement policies
- Task pinning policies (affinity, strict)
- DataReferenceV2 for large payloads with DDict storage
"""

import asyncio
import logging
import numpy as np

from radical.asyncflow import DragonExecutionBackendV2, WorkflowEngine
from radical.asyncflow.logging import init_default_logger
from radical.asyncflow.backends.execution.dragon import (
    WorkerGroupConfigV2, WorkerTypeV2, PolicyConfigV2, DataReferenceV2
)

logger = logging.getLogger(__name__)


async def main():
    import multiprocessing as mp
    from dragon.infrastructure.policy import Policy

    # Set Dragon as multiprocessing backend
    mp.set_start_method("dragon")

    # Configure policies for 4 nodes
    policy_n0 = Policy(host_id=0, distribution=Policy.Distribution.BLOCK)
    policy_n1 = Policy(host_id=1, distribution=Policy.Distribution.BLOCK)
    policy_n2 = Policy(host_id=2, distribution=Policy.Distribution.BLOCK)
    policy_n3 = Policy(host_id=3, distribution=Policy.Distribution.BLOCK)

    # Create 2 workers, each spanning 2 nodes
    w0 = WorkerGroupConfigV2(
        name="worker0",
        worker_type=WorkerTypeV2.COMPUTE,
        policies=[
            PolicyConfigV2(policy=policy_n0, nprocs=128),
            PolicyConfigV2(policy=policy_n1, nprocs=128)
        ]
    )

    w1 = WorkerGroupConfigV2(
        name="worker1",
        worker_type=WorkerTypeV2.COMPUTE,
        policies=[
            PolicyConfigV2(policy=policy_n2, nprocs=128),
            PolicyConfigV2(policy=policy_n3, nprocs=128)
        ]
    )

    resources = {"workers": [w0, w1]}

    # Initialize backend and workflow
    backend = await DragonExecutionBackendV2(resources)
    init_default_logger(logging.INFO)
    flow = await WorkflowEngine.create(backend=backend)

    # Task 1: Affinity pinning (prefers worker0 but can run elsewhere if busy)
    @flow.function_task
    async def affinity_task(task_id, task_description={
        "ranks": 4,
        "worker_hint": "worker0",
        "pinning_policy": "affinity"
    }):
        import os
        return f"Task {task_id} on {os.environ['DRAGON_WORKER_NAME']} (AFFINITY)"

    # Task 2: Strict pinning (must run on worker1)
    @flow.function_task
    async def strict_task(task_id, task_description={
        "ranks": 4,
        "worker_hint": "worker1",
        "pinning_policy": "strict"
    }):
        import os
        return f"Task {task_id} on {os.environ['DRAGON_WORKER_NAME']} (STRICT)"

    # Task 3: Large payload with DataReferenceV2
    @flow.function_task
    async def large_data_task(task_id, task_description={
        "ranks": 2,
        "use_ddict_storage": True  # Enable DataReferenceV2 for large payloads
    }):
        import os
        # Generate 5MB of data per rank
        large_array = np.random.rand(5 * 1024 * 1024 // 8)
        return {
            "rank": int(os.environ["DRAGON_RANK"]),
            "data": large_array,
            "size_mb": large_array.nbytes / (1024 * 1024)
        }

    print("=" * 60)
    print("Dragon V2: Worker Configuration and Scheduling")
    print("=" * 60)

    # Run tasks with different pinning policies
    print("\n1. Testing pinning policies...")
    results = await asyncio.gather(
        affinity_task(1),
        strict_task(2),
        affinity_task(3)
    )

    for result in results:
        print(f"   {result[0] if isinstance(result, list) else result}")

    # Test DataReferenceV2
    print("\n2. Testing DataReferenceV2 with large payload...")
    data_result = await large_data_task(4)

    if isinstance(data_result, DataReferenceV2):
        print(f"   ✓ Got DataReferenceV2: {data_result.ref_id}")
        resolved = data_result.resolve()
        print(f"   ✓ Resolved {len(resolved)} ranks:")
        for rank_data in resolved:
            print(f"     - Rank {rank_data['rank']}: {rank_data['size_mb']:.2f} MB")
    else:
        print(f"   ✗ Expected DataReferenceV2, got direct result")

    print("=" * 60)

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

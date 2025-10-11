import asyncio
import logging
import time
import numpy as np

from radical.asyncflow import DragonExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger
from radical.asyncflow.backends.execution.dragon import DataReference

logger = logging.getLogger(__name__)


async def main():
    import multiprocessing as mp
    mp.set_start_method("dragon")
    from dragon.infrastructure.policy import Policy

    # Configure 2 workers on 4 nodes
    policy_n0 = Policy(host_id=0, distribution=Policy.Distribution.BLOCK)
    policy_n1 = Policy(host_id=1, distribution=Policy.Distribution.BLOCK)
    policy_n2 = Policy(host_id=2, distribution=Policy.Distribution.BLOCK)
    policy_n3 = Policy(host_id=3, distribution=Policy.Distribution.BLOCK)
    


    w0 = WorkerGroupConfig(name="worker0",
                           worker_type=WorkerType.COMPUTE,
                           policies=[PolicyConfig(policy=policy_n0, nprocs=128),
                                     PolicyConfig(policy=policy_n1, nprocs=128)]) # worker 0 will occupy 2 nodes

    w1 = WorkerGroupConfig(name="worker1",
                           worker_type=WorkerType.COMPUTE,
                           policies=[PolicyConfig(policy=policy_n2, nprocs=128),
                                     PolicyConfig(policy=policy_n3, nprocs=128)]) # worker 1 will occupy 2 nodes

    resources = {"workers": [w0, w1]}
    
    backend = await DragonExecutionBackend(resources)
    init_default_logger(logging.INFO)
    flow = await WorkflowEngine.create(backend=backend)
    
    # Multi-rank task with DDict storage enabled
    multi_rank_resources = {
        "ranks": 4,
        "use_ddict_storage": True  # Enable DDict storage
    }
    
    @flow.function_task
    async def generate_large_data(task_id, task_description=multi_rank_resources):
        import socket
        import os
        import numpy as np
        
        rank = int(os.environ["DRAGON_RANK"])
        worker = os.environ["DRAGON_WORKER_NAME"]
        host = socket.gethostname()
        
        # Generate 10MB of data per rank
        data_size = 10 * 1024 * 1024
        large_array = np.random.rand(data_size // 8)
        
        return {
            "rank": rank,
            "worker": worker,
            "host": host,
            "data": large_array,
            "data_size_mb": large_array.nbytes / (1024 * 1024),
            "shape": large_array.shape
        }
    
    print("=" * 80)
    print("Testing Multi-Rank Task with Large Payload (DDict Storage)")
    print("=" * 80)
    
    # Submit task
    print("\n1. Submitting multi-rank task (4 ranks, 10MB each)...")
    start_time = time.time()
    result = await generate_large_data(task_id="big_task_1")
    submit_time = time.time() - start_time
    
    print(f"   Task completed in {submit_time:.2f}s")
    print(f"   Return value type: {type(result)}")
    
    # Check if result is a DataReference
    if isinstance(result, DataReference):
        print(f"   ✓ Got DataReference: {result}")
        print(f"   Reference ID: {result.ref_id}")
        print(f"   Backend ID: {result.backend_id}")
        
        # Test manual resolution
        print("\n2. Manually resolving DataReference...")
        resolve_start = time.time()
        resolved_data = result.resolve()
        resolve_time = time.time() - resolve_start
        
        print(f"   Resolution took {resolve_time:.2f}s")
        print(f"   Resolved data type: {type(resolved_data)}")
        print(f"   Number of ranks: {len(resolved_data)}")
        
        # Inspect each rank's data
        print("\n3. Inspecting resolved data from each rank:")
        for rank_data in resolved_data:
            if rank_data is not None:
                print(f"   Rank {rank_data['rank']}:")
                print(f"     - Worker: {rank_data['worker']}")
                print(f"     - Host: {rank_data['host']}")
                print(f"     - Data size: {rank_data['data_size_mb']:.2f} MB")
                print(f"     - Array shape: {rank_data['shape']}")
                print(f"     - Array sum (validation): {rank_data['data'].sum():.2f}")
        
        print(f"\n4. Performance Summary:")
        print(f"   - Task execution + reference creation: {submit_time:.2f}s")
        print(f"   - Reference resolution (fetching 40MB): {resolve_time:.2f}s")
        print(f"   - Total time: {submit_time + resolve_time:.2f}s")
        
    else:
        print(f"   ✗ Expected DataReference, got {type(result)}")
        print(f"   Data: {result}")
    
    print("\n" + "=" * 80)
    
    # Test small payload WITHOUT DDict storage (default)
    print("\nBonus: Testing inline storage (use_ddict_storage=False)")
    small_task_resources = {
        "ranks": 2
        # use_ddict_storage defaults to False
    }
    
    @flow.function_task
    async def generate_small_data(task_id, task_description=small_task_resources):
        import os
        return f"Small data from rank {os.environ['DRAGON_RANK']}"
    
    small_result = await generate_small_data(task_id="small_task")
    print(f"   Result type: {type(small_result)}")
    print(f"   Result: {small_result}")
    
    if isinstance(small_result, DataReference):
        print("   ✗ Small data should NOT be a reference!")
    else:
        print("   ✓ Small data correctly returned inline")
    
    print("=" * 80)
    
    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

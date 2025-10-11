import asyncio
import logging
import time

from radical.asyncflow import DragonExecutionBackend, WorkflowEngine
from radical.asyncflow.backends.execution.dragon import WorkerGroupConfig, WorkerType, PolicyConfig
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)

async def test_cpu_pinning():
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

    # Create backend and workflow
    backend = await DragonExecutionBackend(resources)
    init_default_logger(logging.INFO)
    flow = await WorkflowEngine.create(backend=backend)
    
    # Define default task resources for different policies
    affinity_resources = {
        "ranks": 128,
        "worker_hint": "worker0",
        "pinning_policy": "affinity"
    }
    
    strict_resources = {
        "ranks": 128,
        "worker_hint": "worker1",
        "pinning_policy": "strict"
    }
    
    soft_resources = {
        "ranks": 64,
        "worker_hint": "worker0",
        "pinning_policy": "soft",
        "pinning_timeout": 10
    }
    
    exclusive_resources = {
        "ranks": 128,
        "worker_hint": "worker1",
        "pinning_policy": "exclusive"
    }
    
    normal_resources = {
        "ranks": 64
    }
    
    # Define separate tasks for each policy type
    @flow.function_task
    async def affinity_task(task_id, task_description=affinity_resources):
        import socket
        import os
        await asyncio.sleep(2)
        return (f'Task-{task_id} (AFFINITY): '
                f'RANK-{os.environ["DRAGON_RANK"]} on '
                f'WORKER-{os.environ["DRAGON_WORKER_NAME"]} '
                f'NODE-{socket.gethostname()}')
    
    @flow.function_task
    async def strict_task(task_id, task_description=strict_resources):
        import socket
        import os
        await asyncio.sleep(2)
        return (f'Task-{task_id} (STRICT): '
                f'RANK-{os.environ["DRAGON_RANK"]} on '
                f'WORKER-{os.environ["DRAGON_WORKER_NAME"]} '
                f'NODE-{socket.gethostname()}')
    
    @flow.function_task
    async def soft_task(task_id, task_description=soft_resources):
        import socket
        import os
        await asyncio.sleep(2)
        return (f'Task-{task_id} (SOFT): '
                f'RANK-{os.environ["DRAGON_RANK"]} on '
                f'WORKER-{os.environ["DRAGON_WORKER_NAME"]} '
                f'NODE-{socket.gethostname()}')
    
    @flow.function_task
    async def exclusive_task(task_id, task_description=exclusive_resources):
        import socket
        import os
        await asyncio.sleep(2)
        return (f'Task-{task_id} (EXCLUSIVE): '
                f'RANK-{os.environ["DRAGON_RANK"]} on '
                f'WORKER-{os.environ["DRAGON_WORKER_NAME"]} '
                f'NODE-{socket.gethostname()}')
    
    @flow.function_task
    async def normal_task(task_id, task_description=normal_resources):
        import socket
        import os
        await asyncio.sleep(2)
        return (f'Task-{task_id} (NORMAL): '
                f'RANK-{os.environ["DRAGON_RANK"]} on '
                f'WORKER-{os.environ["DRAGON_WORKER_NAME"]} '
                f'NODE-{socket.gethostname()}')
    
    print("=" * 80)
    print("Testing Worker Pinning Policies")
    print("=" * 80)
    
    x = time.time()
    
    # Launch tasks with different policies
    tasks = []
    tasks.append(affinity_task(0))
    tasks.append(strict_task(1))
    tasks.append(soft_task(2))
    tasks.append(exclusive_task(3))
    tasks.append(normal_task(4))
    
    # Add more mixed tasks
    for i in range(5, 15):
        if i % 3 == 0:
            tasks.append(affinity_task(i))
        elif i % 3 == 1:
            tasks.append(strict_task(i))
        else:
            tasks.append(normal_task(i))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    y = time.time()
    
    print("\n" + "=" * 80)
    print("Results:")
    print("=" * 80)
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Task {i} FAILED: {result}")
        else:
            # Print first rank result for each task
            if isinstance(result, list):
                print(result[0] if result else f"Task {i}: No results")
            else:
                print(result)
    
    print("\n" + "=" * 80)
    print(f'Total Time: {y-x:.2f}s | Throughput: {len(tasks)/(y-x):.2f} tasks/s')
    print("=" * 80)
    
    # Test edge cases
    print("\n" + "=" * 80)
    print("Testing Edge Cases")
    print("=" * 80)
    
    # Test 1: Invalid worker hint
    @flow.function_task
    async def invalid_worker_task(task_description={"ranks": 10, "worker_hint": "worker99", "pinning_policy": "exclusive"}):
        import socket
        import os
        return f'Should not see this'
    
    try:
        result = await invalid_worker_task()
        print(f"Invalid worker test: {result}")
    except Exception as e:
        print(f"Invalid worker test correctly failed: {e}")
    
    # Test 2: Exclusive with insufficient capacity
    @flow.function_task
    async def huge_exclusive_task(task_description={"ranks": 1000, "worker_hint": "worker0", "pinning_policy": "exclusive"}):
        import socket
        import os
        return f'Should not see this'
    
    try:
        result = await huge_exclusive_task()
        print(f"Huge exclusive test: {result}")
    except Exception as e:
        print(f"Huge exclusive test correctly failed: {e}")
    
    await flow.shutdown()

async def test_gpu_pinning():
    import multiprocessing as mp
    mp.set_start_method("dragon")
    from dragon.infrastructure.policy import Policy
    
    # 1 node with 2 GPUs
    policy_n0 = Policy(host_id=0, distribution=Policy.Distribution.BLOCK)

    w1 = WorkerGroupConfig(name="gpu_worker",
                           worker_type=WorkerType.COMPUTE,
                           policies=[PolicyConfig(policy=policy_n0, nprocs=2, ngpus=2),])
    
    resources = {'workers': [w1]}
    backend = await DragonExecutionBackend(resources)
    init_default_logger(logging.INFO)
    flow = await WorkflowEngine.create(backend=backend)
    
    # Task 1: Pin to GPU 0
    task1_resources = {
        "ranks": 1,
        "gpus_per_rank": 1,
        "worker_hint": "gpu_worker",
        "pinning_policy": "exclusive"
    }
    
    # Task 2: Pin to GPU 1 (same config, will get the other GPU)
    task2_resources = {
        "ranks": 1,
        "gpus_per_rank": 1,
        "worker_hint": "gpu_worker",
        "pinning_policy": "exclusive"
    }
    
    @flow.function_task
    async def gpu_task_1(task_description=task1_resources):
        import os
        import socket
        import time
        
        gpu_id = os.environ.get("CUDA_VISIBLE_DEVICES", "NONE")
        rank = os.environ.get("DRAGON_RANK", "?")
        worker = os.environ.get("DRAGON_WORKER_NAME", "?")
        
        # Simulate GPU work
        await asyncio.sleep(3)
        
        return (f"Task1: Rank {rank} on Worker {worker} "
                f"using GPU(s): {gpu_id} on {socket.gethostname()}")
    
    @flow.function_task
    async def gpu_task_2(task_description=task2_resources):
        import os
        import socket
        import time
        
        gpu_id = os.environ.get("CUDA_VISIBLE_DEVICES", "NONE")
        rank = os.environ.get("DRAGON_RANK", "?")
        worker = os.environ.get("DRAGON_WORKER_NAME", "?")
        
        # Simulate GPU work
        await asyncio.sleep(3)
        
        return (f"Task2: Rank {rank} on Worker {worker} "
                f"using GPU(s): {gpu_id} on {socket.gethostname()}")
    
    print("=" * 80)
    print("GPU Pinning Test: 2 Tasks, 2 GPUs")
    print("Expected: Task1 gets GPU 0, Task2 gets GPU 1")
    print("=" * 80)
    
    start = time.time()
    
    # Launch both tasks concurrently
    results = await asyncio.gather(
        gpu_task_1(),
        gpu_task_2()
    )
    
    end = time.time()
    
    print("\n" + "=" * 80)
    print("Results:")
    print("=" * 80)
    for result in results:
        print(result)
    
    print("\n" + "=" * 80)
    print(f"Execution Time: {end - start:.2f}s")
    print("If < 4s, tasks ran in PARALLEL (correct)")
    print("If > 5s, tasks ran SERIALLY (wrong)")
    print("=" * 80)
    
    await flow.shutdown()

if __name__ == "__main__":
    asyncio.run(test_gpu_pinning())

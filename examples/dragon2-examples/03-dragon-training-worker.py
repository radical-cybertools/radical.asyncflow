import asyncio
import logging

from radical.asyncflow import DragonExecutionBackend, WorkflowEngine
from radical.asyncflow.backends.execution.dragon import WorkerGroupConfig, WorkerType, PolicyConfig
from radical.asyncflow.logging import init_default_logger
from dragon.infrastructure.policy import Policy

async def main():
    import socket
    import multiprocessing as mp
    mp.set_start_method("dragon")

    hostname = socket.gethostname()
    
    # Create policies with GPU affinity - one policy per process
    policy_rank0 = Policy(
        placement=Policy.Placement.HOST_NAME,
        host_name=hostname,
        gpu_affinity=[0]  # Rank 0 uses GPU 0
    )
    policy_rank1 = Policy(
        placement=Policy.Placement.HOST_NAME,
        host_name=hostname,
        gpu_affinity=[1]  # Rank 1 uses GPU 1
    )
    
    # Training worker - GPUs managed entirely by policies
    training_worker = WorkerGroupConfig(
        name="training_worker",
        worker_type=WorkerType.TRAINING,
        policies=[PolicyConfig(policy=policy_rank0),
                  PolicyConfig(policy=policy_rank1)],
        kwargs={'nprocs':2, 'ppn': 32,}, # special kwargs gor "pg.configure_training_group"
    )

    resources = {"workers": [training_worker]}

    backend = await DragonExecutionBackend(resources)
    init_default_logger(logging.DEBUG)
    flow = await WorkflowEngine.create(backend=backend)

    # Test 1: Simple worker check - PIN to training_worker
    @flow.function_task
    async def simple_task(task_description={
        "ranks": 2, 
        "worker_hint": "training_worker",  # ← PIN to training worker
        "pinning_policy": "strict"  # ← Use strict pinning
    }):
        """Simple task to verify training worker is running."""
        import os
        import socket
        rank = int(os.environ.get("RANK", os.environ.get("DRAGON_PG_RANK", 0)))
        return {
            "message": "Training worker is working!",
            "rank": rank,
            "hostname": socket.gethostname(),
            "worker_name": os.environ.get("DRAGON_WORKER_NAME", "unknown")
        }

    # Test 2: GPU availability check - PIN to training_worker
    @flow.function_task
    async def check_gpu(task_description={
        "ranks": 2, 
        "worker_hint": "training_worker",  # ← PIN to training worker
        "pinning_policy": "strict"  # ← Use strict pinning
    }):
        """Check GPU availability on each rank."""
        import torch
        import os
        
        rank = int(os.environ.get("RANK", os.environ.get("DRAGON_PG_RANK", 0)))
        local_rank = int(os.environ.get("LOCAL_RANK", os.environ.get("DRAGON_PG_LOCAL_RANK", 0)))
        
        # Get CUDA_VISIBLE_DEVICES - should be set by Dragon policy
        cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES", "")
        
        # Set CUDA device based on what's available
        if torch.cuda.is_available() and torch.cuda.device_count() > 0:
            torch.cuda.set_device(0)  # Use first visible device
        
        result = {
            "rank": rank,
            "local_rank": local_rank,
            "cuda_available": torch.cuda.is_available(),
            "cuda_device_count": torch.cuda.device_count(),
            "cuda_visible_devices": cuda_visible,
        }
        
        if torch.cuda.is_available():
            try:
                result["cuda_device_name"] = torch.cuda.get_device_name(0)
                result["cuda_version"] = torch.version.cuda
                
                # Simple GPU operation on first visible device
                device = torch.device('cuda:0')
                x = torch.tensor([1.0, 2.0, 3.0], device=device)
                y = x * 2
                result["gpu_computation"] = y.cpu().tolist()
            except Exception as e:
                result["error"] = str(e)
        
        return result

    # Test 3: Real distributed training with DDP - PIN to training_worker
    @flow.function_task
    async def distributed_training(task_description={
        "ranks": 2, 
        "worker_hint": "training_worker",  # ← PIN to training worker
        "pinning_policy": "strict"  # ← Use strict pinning
    }):
        """Real distributed training task with PyTorch DDP."""
        import torch
        import torch.nn as nn
        import torch.optim as optim
        import torch.distributed as dist
        import os
        
        # Get distributed env vars set by Dragon's configure_training_group
        rank = int(os.environ.get("RANK", 0))
        local_rank = int(os.environ.get("LOCAL_RANK", 0))
        world_size = int(os.environ.get("WORLD_SIZE", 1))
        
        # With policies, each process sees only its assigned GPU as device 0
        if torch.cuda.is_available() and torch.cuda.device_count() > 0:
            torch.cuda.set_device(0)  # First (and only) visible device
            device = torch.device('cuda:0')
        else:
            device = torch.device('cpu')
        
        # Initialize distributed training
        if not dist.is_initialized():
            dist.init_process_group(backend='nccl' if torch.cuda.is_available() else 'gloo')
        
        # Simple 2-layer neural network
        class TinyNet(nn.Module):
            def __init__(self):
                super().__init__()
                self.fc1 = nn.Linear(10, 50)
                self.fc2 = nn.Linear(50, 1)
                self.relu = nn.ReLU()
            
            def forward(self, x):
                x = self.relu(self.fc1(x))
                x = self.fc2(x)
                return x
        
        # Create model and wrap with DDP
        model = TinyNet().to(device)
        if torch.cuda.is_available():
            model = nn.parallel.DistributedDataParallel(model, device_ids=[0])
        else:
            model = nn.parallel.DistributedDataParallel(model)
        
        criterion = nn.MSELoss()
        optimizer = optim.SGD(model.parameters(), lr=0.01)
        
        # Dummy training data
        X = torch.randn(100, 10).to(device)
        y = torch.randn(100, 1).to(device)
        
        # Training loop
        losses = []
        for epoch in range(10):
            optimizer.zero_grad()
            outputs = model(X)
            loss = criterion(outputs, y)
            loss.backward()
            optimizer.step()
            losses.append(loss.item())
        
        dist.destroy_process_group()
        
        return {
            "rank": rank,
            "local_rank": local_rank,
            "world_size": world_size,
            "device": str(device),
            "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES", ""),
            "initial_loss": losses[0],
            "final_loss": losses[-1],
            "loss_decreased": losses[-1] < losses[0],
            "model_params": sum(p.numel() for p in model.parameters()),
            "ddp_enabled": True
        }

    print("\n" + "="*60)
    print("DISTRIBUTED TRAINING WORKER TESTS (2 GPUs)")
    print("="*60)

    # Run Test 1
    print("\n[Test 1] Checking training worker is running...")
    results = await simple_task()
    for r in results:
        print(f"  Rank {r['rank']}: {r['message']}")
        print(f"    Hostname: {r['hostname']}, Worker: {r['worker_name']}")
    print("  ✓ Test 1 PASSED - Training worker running on both ranks")

    # Run Test 2
    print("\n[Test 2] Checking GPU availability on each rank...")
    gpu_results = await check_gpu()
    for result in gpu_results:
        print(f"  Rank {result['rank']} (local_rank {result['local_rank']}):")
        print(f"    CUDA Available: {result['cuda_available']}")
        print(f"    CUDA Device Count: {result['cuda_device_count']}")
        print(f"    CUDA_VISIBLE_DEVICES: {result['cuda_visible_devices']}")
        if result['cuda_available']:
            print(f"    Device: {result['cuda_device_name']}")
            print(f"    GPU Computation: {result['gpu_computation']}")
        if 'error' in result:
            print(f"    Error: {result['error']}")
    
    all_cuda = all(r['cuda_available'] for r in gpu_results)
    if all_cuda:
        print("  ✓ Test 2 PASSED - Both GPUs accessible")
    else:
        print("  ✗ Test 2 FAILED - GPU not available on some ranks")

    # Run Test 3
    print("\n[Test 3] Running distributed training with DDP...")
    if all_cuda:
        train_results = await distributed_training()
        for result in train_results:
            print(f"  Rank {result['rank']} (local_rank {result['local_rank']}):")
            print(f"    Device: {result['device']}")
            print(f"    CUDA_VISIBLE_DEVICES: {result['cuda_visible_devices']}")
            print(f"    World Size: {result['world_size']}")
            print(f"    DDP Enabled: {result['ddp_enabled']}")
            print(f"    Initial Loss: {result['initial_loss']:.6f}")
            print(f"    Final Loss: {result['final_loss']:.6f}")
            print(f"    Loss Decreased: {result['loss_decreased']}")
        
        if all(r['loss_decreased'] for r in train_results):
            print("  ✓ Test 3 PASSED - Distributed training converged!")
        else:
            print("  ⚠ Test 3 WARNING - Loss didn't decrease on all ranks")
    else:
        print("  ⊘ Test 3 SKIPPED - GPU not available")

    print("\n" + "="*60)
    print("ALL TESTS COMPLETED")
    print("="*60 + "\n")

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

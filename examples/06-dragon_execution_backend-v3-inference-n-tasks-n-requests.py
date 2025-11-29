"""
Dragon V3 Workflow with VLLM Services - High Concurrency Pattern
Demonstrates N tasks with 1 prompt each (instead of 2 tasks with N prompts)
This stresses concurrent request handling - V2 async polling shines here!
"""
import asyncio
import logging
import multiprocessing as mp
from typing import List
import itertools
import time

from radical.asyncflow import DragonVllmInferenceBackend
from radical.asyncflow import DragonExecutionBackendV3, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    """
    + Start Radical.Asyncflow with Dragon execution backend
    + Start VLLM inference services using Dragon
    + Run 1000s of small tasks (1 prompt each) instead of few large batches
    + This demonstrates where async polling (V2) >> thread pool (V1)
    """
    mp.set_start_method("dragon")

    # Create Dragon backend
    nodes = 2  # Total nodes in allocation
    backend = await DragonExecutionBackendV3(
        num_workers=128,
        disable_background_batching=False)

    init_default_logger(logging.INFO)

    logger.info("=" * 60)
    logger.info("Dragon V3: High Concurrency VLLM Workflow")
    logger.info("=" * 60)

    # Create 2 services, each using 1 node and 2 GPUs
    num_services = 2
    nodes_per_service = 1
    services = []

    logger.info(f"Creating {num_services} VLLM services...")

    for i in range(num_services):
        port = 8000 + i
        offset = i * nodes_per_service

        logger.info(f"Service {i+1}: port={port}, offset={offset}, num_nodes={nodes_per_service}")

        service = DragonVllmInferenceBackend(
            config_file="/anvil/scratch/x-aymen/aici-dragon-inference/config.yaml",
            model_name="/anvil/scratch/x-aymen/_dragon_vllm_env/hf_cache/Qwen2.5-0.5B-Instruct",
            num_nodes=nodes_per_service,
            num_gpus=2,
            tp_size=1,
            port=port,
            offset=offset,
            use_service=True,  # Service mode for cross-process communication
            max_batch_size=128,  # Accumulate up to 128 requests
            max_batch_wait_ms=10  # Wait max 10ms to fill batch
        )

        services.append(service)

    # Initialize ALL services concurrently
    logger.info(f"Initializing all {num_services} services concurrently...")
    await asyncio.gather(*[service.initialize() for service in services])

    # Get endpoints
    service_endpoints = [service.get_endpoint() for service in services]

    logger.info(f"All {num_services} services initialized")
    logger.info("Service endpoints:")
    for i, endpoint in enumerate(service_endpoints, 1):
        logger.info(f"  Service {i}: {endpoint}")

    # Create round-robin load balancer
    endpoint_cycle = itertools.cycle(service_endpoints)

    # Create workflow engine
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def run_single_inference(prompt: str, endpoint: str, task_id: int):
        """
        Task that runs inference for a SINGLE prompt.
        This creates high concurrency - 1000s of these running simultaneously!
        """
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{endpoint}/generate",
                json={"prompts": [prompt], "timeout": 300},  # Single prompt
                timeout=aiohttp.ClientTimeout(total=300)
            ) as resp:
                data = await resp.json()

        if data['status'] == 'success':
            return data['results'][0]  # Return single result
        else:
            raise Exception(f"Service error: {data.get('message', 'Unknown error')}")

    # Execute workflow
    logger.info("\n" + "=" * 60)
    logger.info("Running High Concurrency Workflow")
    logger.info("=" * 60)

    # Create 1024 tasks, each with 1 prompt (instead of 2 tasks with 512 prompts each)
    num_tasks = 1024
    prompts = [
        "What is AI?",
        "Explain quantum computing",
        "What is machine learning?",
        "How does the internet work?",
        "What is blockchain?",
    ]

    logger.info(f"Creating {num_tasks} concurrent tasks (1 prompt each)...")
    logger.info("This stresses concurrent request handling!")

    start_time = time.time()

    # Create tasks with round-robin load balancing
    tasks = []
    for i in range(num_tasks):
        prompt = prompts[i % len(prompts)]  # Cycle through prompts
        endpoint = next(endpoint_cycle)      # Cycle through endpoints
        tasks.append(run_single_inference(prompt, endpoint, task_id=i))

    # Run ALL tasks concurrently
    logger.info(f"Launching {num_tasks} concurrent inference tasks...")
    results = await asyncio.gather(*tasks)

    end_time = time.time()
    total_time = end_time - start_time

    logger.info("\n" + "=" * 60)
    logger.info("High Concurrency Results")
    logger.info("=" * 60)

    logger.info(f"Total tasks: {num_tasks}")
    logger.info(f"Total time: {total_time:.2f}s")
    logger.info(f"Throughput: {num_tasks / total_time:.2f} prompts/second")
    logger.info(f"Avg latency: {total_time / num_tasks * 1000:.2f}ms per prompt")

    # Cleanup
    logger.info("\n" + "=" * 60)
    logger.info("Shutting down all services")
    logger.info("=" * 60)

    await asyncio.gather(*[service.shutdown() for service in services])
    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

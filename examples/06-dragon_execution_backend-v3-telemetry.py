"""
Dragon V3 Workflow with Multiple VLLM Services (Node-Partitioned)
Each service uses different nodes via offset
"""
import os
import json
import asyncio
import logging
import multiprocessing as mp
from typing import List
import itertools

from radical.asyncflow import DragonVllmInferenceBackend
from radical.asyncflow import DragonExecutionBackendV3, WorkflowEngine
from radical.asyncflow.logging import init_default_logger


from node_collector import NodeTelemetryCollector
logger = logging.getLogger(__name__)


async def main():
    """
    + Start Radical.Asyncflow with Dragon execution backend.
    + Start the VLLM inference engine as a service using Dragon
    + Distribuite the workflows across the services in RR fashion via Asyncflow 
    + Service endpoints are centralized on the head node for:
        - Easier endpoint management
        - Simpler load balancing
        - Centralized logging
        - Better control plane
    
    Note: Only send batch tasks. I.e, tasks that can send N requests.
    """
    mp.set_start_method("dragon")

    # Create Dragon backend
    nodes = 2  # Total nodes in allocation
    backend = await DragonExecutionBackendV3(
        num_workers=15,
        disable_background_batching=False)

    init_default_logger(logging.INFO)

    logger.info("=" * 60)
    logger.info("Dragon V3: Multi-Service VLLM Workflow (Node-Partitioned)")
    logger.info("=" * 60)

    # Create 2 inference pipelines, each using 1 node and 2 GPUs from different offset
    num_services = 2
    nodes_per_service = 1
    services = []

    logger.info(f"Creating {num_services} VLLM services...")

    for i in range(num_services):
        port = 8000 + i
        offset = i * nodes_per_service  # Each pipeline starts at different node

        logger.info(f"Service {i+1}: port={port}, offset={offset}, num_nodes={nodes_per_service}")

        service = DragonVllmInferenceBackend(
            config_file="/anvil/scratch/x-aymen/aici-dragon-inference/config.yaml",
            model_name="/anvil/scratch/x-aymen/_dragon_vllm_env/hf_cache/Qwen2.5-0.5B-Instruct",
            num_nodes=nodes_per_service,
            num_gpus=2,
            tp_size=1,
            port=port,
            offset=offset  # Change this to control the number of nodes each inference pipeline takes
        )

        services.append(service)

    # Initialize ALL services concurrently
    logger.info(f"Initializing all {num_services} services concurrently...")
    await asyncio.gather(*[service.initialize() for service in services])

    # Get endpoints
    service_endpoints = [service.get_endpoint() for service in services]


    collector = NodeTelemetryCollector(
        collection_rate=1.0,              # Collect every second
        checkpoint_interval=30.0,         # Checkpoint every 30 seconds
        checkpoint_dir=os.path.join(os.getcwd(), 'telemetry-results'),  # Save checkpoints here
        checkpoint_count=10,              # Keep last 10 checkpoints
        enable_cpu=True,
        enable_gpu=True,
        enable_memory=True,
        metric_prefix="infer-asyncflow"   # Prefix all metrics
    )

    # Start collection (spawns processes on all nodes)
    collector.start()

    logger.info(f"All {num_services} services initialized")
    logger.info("Node allocation:")
    for i in range(num_services):
        offset = i * nodes_per_service
        logger.info(f"Service {i+1}: nodes[{offset}:{offset+nodes_per_service}] -> {service_endpoints[i]}")

    # Create round-robin load balancer
    endpoint_cycle = itertools.cycle(service_endpoints)

    # Create workflow engine
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def run_inference(prompts: List[str], endpoint: str):
        """Task that runs inference using HTTP requests (async)"""
        import time
        start = time.time()
        logger.info(f"START {endpoint} at {start}")
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{endpoint}/generate",
                json={"prompts": prompts, "timeout": 300},
                timeout=aiohttp.ClientTimeout(total=300)
            ) as resp:
                data = await resp.json()

        end = time.time()
        logger.info(f"END {endpoint} at {end}, duration={end-start:.2f}s")

        if data['status'] == 'success':
            logger.info(f'Batch of {len(prompts)} inference requests is completed on {endpoint}')
            return data['results']
        else:
            raise Exception(f"Service error: {data.get('message', 'Unknown error')}")
    
    # Execute workflow
    logger.info("\n" + "=" * 60)
    logger.info("Running Workflow Tasks (Load Balanced)")
    logger.info("=" * 60)
    
    prompts_batch1 = ['hi'] * 512
    prompts_batch2 = ['hi'] * 512
    
    # Run multiple inference tasks with load balancing
    for i in range(100):
        results = await asyncio.gather(
            run_inference(prompts_batch1, next(endpoint_cycle)),
            run_inference(prompts_batch2, next(endpoint_cycle)),
        )

    logger.info("\n" + "=" * 60)
    logger.info("Inference Results")
    logger.info("=" * 60)

    with open("vllm_infer.json", "w") as f:
        json.dump(results, f, indent=2)

    import ast

    # `results` should be a list of batches,
    # where each batch is a list of result dicts from one service.
    #
    # Example shape:
    # [
    #   [ {service1 result}, {service1 result}, ... ],   # batch 1
    #   [ {service2 result}, {service2 result}, ... ],   # batch 2
    # ]

    # Parse all result payloads
    parsed = results

    # Number of services = number of top-level batches
    num_services = len(parsed)

    # Total number of prompts across all services
    total_prompts = sum(len(batch) for batch in parsed)

    # End-to-end total wall time = max latency across ALL requests
    total_time = max(
        r['end_to_end_latency']
        for batch in parsed
        for r in batch
    )

    # Collect per-service throughput (take first entry per service)
    service_total_tokens = []
    service_output_tokens = []

    for batch in parsed:
        # Each batch contains many requests from ONE service
        first = batch[0]
        service_total_tokens.append(first['total_tokens_per_second'])
        service_output_tokens.append(first['total_output_tokens_per_second'])

    # Sum throughput across services
    total_tokens_throughput = sum(service_total_tokens)
    total_output_tokens_throughput = sum(service_output_tokens)

    # Prompts per second using real wall time
    prompts_per_second = total_prompts / total_time

    print("=" * 60)
    print("THROUGHPUT RESULTS")
    print("=" * 60)
    print(f"Total prompts processed: {total_prompts}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Prompts/second: {prompts_per_second:.2f}")
    print(f"Total tokens/second: {total_tokens_throughput:.2f}")
    print(f"Output tokens/second: {total_output_tokens_throughput:.2f}")
    print(f"Services used: {num_services}")
    print("=" * 60)

    # Cleanup
    logger.info("\n" + "=" * 60)
    logger.info("Shutting down all services")
    logger.info("=" * 60)

    await asyncio.gather(*[service.shutdown() for service in services])
    await flow.shutdown()
    collector.stop()

if __name__ == "__main__":
    asyncio.run(main())


"""
Complete Working Example with Your Exact API

This demonstrates the real Dragon Batch integration with WorkflowManager
using your exact API pattern.
"""

import asyncio
import logging
import time
import argparse
from radical.asyncflow import DragonExecutionBackend, WorkflowEngine

from radical.asyncflow.logging import init_default_logger
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run Dragon AsyncFlow workflow with configurable resources')
    parser.add_argument('--tasks', type=int, required=True,
                       help='Total number of tasks to run')
    parser.add_argument('--nodes', type=int, required=True,
                       help='Total number of nodes to run')
    parser.add_argument('--run_id', type=int, required=True,
                       help='number of this run')
    parser.add_argument('--mode', type=str, required=True,
                       help='strong/weak scaling')
    return parser.parse_args()

async def main():
    """
    Main workflow execution using your exact API.
    
    This creates 1024 concurrent workflows, each with 3 dependent tasks.
    """
    args = parse_args()

    import dragon
    import multiprocessing as mp
    # Create backend and workflow
    mp.set_start_method("dragon")
    # Create Dragon Batch backend
    backend = DragonExecutionBackend(num_workers=args.nodes * 128,
                                     disable_background_batching=False)
    init_default_logger(logging.INFO, output_file=f'{args.mode}.n{args.nodes}.t{args.tasks}.{args.run_id}.log')
    # Create workflow engine with Dragon Batch backend
    flow = await WorkflowEngine.create(backend=backend)

    # Define tasks using your exact API
    @flow.function_task
    async def task1(*args):
        # Simulate lightweight data generation (e.g., creating a list of numbers)
        return

    # Run workflows concurrently
    start_time = time.time()
    results = await asyncio.gather(*[task1() for i in range(args.tasks)])
    end_time = time.time()
    elapsed = end_time - start_time

    print(f"Total time for {args.tasks} tasks: {elapsed} seconds")

    # Shutdown
    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())



"""
Complete Working Example with Your Exact API

This demonstrates the real Dragon Batch integration with WorkflowManager
using your exact API pattern.
"""

import asyncio
import logging
import time
from radical.asyncflow import DragonExecutionBackend, WorkflowEngine

from radical.asyncflow.logging import init_default_logger
logger = logging.getLogger(__name__)



async def main():
    """
    Main workflow execution using your exact API.

    """
    import dragon
    import multiprocessing as mp
    # Create backend and workflow
    mp.set_start_method("dragon")
    # Create Dragon Batch backend
    nodes = 4
    backend = DragonExecutionBackend(num_workers=nodes * 128,
                                     disable_background_batching=False)
    init_default_logger(logging.INFO)
    # Create workflow engine with Dragon Batch backend
    flow = await WorkflowEngine.create(backend=backend)

    # Define tasks using your exact API
    @flow.executable_task
    async def single_process_executable(*args, task_description={'process_template': {}}):
        # Simulate lightweight data generation (e.g., creating a list of numbers)
        return "true"

    # Define tasks using your exact API
    @flow.executable_task
    async def parallel_job_executable(*args, task_description={'process_templates': [(2, {}), (2, {})]}
                                                                            ):
        # Simulate lightweight data generation (e.g., creating a list of numbers)
        return "true"

    # Define tasks using your exact API
    @flow.function_task
    async def single_process_function(*args, task_description={'process_template': {}}):
        # Simulate lightweight data generation (e.g., creating a list of numbers)
        return "true"

    # Define tasks using your exact API
    @flow.function_task
    async def parallel_job_function(*args, task_description={'process_templates': [(2, {}),
                                                                                   (2, {})]}):
        # Simulate lightweight data generation (e.g., creating a list of numbers)
        return "true"

    # Define tasks using your exact API
    @flow.function_task
    async def native_function(*args):
        # Simulate lightweight data generation (e.g., creating a list of numbers)
        return "true"


    tasks = [single_process_executable(),
             parallel_job_executable(),
             single_process_function(),
             parallel_job_function(),
             native_function()]

    # Run workflows concurrently
    start_time = time.time()
    results = await asyncio.gather(*tasks)
    end_time = time.time()
    elapsed = end_time - start_time

    print(results)

    # Shutdown
    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())



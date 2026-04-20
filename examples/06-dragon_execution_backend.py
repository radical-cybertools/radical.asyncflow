"""
Dragon V3 Example: Batch Processing

Demonstrates:
- DragonExecutionBackendV3 with batch processing
- executable_task and function_task decorators
- process_template for single-process tasks
- process_templates for multi-process parallel jobs
- Native function execution (no task_description)

Requires: pip install rhapsody-py
RHAPSODY docs: https://radical-cybertools.github.io/rhapsody/
AsyncFlow integration: https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration
"""

import asyncio
import logging

from rhapsody.backends import DragonExecutionBackendV3

from radical.asyncflow import WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    import multiprocessing as mp

    # Set Dragon as multiprocessing backend
    mp.set_start_method("dragon")

    backend = await DragonExecutionBackendV3()
    init_default_logger(logging.INFO)

    # Create workflow engine
    flow = await WorkflowEngine.create(backend=backend)

    # Resource descriptions
    single_process = {"process_template": {}}
    parallel_processes = {"process_templates": [(2, {}), (2, {})]}

    # Single-process executable task
    @flow.executable_task
    async def single_executable(*args, task_description=single_process):
        return "/bin/bash -c 'echo $HOSTNAME'"

    # Parallel-process executable task (2 processes)
    @flow.executable_task
    async def parallel_executable(*args, task_description=parallel_processes):
        return "/bin/bash -c 'echo $HOSTNAME'"

    # Single-process function task
    @flow.function_task
    async def single_function(task_description=single_process):
        import socket

        return socket.gethostname()

    # Parallel-process function task
    @flow.function_task
    async def parallel_function(task_description=parallel_processes):
        import socket

        return socket.gethostname()

    # Native function (no task_description)
    @flow.function_task
    async def native_function():
        import socket

        return socket.gethostname()

    print("=" * 60)
    print("Dragon V3: Batch Processing Example")
    print("=" * 60)

    # Execute all task types concurrently
    results = await asyncio.gather(
        single_executable(),
        parallel_executable(),
        single_function(),
        parallel_function(),
        native_function(),
    )

    print("\nResults:")
    for i, result in enumerate(results, 1):
        print(f"  {i}. {result}")

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

"""
Dragon Example: Batch Processing

Demonstrates:
- DragonExecutionBackend with batch processing
- executable_task and function_task decorators
- process_template for single-process tasks
- process_templates for multi-process parallel jobs
- Native function execution (no task_description)

A function task with a process template runs in separate Dragon processes, and its
future resolves to their exit codes, not to the function's return value. Only a
native function task (no template) returns its value.

Requires: pip install "rhapsody-py[dragon]"
Run with: dragon examples/06-dragon_execution_backend.py
RHAPSODY docs: https://radical-cybertools.github.io/rhapsody/
AsyncFlow integration: https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration
"""

import asyncio
import logging

from rhapsody.backends import DragonExecutionBackend

from radical.asyncflow import WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    import multiprocessing as mp

    # Set Dragon as multiprocessing backend
    mp.set_start_method("dragon")

    backend = await DragonExecutionBackend()
    init_default_logger(logging.INFO)

    # Create workflow engine
    flow = await WorkflowEngine.create(backend=backend)

    # Resource descriptions
    single_process = {"process_template": {}}
    # The processes are not MPI programs, so skip PMI setup
    parallel_processes = {"process_templates": [(2, {}), (2, {})], "pmi": "none"}

    # Single-process executable task
    @flow.executable_task
    async def single_executable(*args, task_description=single_process):
        return "/bin/bash -c 'echo $HOSTNAME'"

    # Parallel-process executable task (2 processes)
    @flow.executable_task
    async def parallel_executable(*args, task_description=parallel_processes):
        return "/bin/bash -c 'echo $HOSTNAME'"

    # Single-process function task: resolves to the process exit code
    @flow.function_task
    async def single_function(task_description=single_process):
        import socket

        print(f"single_function on {socket.gethostname()}")

    # Parallel-process function task: resolves to one exit code per process
    @flow.function_task
    async def parallel_function(task_description=parallel_processes):
        import socket

        print(f"parallel_function on {socket.gethostname()}")

    # Native function (no task_description)
    @flow.function_task
    async def native_function():
        import socket

        return socket.gethostname()

    print("=" * 60)
    print("Dragon: Batch Processing Example")
    print("=" * 60)

    # Execute all task types concurrently
    results = await asyncio.gather(
        single_executable(),
        parallel_executable(),
        single_function(),
        parallel_function(),
        native_function(),
    )

    labels = [
        "single_executable (stdout)",
        "parallel_executable (stdout)",
        "single_function (exit code)",
        "parallel_function (exit codes)",
        "native_function (return value)",
    ]

    print("\nResults:")
    for label, result in zip(labels, results, strict=True):
        print(f"  {label}: {result}")

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

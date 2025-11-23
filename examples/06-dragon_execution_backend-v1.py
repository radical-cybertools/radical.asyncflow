"""
Dragon V1 Example: Simple Backend with Shared Memory (DDict)

Demonstrates:
- DragonExecutionBackendV1 initialization (no configuration needed)
- Basic function task execution
- Dragon distributed dictionary (DDict) for shared memory
- Parallel task execution
"""

import asyncio
import logging

from radical.asyncflow import DragonExecutionBackendV1, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    # Initialize Dragon V1 backend and workflow engine
    backend = await DragonExecutionBackendV1()
    init_default_logger(logging.INFO)
    flow = await WorkflowEngine.create(backend=backend)

    from dragon.data.ddict import DDict

    # Create a Dragon distributed dictionary (shared memory store)
    # 2 managers, 3 nodes, 1GB total size
    shared_dict = DDict(2, 3, 1 * 1024 * 1024 * 1024)

    @flow.function_task
    async def add_to_dict(client_id):
        key = f"key_{client_id}"
        value = f"value_{client_id}"
        shared_dict[key] = value
        return key

    @flow.function_task
    async def validate_keys(keys):
        results = {k: shared_dict[k] for k in keys}
        return results

    # Launch multiple tasks to add entries in parallel
    print("Launching 5 parallel tasks to populate shared dictionary...")
    add_tasks = [add_to_dict(i) for i in range(5)]
    keys = await asyncio.gather(*add_tasks)

    # Validate all keys were written
    result = await validate_keys(keys)
    print(f"Final result: {result}")

    # Cleanup
    shared_dict.destroy()
    await flow.shutdown()


if __name__ == '__main__':
    asyncio.run(main())

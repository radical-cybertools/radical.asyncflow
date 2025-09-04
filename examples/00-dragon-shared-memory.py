import asyncio
import logging
import time
import os


from radical.asyncflow import DragonExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)

async def main():
    # Initialize Dragon backend and workflow engine
    backend = await DragonExecutionBackend()
    init_default_logger(logging.DEBUG)
    flow = await WorkflowEngine.create(backend=backend)

    from dragon.data.ddict import DDict

    # Create a Dragon distributed dictionary (shared memory store)
    # 2 managers, 1 node, 1GB total size

    shared_dict = DDict(2, 3, 1 * 1024 * 1024 * 1024)

    @flow.function_task
    async def add_to_dict(client_id):
        key = f"hello{client_id}"
        value = f"world{client_id}"
        shared_dict[key] = value
        print(f"Task {client_id}: added {key} -> {value}")
        return key  # return the key for chaining

    @flow.function_task
    async def validate_keys(keys):
        print(f"Validating {len(keys)} keys in shared_dict...")
        for key in keys:
            val = shared_dict[key]
            assert val.startswith("world")
        return {k: shared_dict[k] for k in keys}

    async def run_workflow():
        print(f"Starting workflow at {time.time()}")

        # Launch multiple tasks to add entries in parallel
        add_tasks = [add_to_dict(i) for i in range(10)]
        keys = await asyncio.gather(*add_tasks)

        # Validate keys
        result = await validate_keys(keys)
        print(f"Validation complete: {result}")

    await run_workflow()

    # Cleanup
    shared_dict.destroy()
    await flow.shutdown()

if __name__ == '__main__':
    asyncio.run(main())

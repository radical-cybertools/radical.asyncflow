import asyncio
import logging
import time

from radical.asyncflow import DragonExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    init_default_logger(logging.INFO)

    # Create backend and workflow
    backend = await DragonExecutionBackend()
    flow = await WorkflowEngine.create(backend=backend)

    task1_resources = {"ranks": 256}

    @flow.executable_task
    async def task1(task_description=task1_resources):
        import os
        exec1 = os.path.join(os.getcwd(), 'hello_world.sh')
        return exec1

    async def run_wf(wf_id):
        logger.info(f"Starting workflow {wf_id} at {time.time()}")
        t1 = task1()
        t1_result = await t1
        logger.info(f"Workflow {wf_id} finished at {time.time()}")
        logger.info(t1_result)

    # Run workflows concurrently
    await asyncio.gather(*[run_wf(i) for i in range(1)])

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

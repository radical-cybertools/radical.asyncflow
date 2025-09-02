import asyncio
import logging
import time

from radical.asyncflow import DragonExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    init_default_logger(logging.DEBUG)

    # Create backend and workflow
    backend = await DragonExecutionBackend({"resource": "local.localhost"})
    flow = await WorkflowEngine.create(backend=backend)

    task1_resources = {"ranks": 4}

    @flow.executable_task
    async def task1(task_description=task1_resources):
        import os
        exe = os.path.join(os.getcwd(), "hello_mpi")
        return exe

    async def run_wf(wf_id):
        logger.info(f"Starting workflow {wf_id} at {time.time()}")
        t1 = task1()
        tt = await t1
        logger.info(f"Workflow {wf_id} finished at {time.time()}")
        return tt

    # Run workflows concurrently
    results = await asyncio.gather(*[run_wf(i) for i in range(1)])
    logger.info(results)

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

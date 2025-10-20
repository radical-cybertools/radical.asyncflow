import asyncio
import logging
import time

from radical.asyncflow import DragonExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    import dragon
    import multiprocessing as mp
    # Create backend and workflow
    mp.set_start_method("dragon")
    backend = await DragonExecutionBackend()
    init_default_logger(logging.INFO)
    flow = await WorkflowEngine.create(backend=backend)

    task1_resources = {"ranks": 2}

    @flow.function_task
    async def task1(task_description=task1_resources):
        import socket
        import asyncio
        await asyncio.sleep(2)
        return socket.gethostname()

    async def run_wf(wf_id):
        logger.info(f"Starting workflow {wf_id} at {time.time()}")
        t1 = task1()
        t1_result = await t1
        logger.info(f"Workflow {wf_id} finished at {time.time()}")
        logger.info(t1_result)

    # Run workflows concurrently
    x = time.time()
    results = await asyncio.gather(*[task1() for i in range(10000)])
    y = time.time()

    print(f'TTX: {y-x}s, THP: {10000/(y-x)}tasks/s')

    for r in results:
        print(r)

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

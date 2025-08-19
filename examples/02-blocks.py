import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from radical.asyncflow import ConcurrentExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)

async def main():

    init_default_logger(logging.INFO)

    # Create backend and workflow
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor())
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def task1(*args):
        logger.info('TASK: task1 executing')
        return 'task1 result'

    @flow.function_task
    async def task2(*args):
        logger.info('TASK: task2 executing')
        return 'task2 result'

    @flow.function_task
    async def task3(*args):
        logger.info('TASK: task3 executing')
        return 'task3 result'

    @flow.block
    async def block(block_id, wf_id, *args):
        logger.info(f'BLOCK-{block_id}: Starting workflow {wf_id}')
        t1 = task1()
        t2 = task2(t1)
        t3 = task3(t1, t2)
        await t3
        logger.info(f'BLOCK-{block_id}: Workflow {wf_id} completed')

    async def run_blocks(wf_id):
        b1 = block(1, wf_id)
        b2 = block(2, wf_id, b1)
        await b2

    # Run workflows concurrently
    await asyncio.gather(*[run_blocks(i) for i in range(10)])

    await flow.shutdown()

if __name__ == '__main__':
    asyncio.run(main())

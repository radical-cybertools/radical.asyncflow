import time
import asyncio
import logging

from concurrent.futures import ThreadPoolExecutor

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ConcurrentExecutionBackend
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)

async def main():

    init_default_logger(logging.INFO)

    # Create backend and workflow
    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor())
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def task1(*args):
        logger.info(f'TASK: task1 executing at {time.time()}')
        return 'task1 result'

    @flow.function_task
    async def task2(*args):
        logger.info(f'TASK: task2 executing at {time.time()}')
        return 'task2 result'

    @flow.block
    async def block1(*args):
        logger.info(f'BLOCK: block1 started at {time.time()}')
        t1 = task1()
        t2 = task2(t1)
        await t2
        logger.info(f'BLOCK: block1 completed at {time.time()}')

    @flow.block
    async def block2(*args):
        logger.info(f'BLOCK: block2 started at {time.time()}')
        t3 = task1()
        t4 = task2(t3)
        await t4
        logger.info(f'BLOCK: block2 completed at {time.time()}')

    @flow.block
    async def block1_of_blocks(*args):
        logger.info(f'NESTED-BLOCK: block of blocks-1 started at {time.time()}')
        b1 = block1()
        b2 = block2(b1)
        await b2
        logger.info(f'NESTED-BLOCK: block of blocks-1 completed at {time.time()}')

    @flow.block
    async def block2_of_blocks(*args):
        logger.info(f'NESTED-BLOCK: block of blocks-2 started at {time.time()}')
        b1 = block1()
        b2 = block2(b1)
        await b2
        logger.info(f'NESTED-BLOCK: block of blocks-2 completed at {time.time()}')

    async def run_block_of_blocks(i):
        logger.info(f'WORKFLOW: run_block_of_blocks {i} starting at {time.time()}')
        bob1 = block1_of_blocks()
        bob2 = block2_of_blocks(bob1)
        await bob2
        logger.info(f'WORKFLOW: Block of blocks-{i} is finished at {time.time()}')

    await asyncio.gather(*[run_block_of_blocks(i) for i in range(2)])

    await flow.shutdown()

if __name__ == '__main__':
    asyncio.run(main())

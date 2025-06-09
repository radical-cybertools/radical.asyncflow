import time
import asyncio

from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionBackend


async def main():

    backend = RadicalExecutionBackend({'resource': 'local.localhost'})
    flow = WorkflowEngine(backend=backend)

    @flow.executable_task
    async def task1(*args):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.executable_task
    async def task2(*args):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.block
    async def block1(*args):
        print(f'block1 started at {time.time()}')
        t1 = task1()
        t2 = task2(t1)
        await t2

    @flow.block
    async def block2(*args):
        print(f'block2 started at {time.time()}')
        t3 = task1()
        t4 = task2(t3)
        await t4

    @flow.block
    async def block1_of_blocks(*args):
        print(f'block of blocks-1 started at {time.time()}')
        b1 = block1()
        b2 = block2(b1)
        await b2

    @flow.block
    async def block2_of_blocks(*args):
        print(f'block of blocks-2 started at {time.time()}')
        b1 = block1()
        b2 = block2(b1)
        await b2

    async def run_block_of_blocks(i):
        bob1 = block1_of_blocks()
        bob2 = block2_of_blocks(bob1)
        await bob2
        print(f'Block of blocks-{i} is finished')

    await asyncio.gather(*[run_block_of_blocks(i) for i in range(2)])

    await flow.shutdown()


if __name__ == '__main__':
    asyncio.run(main())

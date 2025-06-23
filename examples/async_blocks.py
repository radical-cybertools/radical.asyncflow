import time
import asyncio

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import RadicalExecutionBackend

async def main():
    # Create backend and workflow
    backend = RadicalExecutionBackend({'resource': 'local.localhost'})
    flow = WorkflowEngine(backend=backend)

    @flow.executable_task
    async def task1(*args):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.executable_task
    async def task2(*args):
        return '/bin/echo "I got executed at" && /bin/date' 

    @flow.executable_task
    async def task3(*args):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.executable_task
    async def task4(*args):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.executable_task
    async def task5(*args):
        return '/bin/echo "I got executed at" && /bin/date'


    @flow.block
    async def block1(wf_id, *args):
        print(f'Starting workflow {wf_id}')
        t1 = task1()
        tt = task1()
        t2 = task2(t1)
        t3 = task3(t1, t2)
        t4 = task4(t3)

        if await t4:
            t5 = task5() 
            await t5

        return f'Workflow {wf_id} completed'


    @flow.block
    async def block2(wf_id, *args):
        print(f'\nStarting workflow {wf_id}')
        t1 = task1()
        t2 = task2(t1)
        t3 = task3(t1, t2)
        t4 = task4(t3)

        if await t4:
            t5 = task5()
            await t5

        return f'Workflow {wf_id} completed'


    async def run_blocks(wf_id):

        b1 = block1(wf_id)
        b2 = block2(wf_id, b1)
        await b2
        return "run blocks finished"


    # Run workflows concurrently
    results = await asyncio.gather(*[run_blocks(i) for i in range(1024)])

    for result in results:
        print(result)

    await flow.shutdown()

if __name__ == '__main__':
    asyncio.run(main())

import time
import asyncio

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import RadicalExecutionBackend

async def main():
    # Create backend and workflow
    backend = await RadicalExecutionBackend({'resource': 'local.localhost'})
    async with await WorkflowEngine.create(backend=backend) as flow:

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

        async def run_wf(wf_id):
            print(f'\nStarting workflow {wf_id} at {time.time()}')
            t1 = task1()
            t2 = task2(t1)
            t3 = task3(t1, t2)
            t4 = task4(t3)

            if await t4:
                t5 = task5() 
                await t5

            return f'Workflow {wf_id} completed at {time.time()}'

    # Run workflows concurrently
    results = await asyncio.gather(*[run_wf(i) for i in range(1024)])

    for result in results:
        print(result)

if __name__ == '__main__':
    asyncio.run(main())

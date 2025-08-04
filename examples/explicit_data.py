import time
import asyncio

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import InputFile, OutputFile
from radical.asyncflow import RadicalExecutionBackend

async def main():
    backend = await RadicalExecutionBackend({'resource': 'local.localhost'})
    async with await WorkflowEngine.create(backend=backend) as flow:

        @flow.executable_task
        def task1(*args):
            return 'echo "This is a file from task1" > t1_output.txt'

        @flow.executable_task
        def task2(*args):
            return '/bin/cat t1_output.txt'


        t1 = task1(OutputFile('t1_output.txt'))
        t2 = task2(t1, InputFile('t1_output.txt'))

        print(await t2)


if __name__ == '__main__':
    asyncio.run(main())

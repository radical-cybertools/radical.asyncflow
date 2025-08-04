import asyncio

from concurrent.futures import ThreadPoolExecutor

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import DaskExecutionBackend
from radical.asyncflow import RadicalExecutionBackend
from radical.asyncflow import ConcurrentExecutionBackend

async def main(backends):
    for backend, resource in backends.items():
        backend = await backend(resource)
        async with await WorkflowEngine.create(backend=backend) as flow:

            task = flow.executable_task
            if isinstance(backend, DaskExecutionBackend):
                task = flow.function_task

            @task
            async def task1(*args):
                return '/bin/sleep 10'

            @task
            async def task2(*args):
                return '/bin/date'

            @task
            async def task3(*args):
                return '/bin/date'

            t1 = task1()
            t2 = task2()
            t3 = await task3(t1, t2)

            print(t3)

if __name__ == "__main__":
    print('Running 1-layer funnel DAG workflow with each backend\n')
    print("""
         task1      task2 <---- running in parallel 1st
             \\       /
               task3      <---- running last\n""")

    backends= {
        ConcurrentExecutionBackend : ThreadPoolExecutor(max_workers=4),
        RadicalExecutionBackend: {'resource': 'local.localhost'},
        DaskExecutionBackend   : {'n_workers': 2, 'threads_per_worker': 1}
        }

    asyncio.run(main(backends))
import asyncio

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import RadicalExecutionBackend
from radical.asyncflow import DaskExecutionBackend
from radical.asyncflow import ConcurrentExecutionBackend

from concurrent.futures import ThreadPoolExecutor

backends= {ConcurrentExecutionBackend : ThreadPoolExecutor(max_workers=4),
           RadicalExecutionBackend: {'resource': 'local.localhost'},
           DaskExecutionBackend   : {'n_workers': 2, 'threads_per_worker': 1}}


print('Running 1-layer funnel DAG workflow with each backend\n')
print("""
         task1      task2 <---- running in parallel 1st
             \\       /
               task3      <---- running last\n""")

async def main():
    for backend, resource in backends.items():
        backend = await backend(resource)
        flow = await WorkflowEngine.create(backend=backend)

        task = flow.executable_task
        if isinstance(backend, DaskExecutionBackend):
            task = flow.function_task
        
        @task
        async def task1(*args):
            return '/bin/date'

        @task
        async def task2(*args):
            return '/bin/date'

        @task
        async def task3(*args):
            return '/bin/date'

        t3 = task3(task1(), task2())

        print(await t3)

        await flow.shutdown()

if __name__ == "__main__":
    asyncio.run(main())

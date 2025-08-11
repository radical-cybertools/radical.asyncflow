import time
import asyncio
import logging

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import RadicalExecutionBackend
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)

async def main():
    
    init_default_logger(logging.INFO)

    # Create backend and workflow
    backend = await RadicalExecutionBackend({'resource': 'local.localhost'})
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task1(task_description={'ranks':1, 'gpus_per_rank':1}):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.executable_task
    async def task2(task1, task_description={'ranks':1}):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.executable_task
    async def task3(task1, task2, task_description={'gpus_per_rank':1}):
        return '/bin/echo "I got executed at" && /bin/date'

    async def run_wf(wf_id):
        logger.info(f'Starting workflow {wf_id} at {time.time()}')
        t1 = task1()
        t2 = task2(t1)
        t3 = task3(t1, t2)
        t3_result = await t3
        logger.info(f'Workflow {wf_id} finished at {time.time()}')
        return t3_result

    # Run workflows concurrently
    results = await asyncio.gather(*[run_wf(i) for i in range(1)])

    await flow.shutdown()

if __name__ == '__main__':
    asyncio.run(main())

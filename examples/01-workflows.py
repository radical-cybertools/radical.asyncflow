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

    backend = await ConcurrentExecutionBackend(ThreadPoolExecutor())
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def task1(*args):
        # Simulate lightweight data generation (e.g., creating a list of numbers)
        logger.info("Task 1: Generating data")
        await asyncio.sleep(1)  # Reduced sleep to make it lighter
        data = list(range(1000))
        return sum(data)  # Simple computation: sum of numbers

    @flow.function_task
    async def task2(*args):
        # Simulate processing data from task1 (e.g., filtering even numbers)
        input_data = args[0]
        logger.info(f"Task 2: Processing data from Task 1, input sum: {input_data}")
        await asyncio.sleep(1)
        return [x for x in range(1000) if x % 2 == 0]  # Return list of even numbers

    @flow.function_task
    async def task3(*args):
        # Simulate aggregating results from task1 and task2
        sum_data, even_numbers = args
        logger.info(f"Task 3: Aggregating, sum: {sum_data}, even count: {len(even_numbers)}")
        await asyncio.sleep(1)
        return {"total_sum": sum_data, "even_count": len(even_numbers)}  # Aggregate results

    async def run_wf(wf_id):
        logger.info(f'Starting workflow {wf_id} at {time.time()}')
        t1 = task1()
        t2 = task2(t1)
        t3 = task3(t1, t2)
        result = await t3  # Await the final task
        logger.info(f'Workflow {wf_id} completed at {time.time()}, result: {result}')

    # Run workflows concurrently
    results = await asyncio.gather(*[run_wf(i) for i in range(1024)])

    await flow.shutdown()

if __name__ == '__main__':
    asyncio.run(main())

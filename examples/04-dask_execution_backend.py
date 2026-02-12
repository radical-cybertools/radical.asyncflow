import asyncio
import logging

import numpy as np

from radical.asyncflow import DaskExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)
init_default_logger(logging.INFO)


async def main():
    backend = await DaskExecutionBackend({"n_workers": 2, "threads_per_worker": 4})
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def load_dataset(*args):
        logger.info("Loading dataset into shared memory")
        dataset = np.random.rand(10000)  # Simulate loading a large dataset
        return dataset

    @flow.function_task
    async def analyze_first_batch(*args):
        dataset = args[0]
        logger.info("Analyzing first batch of data")
        return np.mean(dataset[:5000])  # Calculate average of first half

    @flow.function_task
    async def analyze_second_batch(*args):
        dataset = args[0]
        logger.info("Analyzing second batch of data")
        return np.mean(dataset[5000:])  # Calculate average of second half

    @flow.function_task
    async def compute_final_stats(*args):
        first_avg, second_avg = args[1], args[2]
        overall_avg = (first_avg + second_avg) / 2
        logger.info(f"Overall average: {overall_avg}")
        return overall_avg

    # Load dataset once, analyze batches in parallel
    shared_data = load_dataset()
    batch1_analysis = analyze_first_batch(shared_data)
    batch2_analysis = analyze_second_batch(shared_data)
    final_stats = compute_final_stats(shared_data, batch1_analysis, batch2_analysis)

    result = await final_stats
    logger.info(f"Analysis complete: {result}")

    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

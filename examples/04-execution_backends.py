import time
import asyncio
import logging
import numpy as np

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import DaskExecutionBackend
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)

async def main():
    init_default_logger(logging.INFO)
    
    backend = await DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 4})
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def load_dataset():
        dataset = np.random.rand(10000)  # Simulate loading a large dataset
        return dataset

    @flow.function_task
    async def analyze_first_batch(dataset):
        return np.mean(dataset[:5000])  # Calculate average of first half

    @flow.function_task
    async def analyze_second_batch(dataset):
        return np.mean(dataset[5000:])  # Calculate average of second half

    @flow.function_task
    async def compute_final_stats(first_avg, second_avg):
        overall_avg = (first_avg + second_avg) / 2
        return overall_avg

    # Load dataset once, analyze batches in parallel
    logger.info("Loading dataset into shared memory")
    shared_data = load_dataset()

    logger.info("Analyzing first batch of data")
    batch1_analysis = analyze_first_batch(shared_data)

    logger.info("Analyzing second batch of data")  
    batch2_analysis = analyze_second_batch(shared_data)

    final_stats = compute_final_stats(batch1_analysis, batch2_analysis)

    result = await final_stats
    logger.info(f"Overall average: {result}")

    await flow.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
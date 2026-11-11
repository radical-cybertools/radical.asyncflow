"""Dragon Execution Backend Example: Executable Task Dependencies.

Demonstrates HPC execution using DragonExecutionBackend from RHAPSODY:
- executable tasks chained into a dependency graph (task1 -> task2 -> task3)
- per-task resources via ``process_template`` / ``process_templates``
- many workflows running concurrently on one backend

Requires: pip install "rhapsody-py[dragon]"
Run with: dragon examples/07-dragon_executable_dependencies.py
RHAPSODY docs: https://radical-cybertools.github.io/rhapsody/
AsyncFlow integration: https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration
"""

import asyncio
import logging
import multiprocessing as mp
import time

from rhapsody.backends import DragonExecutionBackend

from radical.asyncflow import WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    init_default_logger(logging.INFO)

    # Create backend and workflow
    backend = await DragonExecutionBackend()
    flow = await WorkflowEngine.create(backend=backend)

    task1_resources = {"process_template": {}}
    task2_resources = {"process_template": {}}
    task3_resources = {"process_templates": [(2, {})]}

    @flow.executable_task
    async def task1(task_description=task1_resources):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.executable_task
    async def task2(task1, task_description=task2_resources):
        return '/bin/echo "I got executed at" && /bin/date'

    @flow.executable_task
    async def task3(task1, task2, task_description=task3_resources):
        return '/bin/echo "I got executed at" && /bin/date'

    async def run_wf(wf_id):
        logger.info(f"Starting workflow {wf_id} at {time.time()}")
        t1 = task1()
        t2 = task2(t1)
        t3 = task3(t1, t2)
        t3_result = await t3
        logger.info(f"Workflow {wf_id} finished at {time.time()}")
        return t3_result

    # Run workflows concurrently
    try:
        await asyncio.gather(*[run_wf(i) for i in range(10)])
    finally:
        await flow.shutdown()


if __name__ == "__main__":
    # Set Dragon as multiprocessing backend
    mp.set_start_method("dragon")
    asyncio.run(main())

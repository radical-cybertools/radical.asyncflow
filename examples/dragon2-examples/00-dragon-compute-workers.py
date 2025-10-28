import asyncio
import logging
import time

from radical.asyncflow import DragonExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger
from radical.asyncflow.backends.execution.dragon import WorkerGroupConfig, WorkerType, PolicyConfig
logger = logging.getLogger(__name__)


async def main():
    import dragon
    import multiprocessing as mp
    from dragon.infrastructure.policy import Policy
    
    # This will allow asyncflow and dragon to see all nodes
    mp.set_start_method("dragon")

    # We have 4 nodes
    policy_n0 = Policy(host_id=0, distribution=Policy.Distribution.BLOCK)
    policy_n1 = Policy(host_id=1, distribution=Policy.Distribution.BLOCK)
    policy_n2 = Policy(host_id=2, distribution=Policy.Distribution.BLOCK)
    policy_n3 = Policy(host_id=3, distribution=Policy.Distribution.BLOCK)

    def get_one_node_per_worker(): # total is 4 workers

        w0 = WorkerGroupConfig(name="worker0",
                            worker_type=WorkerType.COMPUTE,
                            policies=[PolicyConfig(policy=policy_n0, nprocs=128),
                                        ]) # worker 0 will occupy 1 nodes

        w1 = WorkerGroupConfig(name="worker1",
                            worker_type=WorkerType.COMPUTE,
                            policies=[PolicyConfig(policy=policy_n1, nprocs=128),
                                        ]) # worker 1 will occupy 1 nodes

        w2 = WorkerGroupConfig(name="worker2",
                            worker_type=WorkerType.COMPUTE,
                            policies=[PolicyConfig(policy=policy_n2, nprocs=128),
                                        ]) # worker 2 will occupy 1 nodes

        w3 = WorkerGroupConfig(name="worker3",
                            worker_type=WorkerType.COMPUTE,
                            policies=[PolicyConfig(policy=policy_n3, nprocs=128),
                                        ]) # worker 3 will occupy 1 nodes
        
        return [w0, w1, w2, w3]


    def get_two_nodes_per_worker(): # total is 2 workers

        w0 = WorkerGroupConfig(name="worker0",
                               worker_type=WorkerType.COMPUTE,
                               policies=[PolicyConfig(policy=policy_n0, nprocs=128),
                                         PolicyConfig(policy=policy_n1, nprocs=128)]) # worker 0 will occupy 2 nodes

        w1 = WorkerGroupConfig(name="worker1",
                               worker_type=WorkerType.COMPUTE,
                               policies=[PolicyConfig(policy=policy_n2, nprocs=128),
                                         PolicyConfig(policy=policy_n3, nprocs=128)]) # worker 1 will occupy 2 nodes

        return [w0, w1]


    # Optional: do not specify anything here and Dragon/Asyncflow will build worker=total number of slots
    resources = {"workers": get_one_node_per_worker()} 
    backend = await DragonExecutionBackend(resources)
    init_default_logger(logging.INFO)
    flow = await WorkflowEngine.create(backend=backend)

    task1_resources = {"ranks": 1}

    @flow.function_task
    async def task1(task_description=task1_resources):
        #import os
        #return os.environ['DRAGON_WORKER_NAME']
        return

    async def run_wf(wf_id):
        logger.info(f"Starting workflow {wf_id} at {time.time()}")
        t1 = task1()
        t1_result = await t1
        logger.info(f"Workflow {wf_id} finished at {time.time()}")
        logger.info(t1_result)

    # Run workflows concurrently
    TASKS = 100000
    x = time.time()
    results = await asyncio.gather(*[task1() for i in range(TASKS)])
    y = time.time()

    print(f'TTX: {y-x}s, THP: {TASKS/(y-x)}tasks/s')
    print(results)
    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

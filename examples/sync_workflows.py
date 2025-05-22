import time

from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionBackend

backend = RadicalExecutionBackend({'resource': 'local.localhost'})
flow = WorkflowEngine(backend=backend)

@flow.executable_task
def task1(*args):
    return '/bin/echo $RP_TASK_NAME'

@flow.executable_task
def task2(*args):
    return '/bin/echo $RP_TASK_NAME'

@flow.executable_task
def task3(*args):
    return '/bin/echo $RP_TASK_NAME'


def run_wf(wf_id):

    print(f'Starting workflow {wf_id} at {time.time()}')
    t3 = task3(task1(), task2())
    print(t3.result())
    print(f'Workflow {wf_id} completed at {time.time()}')

for i in range(5):
    run_wf(i)

backend.shutdown()

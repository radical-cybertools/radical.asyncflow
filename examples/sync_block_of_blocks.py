import time

from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionBackend


backend = RadicalExecutionBackend({'resource': 'local.localhost'})
flow = WorkflowEngine(backend=backend)

@flow.executable_task
def task1(*args):
    return '/bin/echo "I got executed at" && /bin/date'

@flow.executable_task
def task2(*args):
    return '/bin/echo "I got executed at" && /bin/date'


@flow.block
def block1(*args):
    t1 = task1()
    t2 = task2(t1)
    t2.result()
    print(f'block1 done at {time.time()}')

@flow.block
def block2(*args):
    t3 = task1()
    t4 = task2(t3)
    t4.result()
    print(f'block2 done at {time.time()}')


@flow.block
def block1_of_blocks(*args):
    b1 = block1()
    b2 = block2(b1)
    b2.result()
    print(f'block_of_blocks done at {time.time()}')


@flow.block
def block2_of_blocks(*args):
    b1 = block1()
    b2 = block2(b1)
    b2.result()
    print(f'block_of_blocks done at {time.time()}')

bob1 = block1_of_blocks()
bob2 = block2_of_blocks(bob1)
bob2.result()

flow.shutdown()

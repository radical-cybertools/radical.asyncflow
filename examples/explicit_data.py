import time

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import InputFile, OutputFile
from radical.asyncflow import RadicalExecutionBackend

backend = RadicalExecutionBackend({'resource': 'local.localhost'})

with WorkflowEngine(backend=backend) as flow:
    @flow.executable_task
    def task1(*args):
        return 'echo "This is a file from task1" > t1_output.txt'

    @flow.executable_task
    def task2(*args):
        return '/bin/cat t1_output.txt'


    t1 = task1(OutputFile('t1_output.txt'))
    t2 = task2(t1, InputFile('t1_output.txt'))

    print(t2.result())


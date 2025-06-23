RADICAL AsyncFLow (RAF) is a synchronous and asynchronous workflow management layer. RAF supports the management and execution of tasks, set of tasks with dependencies (workflows), sets of workflows with dependencies on other workflows (blocks). RAF flows
the best practice of Python by enabling asynchronous behavior on the task, workflow, and blocks with adaptive execution behavior.
RAF supports different execution backends such is `Radical.Pilot`,
`Dask.Parallel` and more. RAF is agnostic to these execution backends meaning the user can easily extend it with their own custom execution mechanism.


## Basic Usage (sync)
```python
from radical.asyncflow import WorkflowManager
from radical.asyncflow import RadicalExecutionBackend

radical_backend = RadicalExecutionBackend({'resource': 'local.localhost'})
flow = WorkflowManager(backend=radical_backend)

@flow.executable_task
def task1():
    return "echo $RANDOM"

@flow.function_task
def task2(t1_result):
    return t1_result * 2 * 2


# create the workflow
t1_result = task1().result()
t2_future = task2(t1_result) # t2 depends on t1 (waits for it)

t2_result = t2_future.result()

# shutdown the execution backend
flow.shutdown()
```
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import RadicalExecutionBackend

backend = await RadicalExecutionBackend({'resource': 'local.localhost'})
flow = await WorkflowEngine.create(backend=backend)

@flow.executable_task
def task1(*args):
    return '/bin/echo $RP_TASK_NAME'

@flow.executable_task
def task2(*args):
    return '/bin/echo $RP_TASK_NAME'

@flow.executable_task
def task3(*args):
    return '/bin/echo $RP_TASK_NAME'

# ====================================================
# Workflow-1: 1-layer funnel DAG
print('Running 1-layer funnel DAG workflow\n')
print("Shape:")
print("""
  task1      task2 <---- running in parallel
     \\       /
       task3
""")
t3 = task3(task1(), task2())

print(t3.result())

# ====================================================
# Workflow-2: 2-layer funnel DAG
print('Running 2-layer funnel DAG workflow\n')
print("Shape:")
print("""
   task1      task2 <---- running in parallel
     |          |
   task2      task1 <---- running in parallel
     \\        /
       task3
""")
t3 = task3(task2(task1()), task1(task2()))
print(t3.result())


# ====================================================
# Workflow-3: Sequential Pipelines (Repeated Twice)
print('Running sequential pipelines\n')
print("Shape:")
print("""
   task1
     | 
   task2
     | 
   task3
   -------
   task1
     |
   task2
     |
   task3
""")
res = []
for i in range(2):
    t3 = task3(task2(task1()))
    print(t3.result())

# ====================================================
# Workflow-4: Concurrent Pipelines
print('Running concurrent pipelines\n')
print("Shape:")
print("""

   task1          task1
      |             |
   task2          task2
      |             |
   task3          task3
""")
res = []
for i in range(2):
    t3 = task3(task2(task1()))
    res.append(t3)

print([t.result() for t in res])

flow.shutdown()


# Getting Started: Run a Single Workflow with Task Dependencies

This guide walks you through running a **single workflow with task dependencies** using `radical.asyncflow`.

You’ll learn how to define tasks, set dependencies, execute the workflow, and shut down the engine gracefully.

---

## 📄 Prerequisites

- Make sure you have installed `radical.asyncflow` in your Python environment.  
- You also need a working Jupyter Notebook or Python >=3.8.

---

## Configure the environment

You need to set an environment variable to enable async mode in Jupyter.

```bash
%env FLOW_JUPYTER_ASYNC=TRUE
```

**💡 Tip:** If running outside Jupyter, you don’t need `FLOW_JUPYTER_ASYNC` to be set.

---

## Import the necessary modules

You’ll need `time`, `asyncio`, and the key classes from `radical.asyncflow`.

```python
import time
import asyncio

from radical.asyncflow import WorkflowEngine, ThreadExecutionEngine
```

---

## Set up the workflow engine

We initialize the workflow engine with a `ThreadExecutionEngine` using Python’s `ThreadPoolExecutor`.

```python
backend = ThreadExecutionEngine({'max_workers': 3})
flow = WorkflowEngine(backend=backend)
```

---

## Define the tasks

Define your tasks and specify dependencies by passing other tasks as arguments.

```python
@flow.function_task
async def task1(*args):
    print(f"Running task1 at {time.time()}")
    await asyncio.sleep(1)
    return time.time()

@flow.function_task
async def task2(*args):
    print(f"Running task2 at {time.time()}")
    await asyncio.sleep(1)
    return time.time()

@flow.function_task
async def task3(t1_result, t2_result):
    print(f"Running task3 after task1 and task2 at {time.time()}")
    await asyncio.sleep(1)
    return time.time()
```

**📌 Note:**  
- `task3` depends on the outputs of `task1` and `task2`.
- You express this dependency by calling `task3(task1(), task2())`.

---

## Run the workflow

We now submit the tasks with their dependencies and wait for the final task to complete.

```python
async def main():
    start_time = time.time()

    # Define the task graph
    t1 = task1()
    t2 = task2()
    t3 = task3(t1, t2)

    # Wait for the final task to complete
    await t3

    end_time = time.time()
    print(f"\nWorkflow completed in: {end_time - start_time:.2f} seconds")

    # Shutdown the workflow engine
    await flow.shutdown()

asyncio.run(main())
```

---

## Example Output

Here’s an example of the output you might see:

???+ success "Show Output Log"
    ```
    Running task1 at 1721847632.3501
    Running task2 at 1721847632.3505
    Running task3 after task1 and task2 at 1721847633.3534

    Workflow completed in: 0.02 seconds
    ```

---

!!! warning 

Make sure to **await the shutdown** of the `WorkflowEngine` before your script exits. Otherwise, resources may leak.

---

## Summary
You now know how to:

- Define a set of tasks with dependencies.  
- Submit them to the workflow engine.  
- Run the workflow asynchronously.  
- Shut down the engine properly.

---

## Next Steps

- Learn how to run multiple workflows concurrently.
- Explore advanced backends like `DaskExecutionBackend` or `RadicalExecutionBackend`.
- Integrate with HPC schedulers.

---

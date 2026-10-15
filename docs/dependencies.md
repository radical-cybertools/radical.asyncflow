
# Task Dependencies

AsyncFlow builds the workflow graph from the way you call your tasks: when a task receives the future of another task as an argument, it waits for that task to finish and receives its result.

This page explains how to express dependencies and exactly what AsyncFlow considers a dependency.

---

## How to specify a dependency

Pass the future returned by a task (or block) call **directly** as an argument to another task, either **positionally** or as a **keyword argument**:

```python
import asyncio

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine

async def main():
    backend = await LocalExecutionBackend()
    flow = await WorkflowEngine.create(backend=backend)

    @flow.function_task
    async def load(name):
        return f"data:{name}"

    @flow.function_task
    async def merge(left, right, config=None):
        return f"{left}+{right} ({config})"

    a = load("a")
    b = load("b")
    cfg = load("cfg")

    # `a` and `b` are positional dependencies, `cfg` is a keyword dependency.
    result = merge(a, b, config=cfg)

    print(await result)
    await flow.shutdown()

asyncio.run(main())
```

`merge` is submitted only after `load("a")`, `load("b")` and `load("cfg")` have completed, and it receives their **results**, not the futures.

!!! tip
    Tasks that don't depend on each other run in parallel automatically. In the example above, the three `load` calls run concurrently.

---

## What counts as a dependency

A value is treated as a dependency when **all** of the following hold:

- It is a future returned by calling a task or block of **this** engine:
  `@flow.function_task`, `@flow.executable_task`, `@flow.prompt_task` or `@flow.block`.
- It is passed **directly** as a positional argument or as the value of a keyword argument.

The same future passed more than once is **one** dependency, and every position receives the result:

```python
@flow.function_task
async def load_number():
    return 21

@flow.function_task
async def add(x, y):
    return x + y

@flow.function_task
async def scale(value, factor=None):
    return value * factor

x = load_number()
total = add(x, x)           # one dependency on `x`, both arguments get its result
scaled = scale(x, factor=x) # same: positional and keyword uses of `x` are one dependency
```

---

## What does not count as a dependency

| Passed as | Tracked? | What happens |
|---|---|---|
| Plain values (`42`, `"text"`, objects) | No | Passed through unchanged. |
| Futures **nested inside containers**: `f([a, b])`, `f(cfg={"x": a})` | **No** | The task does not wait for them and receives the raw future objects. Pass each future as its own argument instead. |
| `asyncio.Future`s or tasks **not** created by the engine | No | They are not part of the workflow graph. |
| Futures awaited **inside** a task body | No | They don't order scheduling; awaiting just blocks that task while it runs. |

!!! warning
    Only top-level arguments are inspected. If you need several upstream results, pass them as separate arguments (or keyword arguments), not inside a list or dict.

---

## What the dependent task receives

- **Function tasks and blocks** receive the **resolved result** of each dependency in place of its future.
- **Executable and prompt tasks** run their decorated function at registration time to build the command or prompt, so that function sees the **unresolved futures**. The dependency still orders execution (the command runs after its dependencies complete), but the command string cannot embed upstream results.

---

## When a dependency fails or is cancelled

- If a dependency **fails**, the dependent task is not executed and its future raises a `DependencyFailureError`. Its `failed_dependencies` attribute lists the names of the failed dependencies, and `root_cause` holds the original exception:

```python
from radical.asyncflow.errors import DependencyFailureError

try:
    await merge(a, b, config=cfg)
except DependencyFailureError as e:
    print(e.failed_dependencies, repr(e.root_cause))
```

- If a dependency is **cancelled**, the dependent task is cancelled as well.

---

## Data (file) dependencies

Besides task results, tasks can declare **files** they consume and produce by passing `InputFile` and `OutputFile` objects as arguments. With `implicit_data=True` (the default of `WorkflowEngine.create`), AsyncFlow also asks the execution backend to link data between dependent tasks implicitly. How files are staged is specific to each execution backend; see [Execution Backends & HPC Integration](exec_backends.md).

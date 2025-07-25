# RADICAL AsyncFlow (RAF)

**RADICAL AsyncFlow (RAF)** is a fast asynchronous scripting library built on top of [asyncio](https://docs.python.org/3/library/asyncio.html) for building powerful async/sync workflows on HPC, clusters, and local machines. It supports pluggable execution backends with intuitive task dependencies and workflow composition. 

- ⚡ **Powerful asynchronous workflows** — Compose complex async and sync workflows easily, with intuitive task dependencies.

- 🌐 **Portable across environments** — Run seamlessly on HPC systems, clusters, and local machines with pluggable execution backends.

- 🧩 **Flexible and extensible** — Supports campaign management and advanced workflow patterns.

Currently, RAF supports the following execution backends:

- [Radical.Pilot](https://radicalpilot.readthedocs.io/en/stable/#)
- [Dask.Parallel](https://docs.dask.org/en/stable/)
- [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor)
- Noop with `dry_run`
- Custom implementations

---

## Basic Usage

```python
import asyncio

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ThreadExecutionBackend

async def run():
    # Create backend and workflow
    backend = ThreadExecutionBackend({})
    flow = WorkflowEngine(backend=backend)

    @flow.executable_task
    async def task1():
        return "echo $RANDOM"

    @flow.function_task
    async def task2(t1_result):
        return int(t1_result.strip()) * 2 * 2

    # create the workflow
    t1_result = await task1()
    t2_result = await task2(t1_result) # t2 depends on t1 (waits for it)

    # shutdown the execution backend
    await flow.shutdown()

if __name__ == "__main__":
    asyncio.run(run())
```

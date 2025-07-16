# RADICAL AsyncFlow (RAF)

[![License: MIT](https://img.shields.io/badge/License-MIT-gre.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

**RADICAL AsyncFlow (RAF)** is a fast asynchronous scripting library built on top of [asyncio](https://docs.python.org/3/library/asyncio.html) for complex async/sync workflows on HPC, clusters, and local machines.  

It supports pluggable execution backends with intuitive task dependencies and workflow composition.  

Currently, RAF supports the following execution backends:

- [Radical.Pilot](https://radicalpilot.readthedocs.io/en/stable/#)
- [Dask.Parallel](https://docs.dask.org/en/stable/)
- [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor)
- Noop with `dry_run`
- Custom implementations

---

## 🚀 Basic Usage

```python
import asyncio

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import RadicalExecutionBackend

async def run():
    # Create backend and workflow
    backend = RadicalExecutionBackend({'resource': 'local.localhost'})
    flow = WorkflowEngine(backend=backend)

    @flow.executable_task
    async def task1():
        return "echo $RANDOM"

    @flow.function_task
    async def task2(t1_result):
        return t1_result * 2 * 2

    # create the workflow
    t1_result = await task1()
    t2_result = await task2(t1_result)  # t2 depends on t1 (waits for it)

    # shutdown the execution backend
    await flow.shutdown()

if __name__ == "__main__":
    asyncio.run(run())
```

# RADICAL AsyncFlow (RAF)

**RADICAL AsyncFlow (RAF)** is a fast asynchronous scripting library built on top of [asyncio](https://docs.python.org/3/library/asyncio.html) for building powerful asynchronous workflows on HPC, clusters, and local machines. It supports pluggable execution backends with intuitive task dependencies and workflow composition.

- ⚡ **Powerful asynchronous workflows** — Compose complex async and sync workflows easily, with intuitive task dependencies.

- 🌐 **Portable across environments** — Run seamlessly on HPC systems, clusters, and local machines with pluggable execution backends.

- 🧩 **Flexible and extensible** — Supports composite workflows management.

AsyncFlow ships with the following **built-in** execution backends:

- `LocalExecutionBackend` — local execution using Python's [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) (ThreadPoolExecutor / ProcessPoolExecutor)
- `NoopExecutionBackend` — no-op backend for testing and `dry_run` mode

For **HPC execution**, install [RHAPSODY](https://radical-cybertools.github.io/rhapsody/) (`pip install rhapsody-py`) which provides additional backends — see [Execution Backends & HPC Integration](exec_backends.md) for the full guide:

- [Radical.Pilot](https://radicalpilot.readthedocs.io/en/stable/#) — distributed HPC execution
- [Dask](https://docs.dask.org/en/stable/) — parallel computing with Dask distributed
- Concurrent — extended thread/process pool execution
- Dragon — high-performance distributed execution

---

Basic Usage

```python
import asyncio

from concurrent.futures import ThreadPoolExecutor
from radical.asyncflow import WorkflowEngine, LocalExecutionBackend

async def run():
    # Create backend and workflow
    backend = await LocalExecutionBackend(ThreadPoolExecutor(max_workers=3))
    flow = await WorkflowEngine.create(backend=backend)

    @flow.executable_task
    async def task1():
        return "echo $RANDOM"

    @flow.function_task
    async def task2(t1_result):
        return int(t1_result.strip()) * 2 * 2

    # create the workflow
    t1_fut = task1()
    t2_result = await task2(t1_fut) # t2 depends on t1 (waits for it)

    # shutdown the execution backend
    await flow.shutdown()

if __name__ == "__main__":
    asyncio.run(run())
```

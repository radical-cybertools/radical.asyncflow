# Multi-Backend Routing & AI Tasks

AsyncFlow supports registering **multiple named backends** on a single `WorkflowEngine` and routing individual tasks to specific backends using a single decorator parameter. This enables hybrid workflows that mix compute tasks (HPC, threads) with AI inference tasks in the same DAG.

---

## The `prompt_task` Decorator

`prompt_task` is a first-class task type alongside `function_task` and `executable_task`. Instead of executing a Python function, it:

1. Calls the decorated async function to **build a prompt string**.
2. Forwards that string to the registered AI backend for inference.
3. Returns the backend's response as the task result.

### Defining a `prompt_task`

```python
@flow.prompt_task
async def summarize(text):
    return f"Summarize the following in one sentence:\n\n{text}"
```

The decorated function **must return a non-empty string**. AsyncFlow validates this before submitting to the backend — any other return value (`None`, `""`, a number) raises a `ValueError` immediately.

### Task types at a glance

| Decorator | Function body does… | Backend receives… |
|-----------|--------------------|--------------------|
| `@flow.function_task` | Runs as a Python callable | Return value of the function |
| `@flow.executable_task` | Returns a shell command string | Command is executed on the backend |
| `@flow.prompt_task` | Returns a prompt string | Prompt is forwarded to an AI backend |

---

## Registering Multiple Backends

Pass a **list of pre-initialized backends** to `WorkflowEngine.create()`. Each backend must have a `.name` attribute set at construction time.

```python
from concurrent.futures import ThreadPoolExecutor
from radical.asyncflow import LocalExecutionBackend, WorkflowEngine

# HPC / AI backends from RHAPSODY — install with: pip install rhapsody-py
# from rhapsody.backends import DragonVllmInferenceBackend

compute_backend = await LocalExecutionBackend(ThreadPoolExecutor(), name="compute")
ai_backend      = NoopExecutionBackend(name="ai")  # replace with a real AI backend

flow = await WorkflowEngine.create(backend=[compute_backend, ai_backend])
```

**Rules:**

- The **first backend in the list** is the default. Tasks without an explicit `backend=` parameter are sent there.
- Backend names must be unique.
- `WorkflowEngine.create(backend=single_backend)` still works exactly as before — passing a single backend is fully backward compatible.

### The `backend` property

`flow.backend` always returns the default backend (the first one in the list):

```python
assert flow.backend is compute_backend  # True
```

---

## Per-Task Backend Routing

Add `backend="<name>"` to any task decorator to route that task to a specific registered backend:

```python
@flow.function_task(backend="compute")
async def preprocess(raw_text):
    return raw_text.strip().lower()

@flow.prompt_task(backend="ai")
async def summarize(preprocessed):
    return f"Summarize this:\n\n{preprocessed}"

@flow.function_task(backend="compute")
async def postprocess(summary):
    return f"[Result] {summary}"
```

Tasks **without** `backend=` fall back to the default backend. Routing is entirely opt-in — existing workflows with a single backend are unaffected.

!!! note
    Routing to an unregistered backend name causes the task future to fail with a `KeyError`. Always verify that the backend name passed to the decorator matches the `.name` attribute used when the backend was constructed.

---

## Complete Example

The example below runs five document-processing pipelines concurrently. Each pipeline mixes CPU-bound preprocessing and postprocessing (routed to `"compute"`) with an AI summarisation step (routed to `"ai"`).

```python
import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from radical.asyncflow import LocalExecutionBackend, NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

# Replace NoopExecutionBackend with a real AI backend in production:
#   from rhapsody.backends import DragonVllmInferenceBackend
#   ai_backend = DragonVllmInferenceBackend(name="ai", model="llama-3")


async def main():
    init_default_logger(logging.INFO)

    compute_backend = await LocalExecutionBackend(ThreadPoolExecutor(), name="compute")
    ai_backend      = NoopExecutionBackend(name="ai")

    flow = await WorkflowEngine.create(backend=[compute_backend, ai_backend])

    @flow.function_task(backend="compute")
    async def preprocess(raw_text):
        return raw_text.strip().lower()

    @flow.prompt_task(backend="ai")
    async def summarize(preprocessed):
        return f"Summarize the following text in one sentence:\n\n{preprocessed}"

    @flow.function_task(backend="compute")
    async def postprocess(summary):
        return f"[Result] {summary}"

    documents = [
        "AsyncFlow is an async-first workflow engine built on Python asyncio.",
        "RHAPSODY provides HPC and AI execution backends for AsyncFlow.",
        "Tasks can be routed to named backends using the backend= decorator parameter.",
        "prompt_task bridges workflow orchestration and AI inference.",
        "Multi-backend routing lets compute and AI tasks run side by side.",
    ]

    async def run_pipeline(doc_id: int, text: str):
        start = time.time()
        preprocessed = preprocess(text)
        summary      = summarize(preprocessed)
        result       = await postprocess(summary)
        print(f"doc {doc_id} finished in {time.time() - start:.2f}s")
        return result

    results = await asyncio.gather(
        *[run_pipeline(i, doc) for i, doc in enumerate(documents)]
    )

    print(f"All {len(results)} documents processed.")
    await flow.shutdown()


asyncio.run(main())
```

The full runnable script is available at [`examples/04-concurrent_backends.py`](https://github.com/radical-cybertools/radical.asyncflow/blob/main/examples/04-concurrent_backends.py).

---

## Performance Design

AsyncFlow avoids routing overhead on the hot path:

- When **no task** in a submission batch has a `backend=` target, all tasks are forwarded to the default backend directly — **zero overhead**.
- When at least one task carries a `backend=` target, the engine groups tasks by destination and submits to each backend **concurrently** via `asyncio.gather`, so multiple backends are never submitted sequentially.

---

## Using Real AI Backends (RHAPSODY)

For production AI workloads, replace `NoopExecutionBackend` with a RHAPSODY AI backend:

```bash
pip install rhapsody-py
```

```python
from rhapsody.backends import DragonVllmInferenceBackend

ai_backend = DragonVllmInferenceBackend(name="ai", model="llama-3")
```

!!! note
    `LocalExecutionBackend` does not support `prompt_task`. If you route a `prompt_task` to a `LocalExecutionBackend`, the task will fail with a `NotImplementedError` directing you to register an AI backend.

See the [AsyncFlow integration guide](https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration) for details on all available RHAPSODY backends.

---

## Shutdown

`flow.shutdown()` drains **all** registered backends concurrently. No extra calls needed.

```python
await flow.shutdown()  # shuts down compute + ai backends in parallel
```

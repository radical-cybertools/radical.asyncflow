
# Best Practices for AsyncFlow

AsyncFlow is built on top of Python’s `asyncio`, combining asynchronous execution and task dependency management with a simple API.
This page outlines **recommended practices** when using AsyncFlow effectively in your projects.

---

## Why Best Practices Matter

Async programming can easily lead to:

- Hard-to-debug concurrency bugs 🚨
- Deadlocks or race conditions 💥
- Inefficient task scheduling ⚠️

By following these best practices, you can:

- Make your workflows reliable 🏅
- Maximize concurrency 🔥
- Keep code maintainable 🔧

---

## Structure Your Tasks Clearly

- Define **small, composable tasks** that each do one thing well.
- Prefer pure functions or side-effect-free coroutines as tasks.
- Use `@flow.function_task` or `@flow.executable_task` decorators consistently.

!!! tip
    Name your tasks clearly to improve logs and debugging
    ```python
    @flow.function_task
    async def fetch_data(url):
        ...
    ```
---

## Inspect Task and Block State

Every future returned by a task or block call exposes a `.state` attribute that tracks its lifecycle:

| State | Meaning |
|---|---|
| `"PENDING"` | Registered; waiting for dependencies to resolve |
| `"RUNNING"` | Submitted to the backend and executing |
| `"DONE"` | Completed successfully |
| `"FAILED"` | Raised an exception |
| `"CANCELLED"` | Cancelled before or during execution |

```python
fut = my_task()
print(fut.state)        # "PENDING" — not yet submitted

await asyncio.sleep(0)
print(fut.state)        # "RUNNING" or "DONE" depending on backend speed

await fut
print(fut.state)        # "DONE", "FAILED", or "CANCELLED"
```

!!! tip
    Prefer reading `.state` over combining `fut.done()` + `fut.exception()` — `.state` is explicit
    about which terminal outcome occurred without a separate branch.

---

## React to Task Completion Without Background Coroutines

A common mistake is spawning one background coroutine per task to monitor its completion:

```python
# ❌ Anti-pattern: one coroutine per task
async def watch(fut):
    await fut
    handle(fut.result())

for fut in futs:
    asyncio.create_task(watch(fut))   # N coroutines for N tasks
```

Use `add_done_callback` instead — one callback function handles all completions, zero extra coroutines:

```python
# ✅ Correct: zero coroutines, purely event-driven
def on_done(fut):
    if fut.cancelled():
        print(f"cancelled — state={fut.state}")
    elif fut.exception():
        print(f"failed: {fut.exception()} — state={fut.state}")
    else:
        print(f"done: {fut.result()} — state={fut.state}")

for fut in [task1(), task2(), task3()]:
    fut.add_done_callback(on_done)
```

!!! note
    The callback fires synchronously from within the event loop when the future settles.
    Keep it fast — offload heavy work to a queue or schedule it with `asyncio.create_task`.
    `fut.state` is already set before the callback fires, so it is safe to read inside.

---

## Use Dependencies Correctly

Tasks can depend on the output of other tasks:
- Make your dependency graph explicit by passing tasks as arguments.
- Don’t block unnecessarily: let AsyncFlow schedule dependencies.

!!! tip
    Tasks that don’t depend on each other run in parallel automatically.

```python
asyncflow = await WorkflowEngine.create(dry_run=True)

async def task_a():
    asyncio.sleep(2) # (1)!

async def task_a():
    asyncio.sleep(2) # (2)!

async def task_c(task_a_fut, task_b_fut):
    asyncio.sleep(2)


result = task_c(task_a(), task_b()) # (3)!
```

1. Task A will run asynchronously (independently)
2. Task B will run asynchronously (independently)
3. Task C will wait implicitly for other tasks

---

## Await Only at the Top Level

- Inside your workflow logic, don’t await intermediate tasks.
- Let AsyncFlow build the graph; only await the final or root tasks you care about.
- Awaiting early forces serialization and kills concurrency.

!!! warning
    Avoid this as it will be slower:

```python
await task_a()
await task_b()
```
Instead 1 await is faster 🔥:

```python
result = await task_c(task_a(), task_b())
```

---

## Graceful Shutdown

Always shut down the flow explicitly when finished:
- Releases resources (e.g., thread pools, processes).
- Ensures a clean exit.

At the end of your async main:

```python
await flow.shutdown()
```

---

## Logging & Debugging

Enable detailed logs to diagnose issues:
```bash
export RADICAL_LOG_LVL=DEBUG
```

Logs show task dependencies, execution order, errors.

---

## Clean Shutdown

- Use `try`/`finally` in your async main to ensure `flow.shutdown()` is always called, even on exceptions.

---

!!! success
    - Define tasks clearly and concisely.
    - Pass tasks as arguments to express dependencies.
    - Only await at the top level.
    - Shut down cleanly.
    - Log at `DEBUG` level when needed.

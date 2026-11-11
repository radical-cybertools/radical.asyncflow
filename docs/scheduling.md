# Scheduling Policies

Without a policy, the engine hands every ready task to the backend at once, and the
backend's own queue decides which task runs next. A **ready-queue policy** takes that
decision back into AsyncFlow: the engine holds ready tasks in the policy, and submits
the tasks the policy dispatches. The policy decides how many tasks are in flight and
which ready task starts next.

```python
from radical.asyncflow import FifoPolicy, WorkflowEngine

flow = await WorkflowEngine.create(backend=backend, policy=FifoPolicy(64))
```

Policies are opt-in. Without one, nothing changes.

---

## What goes through a policy

- **Plain tasks** (function, executable and prompt tasks) wait in the policy until it
  dispatches them.
- **Blocks** run inside the engine, and **services** are long-lived, so both bypass the
  policy.
- A task holds its place in the policy from the moment it is dispatched until the
  backend reports it done, failed or cancelled.

## Capacity

Policies that count tasks take a `capacity`: the most tasks in flight at once. Set it to the number of tasks the backend runs at once, for example
its number of workers. A smaller capacity leaves workers idle; a larger one hands the
order back to the backend's queue.

## Priorities

A task may declare a priority, a number where larger runs sooner. The default is 0.

```python
@flow.function_task
async def train(data): ...


@flow.function_task
async def simulate(step): ...


model = train(data, task_description={"priority": 10})  # AI tasks first
sims = [simulate(i) for i in range(1000)]  # priority 0
```

`FifoPolicy` starts tasks by priority, then in the order they became ready. A priority
that is not a finite number fails the task with a `ValueError`. Backends that read the
same key, such as Dask's, give it the same meaning.

## Cancellation

- A task cancelled while the policy holds it leaves the policy at once, and its
  dependents are cancelled as without a policy.
- A task cancelled after it was dispatched keeps its place until the backend reports
  it: the backend may still be running it.

## FifoPolicy

`FifoPolicy(capacity)` starts tasks by priority, then in the order they became ready,
at most `capacity` at a time. Without priorities it is first in, first out.

## FairPolicy

`FairPolicy(capacity, weights=None)` shares the backend between workflows, in the style
of Linux's CFS. A workflow is the scope the task was called in (`workflow_scope`, or
a `workflow_id` given at the call), also inside blocks, or else its lineage. Each
workflow accrues virtual runtime, the time its tasks ran divided by its weight, and the
workflow with the least goes next. A workflow with nothing queued that gets new work
starts at no less than the current minimum, so it banks no credit while it has nothing
to run; one that ran ahead keeps its debt while it is idle. A task that ends sooner
than estimated, or never runs, gives no credit below the minimum. Within a workflow, tasks start by priority, then in the order
they became ready.

```python
policy = FairPolicy(64, weights={"interactive": 4})

async with flow.workflow_scope("interactive"):
    answer = await analyse(sample)  # a fourfold share beside the bulk work
```

A weight's key also matches the ids that start with it and a `":"`, so `"sim"` weighs
`"sim:0042"`; the longest matching key wins. Runtimes are measured from dispatch until
the backend reports the task, and estimated per function from the tasks of that function
that finished. The estimate is one number per function: when a function's runtimes vary a
lot, a workflow of short chains gets somewhat less than its share (0.41 instead of 0.5
in a simulation where one task in ten runs 100 times longer).

Tasks called outside any scope are grouped by lineage, so each independent one is a
workflow of its own, and work that shares one dependency (data loaded once and passed
to every workflow) is a single workflow. Call related work in a `workflow_scope` to
say what a workflow is.

## Writing a policy

Any object with these four methods works as a policy; one that lacks any of them is
rejected when the engine is created.

```python
class Policy(Protocol):
    def enqueue(self, uid: str, desc: dict) -> None: ...
    def dispatch(self) -> list[dict]: ...
    def release(
        self, uid: str, state: str | None, observed: dict | None = None
    ) -> None: ...
    def runnable(self) -> bool: ...
```

- `enqueue(uid, desc)`: a task is ready. Raise to reject it: the engine fails the task
  with that exception, and its dependents with a `DependencyFailureError`.
- `dispatch()`: return the descriptions of the tasks to submit now. The engine calls it
  again after it returned tasks, or after a task left the policy.
- `release(uid, state, observed)`: a task leaves the policy. `state` is how it ended
  on the backend, spelled as `future.state` spells it (`"DONE"`, `"FAILED"` or
  `"CANCELLED"`), and `observed` is the resource use the backend reported for it, if
  any. `state` is None for a task the backend never got: it was cancelled while the
  policy held it, or before the engine submitted it; and for the tasks of an engine
  that nothing reports to any more. When the engine shuts down, a task still in flight
  is released as `"CANCELLED"`. Ignore uids that are unknown or
  already released.
- `runnable()`: return True if `dispatch()` may return a task now, False if it would
  not. The engine sleeps while it is False, until a task is enqueued or released.

The engine calls these methods on its event-loop thread, so a policy needs no locking.
A description carries `_seq`, the task's registration order; `_lineage`, the `_seq` of
its oldest ancestor (a dependency or the enclosing block, transitively); `_scope`, the
`workflow_id` the task was called with, or for a block's member the one its block was
called with; and `_backend`, the name of the backend the task runs on.

A policy serves one engine at a time. Another engine may use it once the first has
shut down, or, after `shutdown(skip_execution_backend=True)`, once the backend has
reported the tasks it still ran (tasks of a submission the shutdown cut short are given
back at once, even if the backend took them); or once the first engine's event loop
has closed. An engine collected without a shutdown gives its tasks back when the
policy is used again, as tasks that never ran; so does one whose loop closed.
If `enqueue` raises, the task fails; if `release` or `runnable` raises, the engine logs
the error and goes on; if `dispatch` raises, the engine shuts down, as for any error in
its run loop.

A last-in, first-out policy with room for one task:

```python
class Lifo:
    def __init__(self):
        self.stack, self.inflight = [], set()

    def enqueue(self, uid, desc):
        self.stack.append(desc)

    def dispatch(self):
        if self.inflight or not self.stack:
            return []
        desc = self.stack.pop()
        self.inflight.add(desc["uid"])
        return [desc]

    def release(self, uid, state, observed=None):
        self.inflight.discard(uid)
        self.stack = [d for d in self.stack if d["uid"] != uid]

    def runnable(self):
        return bool(self.stack) and not self.inflight
```

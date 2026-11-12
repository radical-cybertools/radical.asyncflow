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

## ResourcePolicy

`ResourcePolicy` starts a task when what it declares fits its backend: cores, a share
of a GPU or whole GPUs, host and GPU memory.

```python
from radical.asyncflow import MemoryModel, NodeShape, Pool, ResourcePolicy

nodes = [
    NodeShape(cpus=64, gpus=4, host_mem=256, gpu_mem=40, name=f"gpu{i:03d}")
    for i in range(8)
]  # memory in GiB
policy = ResourcePolicy(Pool(nodes, mode="node"), memory=MemoryModel())
flow = await WorkflowEngine.create(backend=backend, policy=policy)


@flow.function_task
async def fold(seq): ...


fold_needs = {"gpus": 0.25, "gpu_mem": 9, "host_mem": 8}
await fold(seq, task_description={"resources": fold_needs})
```

**Demand.** A task declares it as `task_description["resources"]`: `cpus` (default 1),
`gpus` (below 1, a share of one device; from 1, whole devices),
`host_mem` and `gpu_mem` (GiB; `gpu_mem` is per device). Undeclared memory counts as
zero, unless the policy has a `MemoryModel`. A demand that can never fit fails the
task with a `ValueError`. A `task_description` given at the call replaces the
decorator's `resources` whole, so give the whole demand there. For a backend with a
pool, the policy takes `resources` out of the description before the backend sees it,
so a backend with a setting of the same name, such as Dask's worker resources, does
not read it; for a backend without a pool it passes the description on unchanged.

**Pools.** `NodeShape(cpus, gpus=0, host_mem=None, gpu_mem=None, cpu_overcommit=1.0,
name=None)` describes a node; memory left as None is not counted. `Pool(nodes,
mode="aggregate", unknown_per_node=2)` holds one backend's nodes and what admitted tasks
hold of them. Pass one `Pool` for all tasks, or `{backend name: Pool}`, the default
backend included. Tasks for a backend without a pool are not held back.

**Modes.**

- `aggregate`: the demand must fit the pool's summed capacity. The backend still
  chooses the node, and the application the GPU.
- `node`: the demand must fit one node, and a GPU share one GPU. The policy chooses
  them, the emptiest first so load spreads, and writes the choice into the task
  description as `placement`: `{"node": index, "host": name, "gpus": [device, ...]}`.
  A backend that understands `placement` runs the task there; others ignore it.
  RHAPSODY's Dragon backend understands it from its `feature/dragon-placement` branch.
  Give each node the host name the backend knows it by; names must differ.

**Learned memory.** With a `MemoryModel(min_samples=3, headroom=1.1)`, memory a task
does not declare is learned from the peaks the backend reports for each task, under
`observed` (`{"host_mem": ..., "gpu_mem": ...}`, GiB, `gpu_mem` per device), per kind
of task: function, backend and cpu/gpu shape. The estimate is the largest peak times
`headroom`, capped at what one task can get. Every reported peak raises it, but only
tasks that finished count towards `min_samples`: one that failed early may have peaked
low, while one killed at a high peak raises it for the rest of the run. Until a kind is
learned, its tasks count against a limit of `unknown_per_node` tasks of unknown memory
per node, shared by all such kinds (per pool node count in aggregate mode); the policy
logs a warning once per kind when a finished task reports no peak. No released RHAPSODY
backend reports peaks yet, so declare memory where you can.

**Order.** Tasks start by priority, then oldest first; a younger task starts first only
when it fits and the first does not. The first task that does not fit holds a
reservation: its demand is set aside, in node mode on the node closest to fitting it
among those that can hold it (among equals, the one running most tasks), in aggregate
mode on the pool, and other tasks start only in what is left. So once a task that
needs a whole node is first, it waits only for the tasks running then, and smaller
tasks still use the cores, GPUs and memory it does not need, and the other nodes. A
pool holds one reservation at a time: a big task behind another waiting task can be
passed by smaller ones until it is first. Tasks of one kind whose declared memory is
within a power of two wait in one queue, of which only the first is tried: a task can
wait behind one of its kind that declares up to twice its memory.

GPU shares of 1/k of a device add up exactly for k up to 16, and for 20, 25, 32, 50,
64 and 100; other shares are rounded up to a 14,414,400th of a device.

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

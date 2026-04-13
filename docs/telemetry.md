# Telemetry

AsyncFlow exposes the full RHAPSODY Telemetry Abstraction Layer (TAL) through a single call on the `WorkflowEngine`. This page covers what AsyncFlow adds on top of RHAPSODY's core telemetry and how application-level frameworks can consume the event stream.

For the complete reference on events, OTel instruments, JSONL format, Prometheus/Grafana integration, and custom event creation, see the [RHAPSODY Telemetry docs](https://radical-cybertools.github.io/rhapsody/telemetry/).

---

## Enabling telemetry

```python
from concurrent.futures import ProcessPoolExecutor
from radical.asyncflow import WorkflowEngine
from rhapsody.backends import ConcurrentExecutionBackend

backend = await ConcurrentExecutionBackend(ProcessPoolExecutor())
flow = await WorkflowEngine.create(backend)

telemetry = await flow.start_telemetry(
    resource_poll_interval=5.0,       # node CPU/memory/GPU every 5 s
    checkpoint_path="./telemetry/",   # write a JSONL file (optional)
)
```

`start_telemetry()` returns a `TelemetryManager`. Stop it explicitly when done, or it stops automatically when the workflow engine shuts down:

```python
await flow.shutdown()   # also stops telemetry
# or explicitly:
await telemetry.stop()
```

---

## AsyncFlow-specific events

AsyncFlow emits one event that RHAPSODY core does not:

### `asyncflow.TaskResolved`

Emitted when all upstream dependencies of a task are satisfied — the moment the task becomes eligible for submission. Tasks with dependencies emit it when the last upstream `TaskCompleted` fires.

`asyncflow.TaskResolved` is defined using [`define_event()`](https://radical-cybertools.github.io/rhapsody/telemetry/reference/#custom-events-define_event) rather than being hard-coded in RHAPSODY, keeping the core event schema stable:

```python
# event_type = "asyncflow.TaskResolved"
# task_id    = "task.0003"
```

The `asyncflow.TaskResolved` is the **dependency wait time** — how long a task sat in the graph waiting for its inputs.

### Full lifecycle from the AsyncFlow perspective

```
-------------------------- User App Layer ---------------------------
CustomEvent1 (User app)
....
CustomEventN (User app)
-------------------------- AsyncFlow Layer --------------------------

TaskCreated              ← task registered in DAG (AsyncFlow)
    ↓
asyncflow.TaskResolved   ← all upstream deps satisfied (AsyncFlow)
    ↓
TaskSubmitted            ← handed to RHAPSODY session (AsyncFlow)
    ↓

-------------------------- RHAPSODY Layer --------------------------

(TaskQueued)             ← optional, backend-specific (RHAPSODY Backend)
    ↓
TaskStarted              ← worker begins execution (RHAPSODY Backend)
    ↓
TaskCompleted | TaskFailed | TaskCanceled (RHAPSODY Backend)
```

---

## Subscribing to events

Use `telemetry.subscribe()` to receive every event in real time. The callback can be sync or async and runs on the asyncio event loop.

```python
def on_event(event):
    print(event.event_type, event.task_id)

telemetry.subscribe(on_event)
```

**Filter by event type:**

```python
def on_event(event):
    if event.event_type == "TaskCompleted":
        print(f"done: {event.task_id}  {event.duration_seconds*1000:.0f} ms")
    elif event.event_type == "asyncflow.TaskResolved":
        print(f"resolved: {event.task_id}")
    elif event.event_type == "ResourceUpdate" and event.resource_scope == "per_node":
        print(f"cpu: {event.cpu_percent:.1f}%  mem: {event.memory_percent:.1f}%")
```

**Measure dependency wait time:**

```python
resolved_at = {}

def on_event(event):
    if event.event_type == "asyncflow.TaskResolved":
        resolved_at[event.task_id] = event.event_time
    elif event.event_type == "TaskStarted":
        dep_wait_ms = (event.event_time - resolved_at.get(event.task_id, event.event_time)) * 1000
        print(f"{event.task_id}: dep_wait={dep_wait_ms:.0f} ms")
```

---

## Reading results after the run

```python
await flow.shutdown()

summary = telemetry.summary()
print(summary["tasks"])
# {'submitted': 200, 'completed': 197, 'failed': 2, 'canceled': 1, 'running': 0}

if summary["duration"]:
    print(f"mean task time: {summary['duration']['mean_seconds']*1000:.1f} ms")
```

---

## Application-level frameworks

If you are building a framework on top of AsyncFlow (e.g. a domain-specific orchestrator, an ML pipeline library, a data processing layer), the telemetry bus is the right place to attach application-domain observability — without leaking implementation details back into AsyncFlow or RHAPSODY.

### Defining application events

Use `define_event()` to create typed event classes. Names must be namespaced (contain a dot). Base fields (`event_id`, `session_id`, `event_time`, etc.) are protected.

```python
from rhapsody.telemetry import define_event
from rhapsody.telemetry.events import make_event

# Define at module level
DataQualityChecked = define_event(
    "myframework.DataQualityChecked",
    dataset=str,
    score=float,
    rows_checked=int,
)

ModelCheckpoint = define_event(
    "myframework.ModelCheckpoint",
    step=(int, -1),
    loss=(float, float("inf")),
    metric=str,
)
```

### Emitting application events

Emit from your framework code (not from inside task functions — see [why](#emitting-from-inside-tasks)):

```python
# After a task result is available
result = await my_pipeline_task()

telemetry.emit(
    make_event(
        DataQualityChecked,
        session_id=telemetry.session_id,
        backend="myframework",
        dataset="train_2024",
        score=result["quality_score"],
        rows_checked=result["row_count"],
    )
)
```

`telemetry.emit()` is **synchronous** — no `await`.

### Subscribing in a framework layer

```python
class PipelineMonitor:
    def __init__(self, telemetry):
        self.violations = []
        self.checkpoints = []
        telemetry.subscribe(self._on_event)

    def _on_event(self, event):
        et = event.event_type

        if et == "TaskFailed":
            self.violations.append({
                "task_id": event.task_id,
                "error": event.error_type,
            })
        elif et == "TaskCanceled":
            # Cancellation is intentional — track separately from failures
            pass
        elif et == "myframework.DataQualityChecked":
            if event.score < 0.95:
                print(f"[ALERT] quality {event.score:.2%} on {event.dataset}")
        elif et == "myframework.ModelCheckpoint":
            self.checkpoints.append({"step": event.step, "loss": event.loss})

monitor = PipelineMonitor(telemetry)
```

### Emitting from inside tasks

**Do not call `telemetry.emit()` inside a task function.** Task functions are serialized by `cloudpickle` for subprocess execution. Capturing `telemetry` in the closure serializes its asyncio state, which fails at runtime.

```python
# ✗ WRONG — telemetry captured in closure, fails with cloudpickle
@flow.function_task
async def bad_task(data):
    result = process(data)
    telemetry.emit(...)   # ← do not do this
    return result

# ✓ CORRECT — emit after awaiting the task future, in the orchestration layer
async def run():
    result_fut = my_task(data)
    result = await result_fut
    telemetry.emit(make_event(MyEvent, ..., value=result["score"]))
```

---

## Further reading

- [RHAPSODY Telemetry Quick Start](https://radical-cybertools.github.io/rhapsody/telemetry/quickstart/) — enabling telemetry, reading metrics and spans
- [Events & Metrics Reference](https://radical-cybertools.github.io/rhapsody/telemetry/reference/) — all event fields, OTel instruments, JSONL format
- [Integrations & Extensions](https://radical-cybertools.github.io/rhapsody/telemetry/integrations/) — Prometheus, Grafana, Jaeger, MLflow, pandas

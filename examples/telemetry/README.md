# AsyncFlow Telemetry Examples

---

## Files

### `00-telemetry_subscribe_basic.py`

A 5-stage ETL DAG running 20 parallel workflow instances:

```
ingest → validate ──────────┐
       → validate_schema ───┴→ transform → aggregate → report
```

Demonstrates:
- Enabling telemetry via `flow.start_telemetry()`
- Emitting **custom application events** (`etl.StageTimer`) per stage using `define_event` + `telemetry.emit()`
- Full task lifecycle: `TaskCreated` → `asyncflow.TaskResolved` → `TaskSubmitted` → `TaskStarted` → `TaskCompleted`

Writes a JSONL checkpoint to `telemetry-output/` for use with `plot_workflow_dashboard.py`.

```bash
python 00-telemetry_subscribe_basic.py
```

---

### `plot_workflow_dashboard.py`

Workflow-focused visualization of the JSONL checkpoint. Produces a PNG with 7 panels:

```bash
python plot_workflow_dashboard.py telemetry-output/run.jsonl
python plot_workflow_dashboard.py telemetry-output/run.jsonl --out report.png

# Split each figure in the dashboard to be a standalone plot
python plot_workflow_dashboard.py --split telemetry-output/run.jsonl
```

| Panel | What it shows |
|---|---|
| **Task Gantt** | One bar per task coloured by lifecycle phase (Created → Resolved → Submitted → Running → Completed) |
| **Phase Duration Distribution** | Boxplot of time spent in each phase across all tasks — reveals dep wait vs routing vs execution |
| **Concurrency** | Tasks running simultaneously — shows DAG parallelism |
| **Task Waterfall** | Stacked bar sorted by e2e time — identifies the slowest tasks and which phase dominates |
| **Dependency Wait** | Histogram of `asyncflow.TaskResolved → TaskStarted` — the core AsyncFlow scheduling metric |
| **Stage Timer** | Mean ± std per ETL stage from `etl.StageTimer` custom events — application-level breakdown |
| **Task Lifecycle Swim-lanes** *(full row)* | One row per task: purple = dep wait · amber = pre-exec · green = execution |
| **Concurrency + Node Resources** *(full row)* | Concurrency step (left axis) + CPU % and Memory % per node (right axis) |

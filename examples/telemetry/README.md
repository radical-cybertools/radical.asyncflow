# AsyncFlow Telemetry Examples

> **Requires:** `pip install rhapsody-py[telemetry]`

Two self-contained examples, each paired with a plotting script.

---

## Example 00 — Basic telemetry & custom events

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

```bash
python 00-telemetry_subscribe_basic.py
```

Writes a JSONL checkpoint to `telemetry-output/` for `plot_workflow_dashboard.py`.

---

### `plot_workflow_dashboard.py`

Comprehensive 8-panel dashboard for the ETL workflow checkpoint:

```bash
python plot_workflow_dashboard.py telemetry-output/run.jsonl
python plot_workflow_dashboard.py telemetry-output/run.jsonl --out report.png

# Save each panel as a separate file
python plot_workflow_dashboard.py telemetry-output/run.jsonl --split
```

| Panel | What it shows |
|---|---|
| **Task Gantt** | One bar per task coloured by lifecycle phase (Created → Resolved → Submitted → Running → Completed) |
| **Phase Duration Distribution** | Boxplot of time spent in each phase — reveals dep wait vs routing vs execution |
| **Concurrency** | Tasks running simultaneously — shows DAG parallelism |
| **Task Waterfall** | Stacked bar sorted by e2e time — identifies the slowest tasks and which phase dominates |
| **Dependency Wait** | Histogram of `asyncflow.TaskResolved → TaskStarted` — the core AsyncFlow scheduling metric |
| **Stage Timer** | Mean ± std per ETL stage from `etl.StageTimer` custom events — application-level breakdown |
| **Task Lifecycle Swim-lanes** *(full row)* | One row per task: purple = dep wait · amber = pre-exec · green = execution |
| **Concurrency + Node Resources** *(full row)* | Concurrency step (left axis) + CPU % and Memory % per node (right axis) |

---

## Example 01 — Workflow grouping with `workflow_scope()`

### `01-workflow_grouping.py`

Six parallel ML training pipelines, each grouped with `workflow_scope()`:

```
load_data → preprocess → train → evaluate
```

Every task inside a `workflow_scope()` automatically carries
`asyncflow.workflow_id` in every telemetry event — with zero changes to the
task functions themselves. Emits a custom `experiment.Result` event at the
end of each pipeline with loss, accuracy, and elapsed time.

```bash
python 01-workflow_grouping.py
python 01-workflow_grouping.py --out results/
```

Writes a JSONL checkpoint to `telemetry-output/` (or `--out` dir) for `plot_workflow_gantt.py`.

---

### `plot_workflow_gantt.py`

Workflow-grouping dashboard — accepts the output directory or JSONL file as a CLI argument:

```bash
python plot_workflow_gantt.py telemetry-output/
python plot_workflow_gantt.py telemetry-output/ --out dashboard.png
python plot_workflow_gantt.py telemetry-output/session.jsonl --dpi 180
```

Produces a six-panel PNG:

| Panel | What it shows |
|---|---|
| **Workflow Gantt** *(full row)* | One row per workflow instance; bars coloured by stage (load / preprocess / train / evaluate); light band = dependency wait |
| **Stage Execution Time** | Stacked horizontal bars: ms per stage per workflow — reveals which stage dominates each run |
| **Workflow Concurrency** | Step function of in-flight workflows over time — shows scheduler saturation |
| **Workflow Throughput** | Histogram of workflow completion times — when pipelines finish relative to session start |
| **E2E Latency Distribution** | Histogram of total elapsed time per workflow with mean/median lines |
| **Stage × Time Heatmap** *(full row)* | 2-D `YlOrRd` heatmap (stages × time bins): fractional task occupancy per cell — reveals bottleneck stages |

# Changelog

All notable changes to RADICAL AsyncFlow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.5.0] - 2026-06-16

### Added

- **`future.state` attribute** — every component future (task and block) now exposes a `.state`
  string attribute tracking its full lifecycle: `PENDING` → `RUNNING` → `DONE` / `FAILED` /
  `CANCELLED`. Block futures now correctly transition through all states; previously they remained
  at `PENDING` for the duration of their execution. Readable at any point without awaiting.

- **`workflow_id=` call-time kwarg** — any task or block call now accepts `workflow_id="<id>"` as
  a keyword argument to tag that specific component. Takes precedence over any active
  `workflow_scope()`. The kwarg is stripped before the function body is invoked.

### Fixed

- **`shutdown()` no longer crashes when blocks are still running** — block futures are now cancelled directly instead of routing through `handle_task_cancellation`, which assumed `original_cancel` was set (only true for task futures).

- **Block future state lifecycle** — block futures now transition to `RUNNING` immediately after
  `asyncio.create_task`, to `DONE` on normal completion, and to `FAILED` when the block body
  raises. Previously all three transitions were missing.

- **`_block_members` cleanup on non-cancelled outcomes** — member sets are now removed from
  `_block_members` for every terminal outcome (DONE, FAILED, CANCELLED). Previously the cleanup
  only ran on cancellation, leaving stale entries after normal or failed block completion.

- **`patched_cancel` forwards `msg` argument** — `fut.cancel(msg=...)` now correctly forwards
  all positional and keyword arguments to the underlying `asyncio.Future.cancel()`. Previously
  the `msg` was silently dropped, breaking callers that rely on the cancellation message being
  propagated through `CancelledError`.

- **Pending-task cancellation sets `future.state`** — when a pending task is cancelled locally
  via `patched_cancel`, `future.state` is now set to `"CANCELLED"`. All other cancellation paths
  already set this attribute; this was the only missing case.

- **`_clear_internal_records` completeness** — `resolved`, `running`, `_task_submit_times`, and
  `_task_start_times` are now cleared alongside the other internal structures. Previously these
  accumulated stale entries across engine reuse after `shutdown()`.

- **`_block_asyncio_tasks` cleared on shutdown** — `_clear_internal_records` now also clears the
  asyncio.Task registry for blocks, eliminating stale references after engine reset.

### Changed

- **`self.running` changed from `list` to `set`** — membership check (`uid in self.running`) and
  removal (`running.discard`) in the run loop and `patched_cancel` are now O(1) instead of O(n).
  No public API change.

## [0.4.0] - 2026-05-18

### Added

- **`WorkflowEngine.workflow_scope(workflow_id=None)`** — new async context
  manager that groups all tasks registered inside under a shared
  `asyncflow.workflow_id`. Internally sets `_workflow_id_ctx` (a module-level
  `ContextVar`), which asyncio copies into every `create_task` / `gather`
  branch automatically so concurrent workflow instances remain isolated. When
  telemetry is active, also opens an OTel `"workflow"` span, making all task
  spans inside structural children in the trace hierarchy. Auto-generates a
  short UUID if `workflow_id` is `None`.

- **`_workflow_id_ctx` ContextVar** — per-instance `ContextVar` (default
  `None`) that carries the active workflow ID across asyncio task boundaries
  without explicit argument passing. Each `WorkflowEngine` instance owns its
  own uniquely-named ContextVar (`asyncflow_workflow_id.<uid>`), preventing
  context leakage when multiple engines are used within the same coroutine.
  `@flow.block` execution sets it to the block's UID so tasks inside a block
  inherit the workflow ID automatically.

- **`WorkflowEngine.start_telemetry()` new parameters** — `span_processors`,
  `metric_readers`, `resource` — forwarded to `TelemetryManager.__init__()`,
  mirroring `Session.start_telemetry()`. Enables passing pre-built OTel
  exporters (OTLP, Prometheus, Jaeger) without any RHAPSODY code changes.

- **Span enricher for `asyncflow.workflow_id`** — registered automatically by
  `start_telemetry()` via `register_span_enricher()`. Stamps
  `asyncflow.workflow_id` onto every task OTel span when the task was created
  inside a `workflow_scope()` or `@flow.block`. Enables per-workflow Gantt
  views and span filtering in Jaeger / Grafana Tempo.

- **`_emit()` `workflow_id` kwarg** — when `workflow_id` is set, injects
  `asyncflow.workflow_id` into the event's `attributes` dict. All task lifecycle
  events emitted by `WorkflowEngine` (TaskCreated, asyncflow.TaskResolved,
  TaskSubmitted) carry the active workflow ID.

- **Example `01-workflow_grouping.py`** — complete rewrite as an HPC Campaign
  Manager simulation. Models 4 workflow types with resource tracking and
  dependency chains:
  - `simulate` (4 tasks, GPU, no deps) — molecular dynamics runs
  - `analyze` (4 tasks, GPU, deps=simulate) — post-processing per simulation
  - `train` (8 tasks, GPU, no deps) — distributed ML training
  - `evaluate` (8 tasks, CPU, deps=train) — lightweight model evaluation
  Uses `ResourcePool` (asyncio-queue-based GPU/CPU slot tracking) and emits
  `campaign.ResourceAssigned` custom events to record per-instance resource
  assignments in the JSONL checkpoint.

- **`plot_campaign.py`** — new plotting script producing a two-panel Campaign
  Manager timeline figure:
  - **Top panel**: Gantt chart with rows per workflow instance, bars coloured by
    workflow type and labelled with the assigned resource (`gpu:N` / `cpu:N`).
    Right-margin annotations show priority / cpu / gpu per type. Right column
    renders a config table, scheduler description box, and dependency graph.
  - **Bottom panel**: GPU (left axis) and CPU (right axis) utilisation as step
    functions over elapsed time, with total-capacity dashed reference lines.

- **`capture_stdio` decorator parameter** — `@flow.executable_task(capture_stdio=True)` redirects
  stdout/stderr from executable tasks directly to files instead of collecting them in memory.
  The awaited future resolves to the stdout **file path** (`{work_dir}/{uid}/{task_uid}.stdout`)
  instead of a decoded string. Honoured by all RHAPSODY backends (`ConcurrentExecutionBackend`,
  `DaskExecutionBackend`, `DragonExecutionBackendV3`); silently ignored for function tasks.
  `capture_stdio` is stored as a **top-level field** in `comp_desc` (not nested inside
  `task_backend_specific_kwargs`) so RHAPSODY backends can read it as `task.get("capture_stdio")`.
- **`WorkflowEngine` `uid` and `work_dir` parameters** — `WorkflowEngine.create(uid=..., work_dir=...)`
  sets the engine's unique identifier (UUID) and working directory. The engine is now the single authority
  for `backend._work_dir`; it sets `backend._work_dir = {work_dir}/{uid}` and creates the
  directory before any tasks run (mirrors `Session.add_backend()` in RHAPSODY).
- **`WorkflowEngine._attach_backend()`** — new internal method that replaces the inline
  `register_callback` loop. Handles `_work_dir` injection, `is_attached` flag, and callback
  registration in one place.
- Unit tests for `capture_stdio` field placement, default value, `_work_dir` authority,
  and end-to-end file I/O with `ConcurrentExecutionBackend`.

### Fixed

- **Block spans parented correctly** — `execute_block()` now benefits from the
  `span_scope()` session-span fallback added in RHAPSODY: block spans that run
  in a context with no active OTel span (e.g. spawned from `run()` before
  `start_telemetry()` was awaited) now correctly nest under the session root
  span instead of floating as unrooted traces.

- **`execute_block()` dead branch removed** — the `run_in_executor` code path
  (used when the wrapped function was sync) is removed. `WorkflowEngine`
  enforces a strict async API; all block functions must be `async def`.

### Changed

- **`execute_block()` uses `nullcontext`** — the no-telemetry code path uses
  stdlib `nullcontext` (Python >= 3.7) instead of the former custom
  `_null_context()` helper, which is removed.

- **`execute_block()` sets `_workflow_id_ctx`** — the block's UID is set as the
  active `_workflow_id_ctx` for the duration of block execution so every task
  registered inside the block inherits it as `asyncflow.workflow_id` without
  requiring an explicit `workflow_scope()` call.

### Docs

- **`docs/telemetry.md`**:
  - Added "Forwarding to an external backend" section with corrected
    `span_processors` / `metric_readers` code examples (replaces the broken
    `set_tracer_provider()` pattern).
  - Added "`workflow_scope()` context manager" section with usage examples and
    auto-ID generation.
  - Added "OTel span hierarchy" section showing the four-level
    `session -> workflow -> block -> task` tree and explaining how
    `asyncflow.workflow_id` propagates to every span attribute and JSONL event.
  - Updated `start_telemetry()` signature to include `span_processors`,
    `metric_readers`, and `resource` parameters.

## [0.3.1] - 2026-03-09

### Fixed

- **`executable_task` command parsing** — `shlex.split` is now applied correctly to the command string returned by the decorated function, ensuring commands with arguments and quoted strings are parsed properly.

## [0.3.0] - 2026-02-24

### Added

- **`prompt_task` decorator** — new first-class task type alongside `function_task` and `executable_task`. The decorated async function returns a prompt string that is forwarded to an AI execution backend (e.g. `DragonVllmInferenceBackend` from RHAPSODY). `NoopExecutionBackend` returns `"Dummy Prompt Output"` for testing; `LocalExecutionBackend` raises `NotImplementedError` with a clear message directing users to register an AI backend.
- **Multi-backend registry** — `WorkflowEngine.create(backend=...)` now accepts either a single backend or a list of pre-initialized named backends (each must expose a `.name` attribute). The first backend in the list is the default. Built-in backends (`LocalExecutionBackend`, `NoopExecutionBackend`) gain a `name` parameter defaulting to `"default"`.
- **Per-task backend routing** — task decorators (`function_task`, `executable_task`, `prompt_task`) accept an optional `backend="<name>"` parameter to route a specific task to a named backend. Tasks without `backend=` are sent to the default backend.
- **Parallel backend submission** — when a batch contains tasks routed to multiple backends, `submit()` dispatches all backend calls concurrently via `asyncio.gather`. `shutdown()` similarly drains all registered backends in parallel.
- **Example `04-concurrent_backends.py`** — demonstrates a compute + AI dual-backend workflow running document processing pipelines concurrently.
- **Tests** — unit tests (`tests/unit/test_prompt_task.py`) and integration tests (`tests/integration/test_multi_backend.py`) covering `prompt_task` registration, validation, multi-backend routing, parallel submission, and graceful shutdown.

### Fixed

- **`async_wrapper` exception propagation** — errors raised during task registration (e.g. `prompt_task` returning `None`, or `executable_task` returning a non-string) are now propagated to the task's `asyncio.Future` instead of being silently swallowed by the detached `asyncio.create_task`. Previously, such errors would cause `await task(...)` to hang indefinitely.

### Changed

- **Documentation** — all HPC backend references across docs, examples, and the Jupyter tutorial now point to the official [RHAPSODY documentation](https://radical-cybertools.github.io/rhapsody/) instead of the GitHub repository, and include a direct link to the [AsyncFlow integration guide](https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration).
- **Examples** — HPC example scripts (`04-dask_execution_backend.py`, `05-radical_execution_backend.py`, `06-dragon_execution_backend.py`) updated with module-level docstrings referencing RHAPSODY docs and the AsyncFlow integration guide.
- **Proof-of-concept examples** — all basic/getting-started examples (`basic.md`, `async_workflows.md`, `composite_workflow.md`, examples 01–03) consistently use `LocalExecutionBackend`; RHAPSODY backends are only shown in dedicated HPC sections.
- **Install command** — RHAPSODY install command corrected to `pip install rhapsody-py` across all docs and examples.

## [0.2.0] - 2026-02-12

### Added

- **Local execution backend** — `LocalExecutionBackend` ships built-in, supporting both `ThreadPoolExecutor` and `ProcessPoolExecutor` with `cloudpickle` serialization.
- **No-op backend** — `NoopExecutionBackend` for testing and `dry_run` workflows without computational overhead.
- **HPC execution via RHAPSODY** — HPC backends (`RadicalExecutionBackend`, `DaskExecutionBackend`, `DragonExecutionBackendV3`) are now provided by the separate [RHAPSODY](https://github.com/radical-cybertools/rhapsody) package and plug into AsyncFlow via simple import.
- **Composite workflows** — `@flow.block` decorator for grouping tasks into logical units with support for nested blocks and inter-block dependencies.
- **Task dependency resolution** — automatic DAG-based scheduling with implicit and explicit data dependencies between `function_task` and `executable_task` types.
- **Pluggable backend architecture** — any backend conforming to the execution interface can be passed to `WorkflowEngine.create()`, enabling custom and third-party backends.
- **Pre-commit pipeline** — ruff, docformatter, typos, actionlint, detect-secrets, and standard pre-commit hooks.
- **CI/CD** — GitHub Actions workflows for unit tests, integration tests, pre-commit checks, and documentation deployment across Python 3.9-3.13.

### Changed

- **Backend decoupling** — HPC backends (Dask, RadicalPilot, Concurrent, Dragon) moved from `radical.asyncflow` to the standalone `rhapsody` package. AsyncFlow now only ships `LocalExecutionBackend` and `NoopExecutionBackend`.
- **Imports for HPC backends** — HPC backends are now imported from `rhapsody.backends` instead of `radical.asyncflow`.
- **Documentation** — all docs and examples updated to reflect the new architecture, with clear separation between built-in local backends and RHAPSODY HPC backends.

## [0.1.0] - 2025-12-07

Initial public release of RADICAL AsyncFlow.

### Added
- Async-first workflow engine with DAG-based task scheduling.
- Support for async and sync function execution within workflows.
- Execution backend abstraction with pluggable backend interface.
- RADICAL-based HPC execution backend integration.
- Agentic workflow support.
- Event-driven shutdown mechanism for backend lifecycle management.
- Task cancellation signaling and cooperative cancellation support.
- Enhanced async context manager support for workflow execution.
- Unit and integration testing framework.
- Initial project documentation.
- Migration to `pyproject.toml` with `tox`-based test environments.

### Changed
- Renamed project from `flow` to `asyncflow`.
- Removed legacy synchronous execution paradigm in favor of fully async architecture.
- Refactored execution loop for thread-based backend.
- Updated RADICAL execution backend callback mechanism to support service-style operation.
- Improved error propagation for failed tasks.
- Refactored internal utilities to remove dependency on `radical.utils`.
- Eliminated redundant data dependency linking logic.

### Fixed
- Corrected handling of task failure without explicit `wait()`.
- Fixed `handle_task_failure` logic for consistent error reporting.
- Resolved process-based function execution launch issues.
- Ensured `pre_exec` directives are properly applied.
- Addressed multiple execution edge cases in RADICAL backend.

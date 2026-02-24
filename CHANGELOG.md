# Changelog

All notable changes to RADICAL AsyncFlow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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

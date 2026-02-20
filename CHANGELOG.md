# Changelog

All notable changes to RADICAL AsyncFlow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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

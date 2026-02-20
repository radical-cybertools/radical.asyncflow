# AsyncFlow Examples

This directory contains examples demonstrating different features of RADICAL AsyncFlow.

## Local Execution (built-in)

These examples use `LocalExecutionBackend` from `radical.asyncflow` and run locally without any additional dependencies.

| Example | Description |
|---------|-------------|
| [01-workflows.py](01-workflows.py) | Basic workflow with task dependencies — defines three chained function tasks and runs them concurrently. |
| [02-blocks.py](02-blocks.py) | Composite workflows using `@flow.block` — groups tasks into blocks with inter-block dependencies. |
| [03-nested_blocks.py](03-nested_blocks.py) | Nested blocks of blocks — demonstrates hierarchical workflow composition with multiple nesting levels. |
| [04-concurrent_backends.py](04-concurrent_backends.py) | Concurrent backends — registers a `LocalExecutionBackend` and a simulated AI backend on the same engine, routing `function_task` and `prompt_task` to each via the `backend=` decorator parameter. |

### Running

```bash
python examples/01-workflows.py
python examples/02-blocks.py
python examples/03-nested_blocks.py
python examples/04-concurrent_backends.py
```

## HPC Execution (requires RHAPSODY)

These examples require [RHAPSODY](https://radical-cybertools.github.io/rhapsody/) to be installed (`pip install rhapsody-py`). RHAPSODY backends [plug directly into AsyncFlow](https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration) — see the integration guide for full details on configuration and usage.

| Example | Description |
|---------|-------------|
| [04-dask_execution_backend.py](04-dask_execution_backend.py) | Parallel data analysis using `DaskExecutionBackend` — loads a dataset and analyzes batches in parallel with Dask workers. |
| [05-radical_execution_backend.py](05-radical_execution_backend.py) | HPC execution using `RadicalExecutionBackend` — runs executable tasks with resource descriptions (ranks, GPUs) on RADICAL-Pilot. |
| [06-dragon_execution_backend.py](06-dragon_execution_backend.py) | Dragon V3 batch processing — demonstrates `process_template` for single-process tasks, `process_templates` for multi-process parallel jobs, and native function execution. |

### Running

```bash
pip install rhapsody-py
python examples/04-dask_execution_backend.py
python examples/05-radical_execution_backend.py
dragon examples/06-dragon_execution_backend.py
```

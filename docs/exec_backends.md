# Execution Backends: Seamless Backend Switching

AsyncFlow's architecture follows a **separation of concerns** principle, completely isolating the execution backend from the asynchronous programming layer. This **plug-and-play (PnP)** design allows you to switch between different execution environments with minimal code changes — from local development to massive HPC clusters.

## Backend Registry and Factory System

AsyncFlow uses a modern **registry and factory pattern** for backend management:

- **Registry**: Discovers and lazy-loads available backends on demand
- **Factory**: Creates and initializes backend instances with proper configuration
- **Lazy Loading**: Backends are only loaded when requested, avoiding unnecessary dependencies

This architecture provides:
- **Centralized backend management** with automatic discovery
- **Better error messages** with installation hints for missing backends
- **Proper caching** to avoid repeated failed imports
- **Type safety** with automatic validation of backend interfaces

## Available Backends

AsyncFlow automatically discovers and manages the following backends:

- **`noop`** - No-operation backend for dry runs and testing
- **`concurrent`** - Local execution using Python's concurrent.futures
- **`dask`** - Distributed computing with Dask (requires `radical.asyncflow[dask]`)
- **`radical_pilot`** - HPC execution with RADICAL-Pilot (requires `radical.asyncflow[radicalpilot]`)

!!! tip "Backend Discovery"
    Use `factory.list_available_backends()` to see which backends are available in your environment and get installation hints for missing ones.

## The Power of Backend Abstraction

By design, AsyncFlow enforces that the execution backend should be entirely isolated from the asynchronous programming layer. This means you can seamlessly transition your workflows from:

- **Local development** with thread pools
- **HPC clusters** with thousands of nodes
- **GPU clusters** for accelerated computing

**The best part?** Changing execution backends requires modifying just **one line of code**.

## Local vs HPC Execution: A Side-by-Side Comparison

### Local Execution with Factory Pattern

```python
# Local execution with concurrent backend
from radical.asyncflow import factory

# Create backend using factory
backend = await factory.create_backend("concurrent", config={
    "max_workers": 4,
    "executor_type": "thread"  # or "process"
})
```

### HPC Execution with Factory Pattern

```python
# HPC execution with RADICAL-Pilot
from radical.asyncflow import factory

# Create backend using factory
backend = await factory.create_backend("radical_pilot", config={
    "resource": "local.localhost"
})
```

!!! success
**One line change** transforms your workflow from local thread execution to distributed HPC execution across thousands of nodes.

### Backend Discovery and Error Handling

```python
from radical.asyncflow import factory

# List available backends
backends = factory.list_available_backends()
for name, info in backends.items():
    print(f"Backend '{name}': {'✅' if info['available'] else '❌'}")
    if not info['available']:
        print(f"  Installation hint: {info['installation_hint']}")
```

!!! tip "Helpful Error Messages"
    When a backend is not available, AsyncFlow provides clear error messages with installation instructions:
    ```
    Backend 'dask' is not available.
    Available backends: noop, concurrent
    Installation hint: Try: pip install 'radical.asyncflow[dask]'
    ```

## Complete HPC Workflow Example

Below is a complete example demonstrating how to execute workflows on HPC infrastructure using the factory pattern.

### Setup for HPC Execution

```python
import time
import asyncio
from radical.asyncflow import factory, WorkflowEngine

# HPC backend configuration using factory
backend = await factory.create_backend("radical_pilot", config={
    "resource": "local.localhost"  # (1)!
})
flow = await WorkflowEngine.create(backend=backend)
```

1. Configure for HPC execution - can target supercomputers, GPU clusters, local resources

!!! tip
**HPC Resource Configuration**: The `resource` parameter can be configured for various HPC systems:
- `'local.localhost'` for local testing
- `'ornl.summit'` for Oak Ridge Summit supercomputer
- `'tacc.frontera'` for TACC Frontera system
- `'anl.theta'` for Argonne Theta system
- Custom configurations for your institutional clusters

### Define Executable Tasks for HPC

```python
@flow.executable_task # (1)!
async def task1(*args):
    return "/bin/date"

@flow.executable_task
async def task2(*args):
    return "/bin/date"

@flow.executable_task
async def task3(*args):
    return "/bin/date"
```

1. `@flow.executable_task` creates tasks that execute as shell commands on HPC nodes

!!! info
**Executable Tasks**: Unlike function tasks, executable tasks return shell commands that are executed on remote HPC nodes, allowing you to leverage:
- **Specialized HPC software** installed on compute nodes
- **High-performance compiled applications**
- **GPU-accelerated programs**
- **MPI-based parallel applications**

### Define Workflow with Dependencies

```python
async def run_wf(wf_id):
    print(f'Starting workflow {wf_id} at {time.time()}')

    # Create dependent task execution
    t3 = task3(task1(), task2()) # (1)!
    await t3  # Wait for distributed execution to complete

    print(f'Workflow {wf_id} completed at {time.time()}')
```

1. Task3 depends on both task1 and task2 completion, but they execute on HPC nodes

!!! note
**Dependency Handling**: AsyncFlow automatically handles task dependencies across distributed HPC nodes, ensuring proper execution order while maximizing parallelism.

### Execute Multiple Workflows on HPC

```python
start_time = time.time()

# Execute 5 workflows concurrently across HPC infrastructure
await asyncio.gather(*[run_wf(i) for i in range(5)]) # (1)!

end_time = time.time()
print(f'\nTotal time running asynchronously is: {end_time - start_time}')

# Proper cleanup of HPC resources
await flow.shutdown()
```

1. All workflows execute concurrently across available HPC nodes

??? "HPC execution log"
    ```text
    RadicalExecutionBackend started
    Starting workflow 0 at 1752775108.50
    Starting workflow 1 at 1752775108.50
    Starting workflow 2 at 1752775108.50
    Starting workflow 3 at 1752775108.50
    Starting workflow 4 at 1752775108.50
    Workflow 0 completed at 1752775110.25
    Workflow 1 completed at 1752775110.26
    Workflow 2 completed at 1752775110.27
    Workflow 3 completed at 1752775110.28
    Workflow 4 completed at 1752775110.29
    RadicalExecutionBackend: All tasks completed, cleaning up resources

    Total time running asynchronously is: 1.79
    ```

## HPC vs Local Development: Key Differences

| Aspect | ConcurrentExecutionBackend | RadicalExecutionBackend |
|--------|---------------------------|-------------------------|
| **Scale** | Single machine, limited cores | Thousands of nodes, massive parallelism |
| **Memory** | Local system RAM | Distributed memory across nodes |
| **Storage** | Local filesystem | High-performance parallel filesystems |
| **Task Type** | Python functions | Shell executables, compiled programs |
| **Use Case** | Development, testing | Production HPC workloads |

## Advanced HPC Configurations

### GPU Cluster Configuration

```python
# Configure for GPU-accelerated computing
backend = await factory.create_backend("radical_pilot", config={
    "resource": "ornl.summit",
    "queue": "gpu",
    "nodes": 100,
    "gpus_per_node": 6,
    "walltime": 120  # minutes
})
```

### Large-Scale CPU Configuration

```python
# Configure for massive CPU parallelism
backend = await factory.create_backend("radical_pilot", config={
    "resource": "tacc.frontera",
    "queue": "normal",
    "nodes": 1000,
    "cores_per_node": 56,
    "walltime": 240  # minutes
})
```

### Backend Availability Check

```python
# Check if HPC backend is available before creating
backends = factory.list_available_backends()
if backends["radical_pilot"]["available"]:
    backend = await factory.create_backend("radical_pilot", config={...})
else:
    print(f"RADICAL-Pilot not available: {backends['radical_pilot']['installation_hint']}")
    # Fallback to local execution
    backend = await factory.create_backend("concurrent")
```

!!! warning
**Resource Management**: Always call `await flow.shutdown()` to properly release HPC resources and prevent job queue issues.

## Real-World HPC Use Cases

**Scientific Computing**: Execute thousands of simulations across supercomputer nodes for climate modeling, molecular dynamics, or astrophysics calculations.

**Machine Learning**: Train large models across GPU clusters with distributed data processing and model parallelism.

**Bioinformatics**: Process genomic datasets across hundreds of nodes for sequence alignment, variant calling, or phylogenetic analysis.

**Engineering Simulation**: Run computational fluid dynamics, finite element analysis, or structural optimization across distributed computing resources.

!!! tip
**Development Strategy**: Start with `ConcurrentExecutionBackend` for local development and testing, then seamlessly switch to `RadicalExecutionBackend` for production HPC runs.

## The AsyncFlow Advantage

AsyncFlow's backend abstraction means your workflow logic remains **identical** whether running on:
- Your laptop with 8 cores
- A university cluster with 1,000 nodes
- A national supercomputer with 100,000+ cores
- GPU clusters with thousands of accelerators

This **write-once, run-anywhere** approach dramatically reduces the complexity of scaling computational workflows from development to production HPC environments.

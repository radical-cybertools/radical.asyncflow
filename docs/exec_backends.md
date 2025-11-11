# Execution Backends: Seamless Backend Switching

AsyncFlow's architecture follows a **separation of concerns** principle, completely isolating the execution backend from the asynchronous programming layer. This **plug-and-play (PnP)** design allows you to switch between different execution environments with minimal code changes — from local development to massive HPC clusters.

## The Power of Backend Abstraction

By design, AsyncFlow enforces that the execution backend should be entirely isolated from the asynchronous programming layer. This means you can seamlessly transition your workflows from:

- **Local development** with thread pools
- **HPC clusters** with thousands of nodes
- **GPU clusters** for accelerated computing

**The best part?** Changing execution backends requires modifying just **one line of code**.

## Local vs HPC Execution: A Side-by-Side Comparison

### Local Execution with ConcurrentExecutionBackend

```python
# Local execution with threads

from concurrent.futures import ThreadPoolExecutor
from radical.asyncflow import ConcurrentExecutionBackend

backend = ConcurrentExecutionBackend(ThreadPoolExecutor())
```

### HPC Execution with RadicalExecutionBackend

```python
# HPC execution with Radical.Pilot
from radical.asyncflow import RadicalExecutionBackend

backend = RadicalExecutionBackend({'resource': 'local.localhost'})
```

!!! success
    **One line change** transforms your workflow from local thread execution to distributed HPC execution across thousands of nodes.

## Complete HPC Workflow Example

Below is a complete example demonstrating how to execute workflows on HPC infrastructure using `RadicalExecutionBackend`.

### Setup for HPC Execution

```python
import time
import asyncio
from radical.asyncflow import RadicalExecutionBackend
from radical.asyncflow import WorkflowEngine

# HPC backend configuration
backend = RadicalExecutionBackend({'nodes': 1, 'resource': 'local.localhost'}) # (1)!
flow = WorkflowEngine(backend=backend)
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


### Assign Resources for your application (task)

Depending on the `ExecutionBackend` used, Asyncflow supports passing and assigning multiple/different task and resources parameters to each task. This is part of having a full and granular control on the parallelism that asyncflow can offer.  
Both `@flow.executable_task` and `@flow.function_task` can do that by passing `task_description` to your function `kwargs` during task definition or task invocation.

```python
@flow.function_task
async def mpi_func(task_description={'ranks': 8, 'type': 'mpi'}):
    """
    multi-rank task that executes on N ranks in parallel.
    """
    # Initialize MPI communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Each rank does work here
    print(f"[Task1] Rank {rank}/{size} running on host: {os.uname().nodename}")

    # Example: distributed computation
    local_value = rank * 2
    total_sum = comm.allreduce(local_value, op=MPI.SUM)

    # Only rank 0 prints or returns results
    if rank == 0:
        print(f"[Task1] Total sum across {size} ranks = {total_sum}")
        return {"sum": total_sum}
    else:
        return None
```

!!! note
    Specifying `task_description` keys and values depends on the corresponding `ExecutionBackend used`. The example above reflects the usage of `RadicalExecutionBackend`.
    For more information about what each task accepts as a resource parameters, please refer
    to the corresponding runtime system (`ExecutionBackend`) documentation.

!!! note
    If the same `task_description` key (for example, `'ranks'`) is specified both during task definition and again at invocation time, the most recent value — provided at invocation — will take precedence and override the previous one.


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
backend = RadicalExecutionBackend({
    'resource': 'ornl.summit',
    'queue': 'gpu',
    'nodes': 100,
    'gpus_per_node': 6,
    'walltime': 120  # minutes
})
```

### Large-Scale CPU Configuration

```python
# Configure for massive CPU parallelism
backend = RadicalExecutionBackend({
    'resource': 'tacc.frontera',
    'queue': 'normal',
    'nodes': 1000,
    'cores_per_node': 56,
    'walltime': 240  # minutes
})
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
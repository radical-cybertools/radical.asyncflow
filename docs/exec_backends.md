# Execution Backends & HPC Integration

AsyncFlow's architecture follows a **separation of concerns** principle, completely isolating the execution backend from the asynchronous programming layer. This **plug-and-play (PnP)** design allows you to switch between different execution environments with minimal code changes — from local development to massive HPC clusters.

## The Power of Backend Abstraction

By design, AsyncFlow enforces that the execution backend should be entirely isolated from the asynchronous programming layer. This means you can seamlessly transition your workflows from:

- **Local development** with thread pools
- **HPC clusters** with thousands of nodes
- **GPU clusters** for accelerated computing

**The best part?** Changing execution backends requires modifying just **one line of code**.

## Built-in Backends

AsyncFlow ships with two built-in backends:

- `LocalExecutionBackend` — executes tasks locally using Python's `ThreadPoolExecutor` or `ProcessPoolExecutor`
- `NoopExecutionBackend` — no-op backend for testing and `dry_run` mode

## HPC Backends via RHAPSODY

For HPC execution, install [RHAPSODY](https://radical-cybertools.github.io/rhapsody/):

```bash
pip install rhapsody-py
```

RHAPSODY is a dedicated HPC execution layer that provides additional backends, all of which [integrate immediately with AsyncFlow](https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration) — no changes to your workflow logic are needed:

- `DaskExecutionBackend` — parallel computing via [Dask](https://docs.dask.org/en/stable/)
- `DragonExecutionBackend` — high-performance distributed execution on HPC allocations via [Dragon](https://dragonhpc.github.io/dragon/)

## Data-Linking Capabilities

Each backend declares whether it moves data between tasks, through two class attributes:

| Attribute | When `True`, AsyncFlow calls |
|---|---|
| `supports_implicit_data_linking` | `link_implicit_data_deps(src_task, dst_task)` for each dependency that declares no output files (when `implicit_data=True`) |
| `supports_explicit_data_linking` | `link_explicit_data_deps(...)` for each matching `OutputFile` → `InputFile`, and for external input files. The backend must return a descriptor containing at least `"target"`. |

Both default to `False`, and AsyncFlow skips the corresponding hooks entirely for backends that don't declare them. No built-in or RHAPSODY backend currently declares either: they all share a filesystem between tasks. A backend object without these attributes is treated as one that does not link data.

!!! note "Custom backends"
    Leave both attributes `False` if your tasks share a filesystem or your backend moves no files. Set one to `True` only if you implement the matching hook. The two are independent. See [Task Dependencies](dependencies.md#data-file-dependencies) for how file dependencies are declared.

## Local vs HPC Execution: A Side-by-Side Comparison

### Local Execution with LocalExecutionBackend

```python
# Local execution with threads (built-in)
from concurrent.futures import ThreadPoolExecutor
from radical.asyncflow import LocalExecutionBackend

backend = await LocalExecutionBackend(ThreadPoolExecutor())
```

### HPC Execution with DragonExecutionBackend

```python
# HPC execution with Dragon (requires RHAPSODY)
from rhapsody.backends import DragonExecutionBackend

backend = await DragonExecutionBackend()
```

!!! success
    **One line change** transforms your workflow from local thread execution to distributed HPC execution across thousands of nodes.

## Complete HPC Workflow Example

Below is a complete example demonstrating how to execute workflows on HPC infrastructure using `DragonExecutionBackend` from RHAPSODY.

### Setup for HPC Execution

```python
import time
import asyncio
import multiprocessing as mp
from rhapsody.backends import DragonExecutionBackend
from radical.asyncflow import WorkflowEngine

mp.set_start_method("dragon")  # run the script with: dragon my_workflow.py

# HPC backend configuration
backend = await DragonExecutionBackend(batch_kwargs={"num_nodes": 1})  # (1)!
flow = await WorkflowEngine.create(backend=backend)
```

1. Configure for HPC execution - the same backend runs on a laptop or across the nodes of an HPC allocation

!!! tip
    **HPC Resource Configuration**: Dragon runs inside the allocation it is launched in:
    - Start the script with the `dragon` launcher (`dragon my_workflow.py`), not `python`
    - Tune the underlying Dragon `Batch` with `batch_kwargs` (for example `num_nodes`)
    - Set per-task resources through `task_description` with `process_template` (one process) or `process_templates` (multi-process/MPI jobs)
    - See RHAPSODY's [Resource Specification Guide](https://radical-cybertools.github.io/rhapsody/getting-started/resource-specification/) for every option

### Define Executable Tasks for HPC

```python
@flow.executable_task  # (1)!
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


### Capture stdout/stderr to Files

By default, stdout and stderr from executable tasks are collected in memory as strings
(accessible via `task.stdout` / `task.stderr`, and the awaited future resolves to the stdout
string). For long-running tasks or large outputs, set `capture_stdio=True` on the decorator
to redirect output directly to files — zero in-memory buffering.

```python
flow = await WorkflowEngine.create(
    backend=backend,
    uid="my-run",
    work_dir="/scratch/logs",  # files land under /scratch/logs/my-run/
)


@flow.executable_task(capture_stdio=True)
async def simulate():
    return "/bin/bash -c './simulate.sh --steps 10000'"


stdout_path = await simulate()
# stdout_path → "/scratch/logs/my-run/task.000001.stdout"
print(open(stdout_path).read())
```

When `capture_stdio=True`, the awaited future resolves to the **file path** rather than the
decoded string. `work_dir` and `uid` are set on `WorkflowEngine.create()`; files are named
`{work_dir}/{uid}/{task_uid}.stdout` and `.stderr`.

!!! note "Backend support"
    `capture_stdio` is honoured by all RHAPSODY backends (`ConcurrentExecutionBackend`,
    `DaskExecutionBackend`, `DragonExecutionBackendV3`). The built-in `LocalExecutionBackend`
    does not support it — use a RHAPSODY backend for file-based I/O.

!!! note "Function tasks"
    `capture_stdio` only applies to executable tasks. Function tasks (`@flow.function_task`)
    ignore it regardless of the flag value.

---

### Assign Resources for your application (task)

Depending on the `ExecutionBackend` used, AsyncFlow supports passing and assigning multiple/different task and resources parameters to each task. This is part of having a full and granular control on the parallelism that AsyncFlow can offer.
Both `@flow.executable_task` and `@flow.function_task` can do that by passing `task_description` to your function `kwargs` during task definition or task invocation.

```python
@flow.function_task
async def mpi_func(task_description={"process_templates": [(8, {})], "pmi": "pmix"}):
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
    Specifying `task_description` keys and values depends on the corresponding `ExecutionBackend used`. The example above reflects the usage of `DragonExecutionBackend` from RHAPSODY.
    For more information about what each task accepts as a resource parameters, please refer
    to the corresponding runtime system (`ExecutionBackend`) documentation.

!!! note
    If the same `task_description` key (for example, `'process_templates'`) is specified both during task definition and again at invocation time, the most recent value — provided at invocation — will take precedence and override the previous one.


### Define Workflow with Dependencies

```python
async def run_wf(wf_id):
    print(f"Starting workflow {wf_id} at {time.time()}")

    # Create dependent task execution
    t3 = task3(task1(), task2())  # (1)!
    await t3  # Wait for distributed execution to complete

    print(f"Workflow {wf_id} completed at {time.time()}")
```

1. Task3 depends on both task1 and task2 completion, but they execute on HPC nodes

!!! note
    **Dependency Handling**: AsyncFlow automatically handles task dependencies across distributed HPC nodes, ensuring proper execution order while maximizing parallelism.

### Execute Multiple Workflows on HPC

```python
start_time = time.time()

# Execute 5 workflows concurrently across HPC infrastructure
await asyncio.gather(*[run_wf(i) for i in range(5)])  # (1)!

end_time = time.time()
print(f"\nTotal time running asynchronously is: {end_time - start_time}")

# Proper cleanup of HPC resources
await flow.shutdown()
```

1. All workflows execute concurrently across available HPC nodes

??? "HPC execution log"
    ```text
    DragonExecutionBackend started
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
    DragonExecutionBackend: All tasks completed, cleaning up resources

    Total time running asynchronously is: 1.79
    ```

## HPC vs Local Development: Key Differences

| Aspect | LocalExecutionBackend | DragonExecutionBackend (RHAPSODY) |
|--------|----------------------|------------------------------------|
| **Scale** | Single machine, limited cores | Thousands of nodes, massive parallelism |
| **Memory** | Local system RAM | Distributed memory across nodes |
| **Storage** | Local filesystem | High-performance parallel filesystems |
| **Task Type** | Python functions | Shell executables, compiled programs |
| **Use Case** | Development, testing | Production HPC workloads |
| **Install** | Built-in | `pip install "rhapsody-py[dragon]"` |

## Advanced HPC Configurations

Request the nodes from your batch scheduler (for example `salloc -N 100`), then start the
workflow inside the allocation with `dragon my_workflow.py`.

### Large-Scale Multi-Node Configuration

```python
from rhapsody.backends import DragonExecutionBackend

# Spread tasks across the nodes of the allocation
backend = await DragonExecutionBackend(
    batch_kwargs={
        "num_nodes": 100,
        "scheduler_workers": 16,
    }
)
```

### MPI Configuration

```python
from rhapsody.backends import DragonExecutionBackend

# Default PMI for every multi-process (MPI) job
backend = await DragonExecutionBackend(pmi="pmix")


@flow.executable_task
async def simulation(task_description={"process_templates": [(256, {})]}):
    return "./simulate --steps 10000"
```

### GPU Placement

Pin processes to GPUs with Dragon's `Policy`, passed in each task's `process_template`:

```python
from dragon.infrastructure.policy import Policy

gpu0 = Policy(
    placement=Policy.Placement.HOST_NAME, host_name="node001", gpu_affinity=[0]
)


@flow.function_task
async def train(task_description={"process_template": {"policy": gpu0}}): ...
```

See RHAPSODY's [Resource Specification Guide](https://radical-cybertools.github.io/rhapsody/getting-started/resource-specification/) for PMI, `libpmix.so` and GPU-affinity details.

!!! warning
    **Resource Management**: Always call `await flow.shutdown()` to properly release HPC resources and prevent job queue issues.

## Real-World HPC Use Cases

**Scientific Computing**: Execute thousands of simulations across supercomputer nodes for climate modeling, molecular dynamics, or astrophysics calculations.

**Machine Learning**: Train large models across GPU clusters with distributed data processing and model parallelism.

**Bioinformatics**: Process genomic datasets across hundreds of nodes for sequence alignment, variant calling, or phylogenetic analysis.

**Engineering Simulation**: Run computational fluid dynamics, finite element analysis, or structural optimization across distributed computing resources.

!!! tip
    **Development Strategy**: Start with `LocalExecutionBackend` for local development and testing, then install [RHAPSODY](https://radical-cybertools.github.io/rhapsody/) and switch to any RHAPSODY backend for production HPC runs. See the [AsyncFlow integration guide](https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration) for details.

## The AsyncFlow Advantage

AsyncFlow's backend abstraction means your workflow logic remains **identical** whether running on:
- Your laptop with 8 cores
- A university cluster with 1,000 nodes
- A national supercomputer with 100,000+ cores
- GPU clusters with thousands of accelerators

This **write-once, run-anywhere** approach dramatically reduces the complexity of scaling computational workflows from development to production HPC environments.

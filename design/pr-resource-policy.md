<!--
Title: Scheduler: resource-aware admission (stacked on the ordering-policies PR)
Base:  feature/ordering-policies (its PR is not open yet)
Head:  feature/resource-policy (local only, not pushed)
Commits:
  6285975 Policies: resource-aware admission (ResourcePolicy)
  22d4882 Merge feature/ordering-policies into feature/resource-policy
          (brings main #106-#108 up the stack; local, not pushed)
  6f16556 Merge feature/ordering-policies into feature/resource-policy
          (brings #110's review fixes, 0157751; local, not pushed)
-->

## Summary

Stacked on the ordering-policies PR. Adds `ResourcePolicy`, a policy behind the
same `policy=` hook that starts a task when what it declares fits its backend:
cores, a share of a GPU or whole GPUs, host and GPU memory. This is stage S1 of
the policy-layer design.

```python
from radical.asyncflow import MemoryModel, NodeShape, Pool, ResourcePolicy, WorkflowEngine

node = NodeShape(cpus=64, gpus=4, host_mem=256, gpu_mem=40)  # memory in GiB
policy = ResourcePolicy(Pool([node] * 8, mode="node"), memory=MemoryModel())
flow = await WorkflowEngine.create(backend=backend, policy=policy)

@flow.function_task
async def fold(seq):
    ...

await fold(seq, task_description={"resources": {"gpus": 0.25, "gpu_mem": 9}})
```

```text
radical.asyncflow.resources
  NodeShape       cpus, gpus, host_mem and gpu_mem (GiB, gpu_mem per GPU; None: not
                  counted), cpu_overcommit, name (the host name a placement reports)
  Pool            one backend's nodes and what admitted tasks hold; "aggregate" or "node"
  MemoryModel     peak memory per kind of task, learned from the peaks backends report
  ResourcePolicy  the policy operations over one Pool, or a Pool per backend
```

```text
enqueue(task)
  demand = task_description["resources"]       # cpus (default 1), gpus, host_mem, gpu_mem
  can never fit the pool  → raise              # the engine fails the task
  wait in the queue of the task's kind         # kind = function, backend, cpu/gpu shape

dispatch()
  try the oldest waiting task of each kind, oldest first
    aggregate: fits the pool's free totals?
    node:      fits one node, and one GPU for a share?  pick the emptiest
               → task["placement"] = {"node": i, "host": name, "gpus": [j, ...]}
  a task passed over starve_limit times: younger tasks wait for it

release(task, observed)
  give the demand back
  learn the kind's memory from observed peaks; charge its running tasks the estimate
```

- **Opt-in.** Nothing changes without a `ResourcePolicy`. The one engine change
  is on the policy path: a policy may now reject a task by raising from
  `enqueue`, and the engine fails that task and its dependents. The `Policy`
  protocol documents this.
- **`placement` is advice.** A backend that understands it places the task
  there; others ignore it. RHAPSODY's Dragon backend does not understand it yet;
  a separate RHAPSODY PR (`feature/dragon-placement`) adds that, and on rust
  every placed task ran on the node and GPU the policy chose. Without it, on
  Dragon, node mode keeps the right totals but Dragon still picks the node, so
  per-node accounting can be wrong.
- **Learned memory needs `observed`.** No released RHAPSODY backend reports
  peaks yet (a local RHAPSODY branch reports them for Dragon tasks), so declare
  memory for now. With a `MemoryModel`, until a kind of task has
  reported its peak, only `unknown_per_node` such tasks run per node. A learned
  peak above what one task can get is capped, so the task runs alone instead of
  never.
- **Units.** Memory in GiB. A GPU share is counted in eighths of a device.
- **Not ported from the prototype:** its count mode (`FifoPolicy` covers it) and
  arbitrary counted resources.

## Evidence

**Tests.** `tests/unit/test_resources.py`, 26 tests: configuration and demand
validation; demands that can never fit (node and aggregate); aggregate and node
admission; GPU sharing by slices and memory; whole GPUs; host memory; a task
that never ran giving its demand back; starvation; pools per backend; learned
memory with recharge, the cap, and uncounted memory; and the engine placing
tasks, learning memory and failing a task that can never fit.

```text
test_node_mode_shares_gpus_by_slices_and_memory
  one node, 2 GPUs of 40 GiB; 9 tasks of a quarter GPU and 9 GiB
  expect: 8 admitted, 4 per GPU (slices allow 4, memory 36 of 40 GiB); the 9th waits

test_engine_places_tasks_and_learns_memory
  node "n0": 8 cpus, 2 GPUs; MemoryModel; backend reports a 30 GiB peak per task
  4 half-GPU tasks → placed on n0, two per GPU
  3 tasks without declared memory → one at a time until the first reports 30 GiB,
  then two at once on the 64 GiB node
```

I removed seven pieces one at a time (rejection in the engine, placement,
recharge, the memory cap, the starvation guard, the unknown-memory limit, the
can-never-fit check), and each removal fails a test. Full unit suite with
rhapsody-py 0.6.0 and its telemetry extra: 228 passed (202 + 26) on macOS with
Python 3.9 to 3.14, and with `PYTHONASYNCIODEBUG=1` on Python 3.13, where
asyncio reported no non-thread-safe call. ruff and pre-commit are clean.

**On a real backend.** `LocalExecutionBackend` with 16 threads, 12 tasks of 2
cores that sleep 0.1 s each (AMD EPYC 7502P, Python 3.14, one pinned core, 3
runs):

| | Makespan | Most running at once |
|---|---|---|
| No policy | 0.103 s | 12 |
| `ResourcePolicy`, aggregate, one node of 4 cores | 0.607 s | 2 |
| `ResourcePolicy`, node mode, two nodes of 2 cores | 0.606 s | 2 |

**Cost.** 20,000 tasks of 4 kinds (GPU shares with GPU memory, and cpu-only) on
8 nodes of 64 cores, 4 GPUs, 256 GiB and 40 GiB per GPU, each enqueued,
dispatched and released (same machine, 3 runs):

| Mode | Per task |
|---|---|
| aggregate | 14.6–15.0 µs |
| node | 27.6–27.7 µs |

The engine itself costs about 31 µs per task, so admission is negligible for
tasks that run for seconds and noticeable for millisecond tasks. It is not
optimised yet.

**On RHAPSODY, Dragon and real GPUs (rust cluster).** 24 IMPRESS-like
pipelines (rfd3 → mpnn → boltz/rfd3) for 12 minutes. Each task is a synthetic
CUDA process that holds an IMPRESS-like GPU memory footprint, declares it, and
asks for 1/8 of a GPU; a CUDA out-of-memory error ends its pipeline. Node mode
writes `placement`, and RHAPSODY's placement change (an earlier state of
`feature/dragon-placement`) runs the task on that node and GPU. Median of 3
runs.

One node, haskell, with 2 RTX 6000 Ada of 47.4 GiB (12 pipelines per GPU; the
application pins pipeline p to GPU p mod 2 unless the policy places it):

| Dragon | Admission | CUDA OOMs = pipelines lost (of 24) | Tasks per hour | GPU busy | GPU memory peak |
|---|---|---|---|---|---|
| 0.14.1 | None | 5 | 1,240 | 73% | 46.7 GiB |
| 0.14.1 | Aggregate | **0** | 1,190 | 71% | 37.3 GiB |
| 0.14.1 | Node | **0** | 1,160 | 68% | 37.3 GiB |
| 0.14.1, submissions 8 ms apart¹ | None | 4 | 1,690 | 94% | 46.2 GiB |
| 0.14.1, submissions 8 ms apart¹ | Aggregate | 0 (1 in 3 runs) | 1,600 | 93% | 43.0 GiB |
| 0.14.1, submissions 8 ms apart¹ | Node | **0** | 1,645 | 94% | 38.4 GiB |

Two nodes, one GPU each: haskell's RTX 6000 Ada (47.4 GiB) and fortran's RTX PRO
6000 Blackwell (95.0 GiB). GPU memory ×1.5, so at most 134 GiB is declared
against 141.5 GiB: the totals never refuse a task, but haskell's memory binds.
Dragon picks the node unless the policy places the task.

| Dragon | Admission | CUDA OOMs = pipelines lost (of 24) | Tasks done in 12 min | GPU busy, haskell · fortran |
|---|---|---|---|---|
| 0.14.1 | None | 16 | 311 | 84% · 75% |
| 0.14.1 | Aggregate | 15 | 320 | 81% · 86% |
| 0.14.1 | Node | **0** | **357** | **89% · 92%** |
| per-task reports² | None | 14 | 326 | 85% · 81% |
| per-task reports² | Aggregate | 16 | 322 | 76% · 82% |
| per-task reports² | Node | **0** | **370** | **93% · 97%** |

¹ Each task in a launch round of its own, so Dragon 0.14.1 reports it when it
ends. ² A local Dragon branch that reports each task when it ends; not
released.

- **Node mode: no CUDA OOM in 12 runs,** and every task ran on the GPU the
  policy chose (1,791 tasks on one node, 2,302 on two; bus ids checked).
- **One node: admission does not raise throughput.** No admission did 3–7% more
  tasks per hour despite its lost pipelines: it ran up to 12 tasks per GPU,
  against the 8 that the declared 1/8 share allows.
- **Two nodes of unequal GPUs: aggregate admission is no better than none,**
  with 13–17 OOMs per run, 178 of the 180 on the smaller GPU. Dragon sends half
  the tasks to each node without looking at GPU memory. Node mode removes the
  OOMs and does 12–15% more work, with the GPUs 89–97% busy.
- **No latency cost per task on Dragon:** 57.1 ms per dependent task with no
  policy, aggregate or node mode (julia, 32 workers, capacity 32). A placed
  function task, though, runs as its own Dragon process: 693 ms per dependent
  task and 28 trivial tasks per second. Placement is for GPU and executable
  tasks.
- **Not yet on Dragon:** learned memory (no released backend reports peaks),
  more than two nodes, and two GPUs per node.
- **E2 on PSC Bridges-2's V100 nodes has not run yet.** With Dragon 0.14.1, 48
  `Batch` workers on 20 cores (32 pipelines on 4 GPUs) stalled the run or made
  Dragon fail to launch tasks, in 4 of 4 runs. Up to 32 workers on 20 cores ran
  clean, at 8 tasks per GPU and with no CUDA OOM. So the limit lies between 1.6
  and 2.4 workers per core. A 40-core, 8-GPU node should then run 8 tasks per
  GPU with 64 workers; E2 at that density is not run.

**Why node mode at scale.** From the S1 prototype's simulation of IMPRESS on
NCSA Delta (`design/policy-s1-results.md`). The model reproduces IMPRESS's
measured GPU-saturation ladder on one node within a few percent. Its 8-node
behaviour rests on a model of Dragon's placement; the two-node runs above show
the same mechanism (Dragon picks the node without looking at GPU memory), but 8
nodes are not checked against a real run.

- **One node:** admission removes the out-of-memory kill and the need for the
  hand-tuned staggered start, at the stagger's throughput (+3% per node-hour).
- **Eight nodes, aggregate:** 115–183 CUDA OOMs and lost pipelines, as today,
  because Dragon picks the node and the application the GPU, so totals that fit
  still overload single GPUs.
- **Eight nodes, node mode:** no failures, 28% more passing folds than the
  staggered start, 0.045 instead of 0.058 GPU-hours per fold.
- **Learned memory:** within 1–3% of declared memory.

## Merge Danger

**Door:** two-way until released

Opt-in, and reverting the commit restores the ordering-policies branch. Once
released, `ResourcePolicy`, `Pool`, `NodeShape`, `MemoryModel`, and the
`resources`, `placement` and `observed` keys are public API, and the last two
are a convention RHAPSODY's backends rely on.

**Blast Radius:** opt-in API

- **Without a `ResourcePolicy`:** no change. The engine change runs only when a
  policy is set.
- **With one:** a wrong declaration (memory in MB instead of GiB, say) holds
  tasks back or fails them. A task that can never fit fails at once with a
  `ValueError`, instead of waiting forever.
- **Merge order:** after #91, #100, #110 and the ordering-policies PR. The
  RHAPSODY placement PR can merge in either order.

Generated with Matteo's hands and Claude Code

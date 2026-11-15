# asyncflow policy layer: redesign (draft 1)

Status: design draft, 2026-10-23. No code yet.
Builds on the tickless engine (`feature/tickless-scheduler`) and replaces the
prototype policy layer (task-count capacity with `fifo`, `lineage` and `fair`
policies) evaluated in the PR #91 review.

## 1. Requirements

From the application requirements:

| ID | Requirement |
|---|---|
| R1 | Run CPU and GPU executables and Python functions: single or multi-core, single or multi-GPU, MPI or not |
| R2 | Run different task types: compute, AI, services |
| R3 | Task durations from milliseconds to hours |
| R4 | Tasks arrive at any time during the run |
| R5 | Tasks are independent or form a DAG of control and data relations |
| R6 | Tasks read and write files, and possibly streams or other channels |

From the workload model (`workload_model/`):

| ID | Derived requirement | Evidence |
|---|---|---|
| W1 | Admission must be bounded by memory, not only by devices | IMPRESS: 9.5 GB host RAM per rfd3 task; node OOM at 32 simultaneous starts; CUDA OOM at 8 per GPU |
| W2 | GPUs must be shareable by fractions | IMPRESS GPUs are 9-15% busy at one pipeline per GPU and saturate at about eight |
| W3 | Control-plane work (adaptive callbacks, selection) must never wait behind compute | IMPRESS: a blocking callback cut throughput to 37%; healthy callbacks are 3 ms p50, 41-140 ms p99 |
| W4 | Dragon is the production backend, as it is today | user direction; IMPRESS and MATEY run on Dragon |
| W5 | Without a configured policy, behaviour is unchanged | existing users and tests |
| W6 | A scheduling decision costs far less than the shortest task | millisecond tasks (R3); target under 20 µs per admission on one core |

Non-goals: node-level placement inside a backend that does its own placement;
application checkpointing; scheduling across engines, allocations or facilities
(campaign level); changing Dragon's internals (we list what we would ask for).

## 2. What exists today

**asyncflow.** The engine resolves dependencies and submits every ready task at
once to the backend named by `backend=`. `task_description` is opaque and passed
through. Blocks run as coroutines in the engine. `service=True` needs a backend
that passes a service callback; rhapsody's Dragon and Concurrent backends do not,
so only the deprecated RP backend supports it. `prompt_task` goes to AI backends.

**rhapsody's Dragon backend over Dragon Batch.** Batch's client compiler assigns
each single-node task to a per-node manager using a critical-path weight (1 per
function or serial process, 100 x processes for a parallel job), current load,
and locality to argument producers. It does no GPU or memory accounting. A
central Batch scheduler grants whole-node allocation phases to per-node managers
and node allocations to multi-node jobs. A manager releases its nodes only when
its phase drains. `Batch.options()` takes reads, writes, name, timeout, stdout,
stderr and pmi, but no priority. asyncflow submits only ready tasks, so Batch's
critical-path ordering sees no edges. A process without `gpu_affinity` gets every
GPU on its node, so applications pin GPUs themselves (IMPRESS). Batch honours an
explicit host in a process template's policy, but that bypasses its allocation
accounting.

**rhapsody.** Backends declare only data-linking capabilities. There is no
resource discovery API. The vLLM backend is itself an inference service. Data
backends (Dragon dictionary, Redis) expose endpoints but are not connected to
asyncflow's dependency tracking.

**The prototype.** One global capacity counted in tasks, no resource shapes, no
gangs, no services, no duration handling, and no priority inheritance.

## 3. The split between asyncflow and the backend

asyncflow decides *whether* a task may enter a backend now (admission) and
*which* ready task goes first (order), because only asyncflow knows the DAG,
workflow groups, priorities, services and duration history. The backend decides
*where* the task runs (placement) inside what it was given, and runs it.

| Concern | Owner today | Owner in this design | With Dragon as it is | Ask to Dragon |
|---|---|---|---|---|
| Readiness (dependencies) | asyncflow | asyncflow | same | optional: accept edges between Batch tasks |
| Admission into the backend | nobody (all ready tasks submitted) | asyncflow, per backend pool | asyncflow withholds work | none |
| Order among admitted work | Batch critical-path heap per manager | asyncflow; backend order inside the small admitted window | asyncflow's window is small, so its order dominates | priority hint in `Batch.options()` |
| Node placement, single-node work | Batch client compiler | backend | Batch | report placement at task start |
| GPU device choice | application (IMPRESS) | backend if it accounts GPUs, else asyncflow's device shim | asyncflow shim, collisions possible | GPU accounting, or a `gpus` request in options |
| Node allocation for MPI gangs | Batch scheduler | backend; asyncflow decides when to submit and reserves capacity | Batch | none |
| Service hosting | RP only | backend, as a lease | Dragon native processes outside Batch phases | confirm, or provide a service API |
| Suspend and resume | none | asyncflow decides, backend executes | not possible through Batch | handle to the task's Process/ProcessGroup |

Rules that follow from the split:

- asyncflow never admits more than a pool's ledger allows, plus an optional
  prefetch per pool to hide the submit round trip. It defaults to 0: the
  prototype gained nothing from it on a local backend, but it may matter on
  Dragon, where completions reach asyncflow through rhapsody's monitor thread,
  which polls Batch with a 50 ms timeout.
- Where the backend cannot place a resource dimension, asyncflow assigns it at
  the finest level the backend lets it express (today: `gpu_affinity` device
  indexes on Dragon), and says so in the pool's capabilities.
- With no policy configured, asyncflow submits everything at once, as today (W5).

## 4. Resource model

### 4.1 Resource vectors

A resource vector has these dimensions:

| Dimension | Unit | Notes |
|---|---|---|
| `cpu` | cores | logical processors; overcommittable, because thread caps are not reservations (S1 prototype: IMPRESS's 8-thread caps counted as cores would admit a quarter of its operating point) |
| `gpu` | device fractions | granularity 1/8 by default, so 8 tasks can share a GPU (W2) |
| `host_mem` | GiB | required for admission when known (W1) |
| `gpu_mem` | GiB | bounds GPU sharing |
| `nodes` | whole nodes | only for exclusive multi-node gangs |
| counted resources | units | user-declared, e.g. `license:openeye` for MLDocking docking |

### 4.2 Task demand

A new backend-agnostic key in `task_description` states demand:

```python
task_description={"resources": {"cpus": 8, "gpus": 0.125, "host_mem": 9.5,
                                 "ranks": 1, "nodes": None, "licenses": {}},
                  "expected_runtime": 110}   # optional hint, seconds
```

The backend adapter translates it into backend-specific fields (Dragon
`process_template` / `process_templates` with ranks, `Policy(gpu_affinity=...)`).
When `resources` is absent, asyncflow infers what it can from backend-specific
fields (sum of `nranks`, length of `gpu_affinity`, `OMP_NUM_THREADS`) and
otherwise assumes `{"cpus": 1}`.

Memory is the dimension users know least. asyncflow learns peak resident memory
per task kind from telemetry (ER7) and uses the learned value when no demand is
given. A task kind is (function name, backend, resource shape). Until enough
samples exist, memory is not used for admission and a per-pool count limit on
unknown-memory tasks applies instead.

### 4.3 Pools and ledgers

One pool per backend, as named by `backend=` today. A pool's capacity is a list
of node shapes (homogeneous nodes first). It comes from a new backend method,
`describe_resources()`, or from user configuration.

Each pool keeps:

- an aggregate ledger: the sum of admitted demand per dimension. Always kept.
- a per-node ledger for the dimensions asyncflow places itself (the GPU shim),
  and for every dimension when the backend reports where each task started.

Aggregate admission can admit work that fits in total but on no single node.
On a backend that accounts for every dimension it places, the backend then queues
that work, which delays it but breaks nothing. On Dragon today it breaks things:
Batch places nodes without counting GPUs or memory, and the application pins GPUs,
so totals that fit still overload single GPUs and nodes. In the S1 prototype
(`policy-s1-results.md`), aggregate admission on 8 IMPRESS nodes lost 115-183 of 256
pipelines to CUDA OOM; per-node placement lost none. So for co-located GPU work on
Dragon the pool runs in node mode: asyncflow chooses host and GPU and passes them as
a process `Policy`. This stays until Dragon accounts for GPUs and memory or reports
placement (asks 1 and 4).

## 5. Ordering: entities, EEVDF and charging

### 5.1 Scheduling entities

Entities are workflow groups arranged as a tree: the engine at the root,
workflow scopes and blocks as interior nodes, tasks as leaves. A task with no
scope belongs to its lineage, the oldest ancestor. Each entity has a weight,
default 1.

Today `workflow_scope(workflow_id)` only tags tasks with a flat id (a context
variable, so an inner scope replaces the outer one), and blocks are not tracked as
groups. The tree needs two additions: a `weight=` argument, and a parent link when
scopes or blocks nest.

### 5.2 EEVDF across entities

At every level of the tree the next entity is chosen by EEVDF: among eligible
entities (non-negative lag) the one with the earliest virtual deadline, where

    virtual deadline = vruntime + expected request / weight
    expected request = dominant share x expected duration of the entity's next task
    dominant share   = max over dimensions d of demand_d / capacity_d

Charging dominant-resource-seconds makes fairness meaningful across a GPU
workflow and a CPU workflow (Dominant Resource Fairness). Using the expected
request gives short work an earlier deadline at the same weight, which a CFS
policy cannot do (it only rewarded entities that had slept). Charges are made at
admission from the estimate and corrected with the observed duration at
completion, so misestimates move lag rather than accumulate.

Tasks and workflows arrive at any time (R4). An entity that appears, or becomes
runnable again after having nothing ready, joins at the current average virtual
time of its siblings with zero lag. Neither a new workflow nor one that waited on
I/O can claim credit for time it was absent, and neither is pushed behind the
others.

Inside an entity, order is by effective priority (section 8), then lineage, then
readiness time.

### 5.3 Estimates

Duration estimates are an exponential average per task kind, with
`expected_runtime` overriding it. Unknown kinds are treated as the "medium" class
(section 6). Estimates feed EEVDF requests, duration classes and gang
reservations, so their quality is measured (predicted vs observed, per kind) and
reported.

### 5.4 Cost

Runnable entities sit in heaps keyed by virtual deadline per tree level, so a
decision is O(depth x log entities). With depth at most 3-4 in the exemplars this
fits the 20 µs target (W6). The prototype's capped policies (FIFO, lineage and
flat fair) cost 1-4% makespan against no policy, from extra engine work on the one
core the engine runs on (512-diamond benchmark, PR #91 review).

## 6. Duration classes

Classes by expected duration of the task kind, thresholds configurable:

| Class | Default bound | Typical tasks |
|---|---|---|
| control | runs in the control plane | adaptive callbacks, in-process analysis |
| interactive | < 1 s | per-request inference, micro-tasks |
| short | < 60 s | docking, MPNN, Rosetta stages |
| medium | < 1 h | rfd3 and Boltz under sharing, simulations |
| long | >= 1 h | multi-node training, long simulations |

Rules:

- **Control work never enters a compute pool.** It runs on a dedicated local
  backend (as IMPRESS already does with a thread pool) with its own small
  capacity, so it cannot queue behind compute (W3).
- **Long work has a share cap.** While shorter classes have runnable work, long
  tasks may hold at most a configurable fraction of a pool's dominant capacity
  (default 0.75). When nothing shorter is waiting they may fill the pool, so the
  cap is work-conserving. Without preemption this is the only way to stop
  millisecond and second tasks from waiting hours behind long tasks (R3).
- **Within the cap, EEVDF orders the classes** through their requests (5.2).

Static per-class pools were rejected because they leave capacity idle and
fragment small allocations.

## 7. MPI gangs

Two kinds of MPI work:

- **Single-node MPI**: ranks within one node. This is an ordinary multi-core
  demand.
- **Multi-node gangs**: `nodes = k` exclusive, or ranks spread across nodes. On
  Dragon, Batch's scheduler allocates the nodes; asyncflow cannot and should not
  place them.

asyncflow's job for gangs is to submit them at the right time and protect them
from starvation:

1. The pool ledger counts whole nodes for gangs separately from fractional
   per-node work.
2. When the highest-priority runnable item is a gang that does not fit, the
   pool records a **reservation** for k nodes. From then on, asyncflow admits
   only **backfill** work: tasks whose estimated completion falls before the
   reservation's estimated start, computed from the expected completions of
   running work.
3. With no usable estimates, the reservation drains: asyncflow stops admitting
   single-node work until k nodes' worth of capacity is free, with an aging bound
   so a stuck gang cannot block a pool forever.
4. Withholding work makes Batch's per-node allocation phases drain, managers
   release their nodes, and Batch's scheduler can grant the gang. This uses
   Batch's existing behaviour; it needs no Dragon change. Batch also orders
   deferred allocation requests by descending task weight, and a parallel job's
   weight (100 x processes) is far above a phase of functions (1 each), so a
   gang is granted before new single-node phases once nodes free up.

Suspending a gang (section 10) must stop every rank at once, through Dragon's
`ProcessGroup.send_signal`.

## 8. DAG priority inheritance

Priorities come from `task_description["priority"]` (an integer class) and
workflow weights. Without inheritance, a high-priority task waiting on a queued
low-priority ancestor waits for that ancestor's turn: priority inversion, which
any DAG will hit once priorities are user-facing.

Effective priority:

    eff(t) = max(own(t), max over pending dependents d of eff(d))

computed over *pending* ancestors only. It is updated when a dependent registers
(walk up the pending ancestors, raising each until nothing changes) and when a
dependent finishes or is cancelled (each task keeps a count of donations per
priority level, so reverting is O(1) per edge). The engine already keeps both
edge directions for pending components (`dependencies`, `_dependents_map`).

Across entities, inheritance is **donation**: when a task runs early because a
dependent in another workflow raised it, its service is charged to the donor's
entity. The boosting workflow pays for the boost, so fairness accounting stays
truthful and no workflow can jump the queue by depending on others.

Inheritance also reaches services (a high-priority task waiting on a service
raises the service's start) and gangs (it raises the reservation's priority).

Cost is bounded by the number of pending ancestors. In the exemplars chains are
short (IMPRESS about ten steps per pass), so a depth limit with a logged
fallback is enough.

## 9. Services

Three kinds, one model:

| Kind | Example | Today |
|---|---|---|
| Backend as service | rhapsody vLLM inference backend | separate backend; its GPUs are invisible to any policy |
| User service component | `service=True`; MATEY compute-side service | RP only |
| Data service | Dragon dictionary, Redis | data backends, outside asyncflow |

A service is a **lease**: admitted like a task (demand, priority, entity), but it
holds its resources until stopped. Lease holdings reduce the pool capacity
available to tasks. Requests to a service do not consume pool capacity; they
consume the service's own concurrency (its batching, such as vLLM continuous
batching, stays inside the service).

Lifecycle: pending, starting (startup cost, EC5), ready, draining, stopped.
Dependents of a service wait for *ready*, which matches asyncflow's current
service future semantics.

Policies:

- **Keep warm or release.** Release an idle service once its idle time exceeds
  its measured startup cost (the ski-rental rule), unless it is pinned. MATEY's
  startup path and ExaServe's ~30 min bring-up at 256 nodes make this decision
  matter.
- **Replicas** (optional): scale on request queue length against a latency
  target (EQ3).

On Dragon a service should not run inside a Batch allocation phase, because a
phase that never drains keeps its node from being released (section 7). Services
should run as Dragon native processes placed on hosts the pool reserves, with
their resources in the ledger. This needs rhapsody support and confirmation from
the Dragon team.

## 10. Preemption

Off by default. Backends declare `suspend` (SIGSTOP, cgroup freezer, Dragon
`send_signal`), `kill_requeue` (for tasks declared idempotent), or nothing.

Victim selection uses the ledger. Suspension frees CPU and GPU compute but not
memory, so it helps only when the urgent task's binding dimension is compute.
When memory binds, as in IMPRESS, the option is kill-and-requeue of restartable
tasks with the least elapsed time. Gangs are suspended whole or not at all, and a
suspended task's backend timeout must be extended.

## 11. Data (R6)

Mostly outside the scheduler; three hooks:

- **File edges** (optional): a task with `InputFile(x)` depends on the registered
  task that declared `OutputFile(x)`. Today files never create edges, so they
  also need a future to be ordered.
- **Locality hints**: pass the producing task's placement (when the backend
  reports it) to the backend for the consumer. Dragon's locality heuristic only
  sees argument edges, which asyncflow resolves before submission.
- **Streams**: model a stream as a data service (a lease with an endpoint) with
  producer and consumer components that are ready when the stream opens. The
  schema reserves an edge type `stream`; no implementation in the first stages.

## 12. Interfaces (sketch)

```text
ResourceVector   cpu, gpu (fractions), host_mem, gpu_mem, nodes, counted{}
Demand           ResourceVector + ranks, exclusive, expected_runtime, priority, kind
Pool             capacity (node shapes), aggregate ledger, per-node ledger,
                 reservations, capabilities
Entity           weight, vruntime, lag, parent, runnable heap
Lease            service handle: demand, state, endpoint, idle clock, startup cost
Policy ops       on_register(task)        inheritance, entity membership
                 enqueue(task)            task became ready
                 pick(pool) -> task|None  EEVDF + class cap + fit + reservation
                 on_start(task, placement)
                 on_finish(task, observed) charge correction, estimates, release
BackendCapabilities (new rhapsody method)
                 describe_resources()     node shapes
                 places: {cpu, gpu, mem} -> backend | shim | none
                 gangs, services, suspend, kill_requeue,
                 priority_hint, reports_placement
```

Configuration is opt-in, for example
`WorkflowEngine.create(..., scheduler=SchedulerConfig(pools=..., classes=..., policy="eevdf"))`.
Every decision emits a telemetry event (admit, defer with reason, reserve,
backfill, donate, release, preempt), and EC4 scheduling wait is split by reason:
no capacity, policy wait, reservation.

## 13. Asks

To Dragon, in priority order:

1. Report placement (host, GPU ids) when a Batch task starts, so asyncflow can
   keep a per-node ledger and detect collisions.
2. Resource introspection: per-node cores, GPUs and memory, and current use.
3. A priority or weight in `Batch.options()`, honoured by managers' ready heaps
   and allocation requests.
4. GPU accounting, or a `gpus` request in options that Batch places against free
   devices.
5. A handle to a Batch task's Process or ProcessGroup for `send_signal`, and a
   way to extend its timeout.
6. The supported way to host long-lived services next to Batch without pinning
   allocation phases.
7. Immediate completion delivery, or a tunable poll (not policy, but latency).
8. Optional: dependency edges between Batch tasks submitted by asyncflow, so
   edges between Dragon tasks resolve inside Dragon.

To rhapsody: `describe_resources()` and the capability flags; translating the
`resources` key for Dragon and Concurrent; services on Dragon; letting users
choose the PMI backend (Batch defaults to Cray PMI).

## 14. Requirements coverage

| Requirement | Covered by | Remaining limits |
|---|---|---|
| R1 executables and functions, CPU/GPU, MPI | resource vectors (4), gangs (7), backend translation (4.2) | GPU device placement is a shim until Dragon accounts GPUs or reports placement |
| R2 compute, AI, services | pools per backend (4.3), leases (9) | services on Dragon need rhapsody work and Dragon confirmation |
| R3 milliseconds to hours | duration classes and long-share cap (6), EEVDF requests (5.2), decision cost (5.4) | no preemption by default; estimates start cold |
| R4 arrival at any time | entities created on demand; new entities placed by lag (5.2) | registration ordering bug fixed by PR #109, not by this layer |
| R5 independent tasks and DAGs | inheritance with donation (8); lineage tie-break (5.1) | edges stay in asyncflow unless Dragon accepts them (ask 8) |
| R6 files, streams | file edges, locality hints, stream leases (11) | streams are a reserved design only |
| W1-W2 memory and GPU sharing | memory dimension, fractional GPUs (4.1), learned memory (4.2) | memory learning needs telemetry with peak RSS |
| W3 control plane | control class on a local backend (6) | none |
| W5 default unchanged | opt-in configuration (12) | none |
| W6 decision cost | heaps per tree level (5.4) | to be measured |

## 15. Stages and evaluation

Each stage is opt-in and measured with the performance-model metrics.

| Stage | Adds | Evaluation workload | Metrics | Data still needed |
|---|---|---|---|---|
| S0 | tickless engine (done, branch) | microbenchmarks | EC4, ER1 | none |
| S1 | resource vectors, pools, aggregate admission (FIFO), GPU shim, learned memory | IMPRESS-calibrated trace: task-count vs memory-aware co-location vs Dragon default. **Prototyped in simulation**, see `policy-s1-results.md` | EW1, ER3, ER5, ER7 | per-task peak host and GPU memory; 22726386 placement logs |
| S2 | duration classes, long-share cap, control backend rule | mixed millisecond/hour workload; MLDocking docking + training | EC4 by class, EW1 | MLDocking iteration structure |
| S3 | EEVDF over entity tree, DRF charging, priority inheritance | interactive chains beside bulk; inversion tests | EQ3, EDec2, A6 | one latency requirement from an owner |
| S4 | gang reservations and backfill | MPI gangs plus short tasks on Dragon | EC4 of gangs, ER4 | ROSE MPI task durations; a Dragon allocation |
| S5 | services as leases, keep-warm rule | vLLM backend beside tasks; MATEY startup | EC2, EC5, EQ3, ER3 | MATEY startup trace |
| S6 | preemption capability | urgent work with a deadline | A5, A6, ER8, ER9 | a deadline from an owner; Dragon handle (ask 5) |

## 16. Alternatives considered

- **Let Dragon schedule everything** by pushing the DAG and priorities into
  Batch. Not possible today (no priority, no GPU accounting, edges resolved in
  asyncflow), and Batch knows nothing of workflows, services or duration history.
  Kept as a later option through asks 3, 4 and 8.
- **Static partitions per class or backend.** Simple, but leaves capacity idle
  and fragments small allocations.
- **Full HEFT in asyncflow.** The DAG is revealed online as coroutines run, so
  upward ranks are myopic; kept only as a tie-breaker through lineage.
- **CFS-style fairness (the prototype).** Couples latency to share; EEVDF
  decouples them at little extra cost.

## 17. Risks and open questions

- **Double scheduling.** asyncflow admits against an aggregate ledger while
  Batch places by its own heuristic. Without placement reports (ask 1) asyncflow
  cannot see GPU collisions or per-node memory overload. Confirmed by the S1
  prototype: on IMPRESS at 8 nodes, aggregate-only admission does not just lose
  throughput, it fails tasks (see §4.3). Node mode avoids this, but its placement
  through `Policy(HOST_NAME)` bypasses Batch's allocation accounting, an effect
  the simulation does not model.
- **Allocation-phase churn.** Withholding work changes how Batch phases start and
  drain. Does reservation-by-withholding cause allocation chatter?
- **Cold estimates.** Durations and memory are unknown at first. Are the
  defaults safe, and how fast do estimates converge per kind?
- **Decision cost in Python.** Hierarchical EEVDF must stay under the W6 target
  at tens of thousands of tasks per second; start flat (one level) if not.
- **No latency requirement exists yet** in any exemplar. S3 and S6 cannot be
  evaluated against a real objective until an owner provides one.
- **Interplay with PR #109** (dependents waiting for dependency registration):
  independent of this layer, but inheritance needs registered ancestors.

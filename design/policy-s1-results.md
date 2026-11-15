# Policy layer S1 prototype: results

Status: prototype, 2026-10-23. Stage S1 of [policy-layer.md](policy-layer.md).
Code on the local branch `prototype/policy-s1` (from `feature/tickless-scheduler`,
uncommitted, not pushed), in a local worktree.

## Question and answer

**Question.** IMPRESS's limit is memory, not GPU count. Can per-backend
resource-vector admission keep it at its operating point without the hand-tuned
staggered start, and does that hold on 8 nodes? The admission tested counts host
memory, GPU fractions and GPU memory, with memory declared or learned. It is
compared with today's behaviour and with the earlier task-count capacity.

**Answer.**
- **On one node:** yes. Admission removes the out-of-memory kill and the need for
  the stagger, and matches the stagger's throughput (+3% rfd3 per node-hour).
- **On 8 nodes:** only if asyncflow also chooses the node and GPU.
  - Admission against pool totals fails like today, with 115–183 CUDA OOMs and
    pipelines lost. Dragon picks the node and the application pins the GPU, so
    totals that fit still overload single GPUs.
  - Per-node placement has zero failures. It delivers 28% more passing folds than
    today's staggered start, at 0.045 instead of 0.058 GPU-h per fold.
- **Learned memory** is within 1–3% of declared memory.
- **Admission cost** is 3–24 µs per decision, depending on the scenario and on
  machine load.

These are simulation results. The model reproduces IMPRESS's measured
GPU-saturation ladder within a few percent on one node. The 8-node behaviour rests
on a model of Dragon's placement that has not been checked against a real run.

## What was built

| Piece | Path on the branch | Role |
|---|---|---|
| Admission module | `src/radical/asyncflow/prototype_admission.py` | `Demand`, `NodeShape`, `Pool` (`count`, `aggregate` and `node` modes), `MemoryModel`. Pure Python, with no engine imports; this is the part that would move into asyncflow. |
| Engine hook | `src/radical/asyncflow/workflow_manager.py` (+58 lines) | `WorkflowEngine.create(..., admission={backend: Pool})`. Ready tasks wait in their backend's pool; the placement goes into the task description as `placement`; completion releases the demand together with the backend's `observed` peaks. Inactive without `admission=`. 125 unit tests pass; two fail on the installed rhapsody's missing `ConcurrentExecutionBackend`, unrelated to this hook. |
| IMPRESS model | `prototypes/policy_s1/impress_model.py` | Tools, durations, memory, the adaptive chain and overheads, each with its source or marked as estimated |
| Simulator | `prototypes/policy_s1/sim.py` | Discrete-event model: Delta nodes; GPU processor sharing; Dragon Batch placement; IMPRESS's GPU pinning; host OOM kills the job, GPU OOM fails the task and its pipeline |
| Evaluation | `prototypes/policy_s1/run_eval.py` → `results.md` | Validation plus four scenarios, 5 seeds each, 37 s in total |
| Engine smoke run | `prototypes/policy_s1/engine_smoke.py` → `engine_smoke.md` | 32 pipelines as real asyncflow coroutines, through the tickless engine and a simulated backend in compressed time |

The pool modes:

- **`count`:** the earlier prototype's capacity, counted in tasks.
- **`aggregate`:** demand must fit the pool's summed capacity. Dragon still chooses
  the node and the application the GPU.
- **`node`:** demand must fit one node and one GPU. The pool returns that placement:
  this is the "GPU shim" of the design. On Dragon it would become a process
  `Policy` with a host name and `gpu_affinity`.

Ordering is first-fit over the oldest heads of per-kind queues, with a starvation
bound. CPU threads are counted with 4× overcommit (see finding 7).

## Calibration and validation

The model is calibrated to the IMPRESS run notes (IMPRESS repo:
`examples/small_molecule_binding/plans/`):

- **Durations and the pipeline mix:** the 1-pipeline-per-GPU run 22692293.
- **The GPU share of each tool:** "~20% of an A40" in the run notes, raised to 25%,
  which fits the measured GPU busy better.
- **rfd3 host RAM:** measured (~7.5 GB actual, 9.5 GB planning number).

Three things are estimated, not measured:
- GPU memory per tool, fitted to the ladder's per-GPU peaks.
- One co-location slowdown coefficient, fitted to the ladder. Its likely causes are
  CPU contention and Dragon workers busy-spinning.
- Small per-task launch and delivery overheads.

Model against the measured ladder (1 node, 1 h, rates counted from minute 10; model
values are means of 5 seeds):

| Pipelines per GPU | rfd3 per node-h (measured / model) | rfd3 median s | GPU busy % | GPU mem peak GB | host RAM peak % |
|---|---|---|---|---|---|
| 1 | 57.8 / 56.2 | 74 / 74 | 11 / 13 | – / 6 | 27 / 26 |
| 2 | 104.6 / 101.3 | 78 / 83 | 23 / 23 | 10 / 11 | 28 / 33 |
| 4 (no stagger) | 183.3 / 187.4 | 82 / 94 | 39 / 42 | 20 / 22 | 61 / 63 |
| 6 | 255.8 / 257.3 | 96 / 102 | 60 / 58 | 25 / 30 | 50 / 57 |
| 8 | 315.5 / 313.4 | 110 / 112 | 74 / 72 | 32 / 38 | 62 / 72 |
| 8, no stagger | killed by OOM after 2 min / killed after 0.5 min | | | | |

- **mpnn rates:** within 10%.
- **boltz rates:** within 33%. They are small counts, and the gate probabilities
  for that branch are estimated.
- **Full tables:** `prototypes/policy_s1/results.md`.

## Results

The policies compared:
- **`none`:** asyncflow today, everything submitted when ready.
- **`stagger`:** today plus IMPRESS's 60 s start groups.
- **`countN`:** at most N tasks in flight per node.
- **`aggregate`:** resource vectors against pool totals.
- **`node`:** resource vectors with per-node placement and declared memory.
- **`node-nomem`:** the same with GPU fractions only.
- **`node-learned`:** the same with memory learned.

All runs last 4 h, with 5 seeds per row; values are means.

**One node, 32 pipelines (8 per GPU):**

| Policy | Failures | Passing folds | GPU-h per fold | rfd3 per node-h | GPU busy % | host peak % |
|---|---|---|---|---|---|---|
| none | job killed at 0.5 min | 0 | – | – | – | 99 |
| stagger (today) | 0 | 354 | 0.045 | 315 | 72 | 75 |
| count16 | 0 | 199 | 0.080 | 184 | 42 | 63 |
| count24 | 0 | 278 | 0.058 | 253 | 58 | 89 |
| aggregate | 0 | 333 | 0.048 | 316 | 72 | 82 |
| node-nomem | job killed at 0.5 min | 0 | – | – | – | 99 |
| **node** | 0 | 350 | 0.046 | 325 | 73 | 81 |
| **node-learned** | 0 | 360 | 0.045 | 323 | 73 | 83 |

**Eight nodes, 256 pipelines (8 per GPU), the setup of the failed run 22726386:**

| Policy | CUDA OOM (in the first 10 min) | Pipelines lost | Passing folds | GPU-h per fold | rfd3 per node-h |
|---|---|---|---|---|---|
| none | 183 (183) | 183 | 976 | 0.131 | 112 |
| stagger (today) | 73 (20) | 73 | 2216 | 0.058 | 255 |
| count16 | 96 (94) | 96 | 1566 | 0.082 | 181 |
| count24 | 158 (158) | 158 | 1219 | 0.112 | 143 |
| aggregate | 156 (155) | 156 | 1261 | 0.110 | 145 |
| node-nomem | job killed at 0.5 min | – | 0 | – | – |
| **node** | 0 | 0 | 2833 | 0.045 | 324 |
| **node-learned** | 0 | 0 | 2798 | 0.046 | 324 |

**One node, past the operating point (10 and 12 pipelines per GPU):**

| Pipelines per GPU | Policy | Failures | Passing folds | GPU-h per fold | admission wait p95 s |
|---|---|---|---|---|---|
| 10 | stagger | 1 CUDA OOM | 388 | 0.041 | – |
| 10 | node | 0 | 404 | 0.040 | 5 |
| 12 | stagger | job killed in 3 of 5 runs, at about 22 min | 181 | 0.051 | – |
| 12 | node | 0 | 431 | 0.037 | 37 |
| 12 | node-learned | 0 | 399 | 0.040 | 75 |

**Through the real engine** (`engine_smoke.md`; 1 node, 32 pipelines, 1 simulated
hour; GPU sharing not modelled here, so the rates are not comparable to the tables
above):
- **No admission:** the job is killed at 0.5 min and all 32 pipelines are lost.
- **Declared memory:** 0 failures, 370 rfd3.
- **Learned memory:** 0 failures, 360 rfd3.
- **Placement:** every task was placed by the pool and the backend received its
  placement.
- **Cost:** 8–24 µs per admission. It is wall-clock time on an unpinned desktop
  machine, so it depends on machine load: 22–24 µs in the saved run, 8–9 and
  12–14 µs in two later runs.
- **Run-to-run variation:** counts move by a few percent between runs, because
  simulated time is compressed into real time.

## Findings

1. **Memory-aware admission replaces the hand-tuned stagger.**
   - On one node, `node` and `node-learned` run 32 pipelines from time zero, with no
     failures, at the stagger's throughput or slightly above (rfd3 +3%).
   - The stagger is a guess tied to one workload ((N−1)//slots × 60 s). Admission
     derives the same safety from the ledger.

2. **Host memory is the binding dimension, as the workload model said (W1).**
   - GPU fractions alone (`node-nomem`) admit 32 rfd3 at once, and the job is
     OOM-killed at 30 s, as in job 22714785.
   - Task-count caps are safe only when set low. On one node `count16` loses 44% of
     the folds and `count24` 21%.

3. **On Dragon, admitting against pool totals is not enough; asyncflow must place.**
   - On 8 nodes, `aggregate` and `countN` lose 96–158 pipelines on average to CUDA
     OOM. Today's stagger loses 73.
   - The admission is correct in total. The failures come from where Dragon puts
     the tasks and which GPU the application pins.
   - `node` mode places host and GPU itself and has no failures. Per-node
     throughput (324 rfd3 per node-hour) is the same as on one node, so it scales.
   - This corrects the design. §4.3 said aggregate admission "delays but does not
     break anything", and that is wrong for co-located GPU work on a backend that
     does not account for GPUs and memory.

4. **A likely mechanism for the 8-node CUDA OOMs (hypothesis).**
   - Dragon Batch spreads one compile batch round-robin across nodes, starting at a
     random node (`hefty_partition`: least-loaded partition, ties by index). Load
     from earlier batches is not counted.
   - IMPRESS pins pipeline N to GPU (N−1) mod 4.
   - With 8 nodes, the tasks of one batch that land on a node are 8 pipelines
     apart, so they share the same N mod 4: all of a node's tasks from that batch
     pin the same GPU.
   - In the model this produces 183 CUDA OOMs when all 256 pipelines start at once,
     and 66–80 over 4 h with the stagger. Run 22726386 recorded 59 during its ramp
     before the inode quota ended it.
   - The timing differs: the model spreads the stagger's OOMs over the run, while
     the real ones clustered in the ramp. So this is a candidate explanation, not a
     reproduction.
   - It can be checked from 22726386's `TASK HOST` lines and each task's
     `gpu_affinity`.
   - If it holds, IMPRESS can avoid it today without asyncflow changes: pin GPU by
     `N // nodes`, not `N mod 4`, or pass the host as well.

5. **Learned memory is nearly free after the first minutes.**
   - With no memory declared, `node-learned` is within 1–3% of `node` over 4 h. The
     learning rule is: until 3 samples per task kind, at most 4 unknown-memory
     tasks per node; then the estimate is the observed maximum × 1.1.
   - It loses about 13% of rfd3 output in the first 10 minutes and about 4% over 30
     minutes, and still beats the stagger over the same windows.
   - At 12 pipelines per GPU it is 7% below `node`. Max × 1.1 of observed rfd3
     peaks is about 11 GB, more conservative than the declared 9.5 GB.
   - Learning needs the backend to report each task's peak host and GPU memory.
     Neither rhapsody nor Dragon does that today.

6. **Admission lets IMPRESS run past its saturation point safely.**
   - At 12 pipelines per GPU, `node` gives 23% more folds per node than 8 per GPU,
     at 0.037 GPU-h per fold. Admission absorbs the overload as queueing (p95 wait
     37 s).
   - The stagger at 12 per GPU is OOM-killed in 3 of 5 runs.
   - This extrapolates beyond the calibrated range. The run notes expect at most
     10–15% more beyond 8 per GPU, and the model gives +14% at 10 per GPU.

7. **CPU threads must not be treated as reservations.**
   - IMPRESS caps rfd3 and boltz at 8 threads and mpnn at 4. Counted as cores, 32
     pipelines would admit only about 8 GPU tools per node, far below the operating
     point.
   - Measured CPU use at 8 per GPU was 68%. The prototype counts threads with 4×
     overcommit, and the CPU dimension never bound.
   - The design should treat CPU as a soft, overcommittable dimension, or learn CPU
     use as it learns memory.

8. **Decision cost is around the W6 target (20 µs) and depends on machine load.**
   - In the simulator (`node` and `node-learned`): 4–15 µs per admission in the
     saved run, 3–9 µs in a later re-run.
   - Through the engine: 8–24 µs, including the cancellation check.
   - All of these are wall-clock times on an unpinned desktop machine, and they
     move with its load. Judging W6 needs a measurement on a pinned, idle core.
   - The `MemoryModel` first kept every observation and took a maximum on every
     call, costing 159 µs at 8 nodes. It now keeps a running count and maximum.

## Limits

- **GPU memory per tool** (rfd3 5, mpnn 1.5, boltz 4 GB) is fitted to per-GPU
  peaks, not measured per task. The 8-node CUDA-OOM counts depend on it.
- **The co-location slowdown** is one fitted coefficient, with no CPU model behind
  it.
- **Dragon's placement model** comes from reading `hefty_partition`. It has not
  been observed on a real run.
- **Not modelled:**
  - Dragon's completion stalls.
  - The inode quota.
  - The 8-node run's teardown.
  - Placement through `Policy(HOST_NAME)` bypassing Batch's allocation accounting
    (design §17), which could change how Batch phases behave on a real allocation.
- **Throughput per GPU-hour** is the objective. No exemplar states a latency
  target.

## Changes folded into policy-layer.md

- §4.1: CPU is overcommittable; thread caps are not reservations.
- §4.3: aggregate admission is unsafe for co-located GPU work on Dragon today; node
  mode is the default there until Dragon accounts for GPUs and memory or reports
  placement.
- §15: S1 marked prototyped, with a pointer here.
- §17: the double-scheduling risk is confirmed.

## Next steps

1. **Ask IMPRESS for data that can confirm or refute finding 4:**
   - 22726386's `TASK HOST` lines and GPU indices.
   - Per-task peak host and GPU memory (NVML per process).
2. **A rhapsody adapter:**
   - Translate `placement` into a Dragon process `Policy` (host name,
     `gpu_affinity`).
   - Report each task's peak RSS and GPU memory as `observed`.
3. **Run `node` mode on Delta:** 1 node at 8 and 12 pipelines per GPU, then
   8 nodes, without the stagger.

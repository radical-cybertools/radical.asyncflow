<!--
Title: Scheduler: opt-in ready-queue policies (stacked on #110)
Base:  feature/tickless-on-hot-path (#110)
Head:  feature/ordering-policies (local only, not pushed)
Commits:
  b002de3 Policies: opt-in ready-queue policies (FIFO, lineage, fair share)
  3f4fe00 Merge feature/tickless-on-hot-path into feature/ordering-policies
          (brings main #106-#108 up the stack; local, not pushed)
  c8f6990 Merge feature/tickless-on-hot-path into feature/ordering-policies
          (brings #110's review fixes, 0157751; local, not pushed)
-->

## Summary

Stacked on #110. `WorkflowEngine.create` takes an optional `policy=`: the
engine hands each ready task to the policy and submits the tasks the policy
dispatches, so the policy decides how many tasks are in flight and which ready
task starts next. Without a policy nothing changes.

```diff
 run loop, a component is ready
-  to_submit.append(desc)
+  plain task and a policy is set:  policy.enqueue(uid, desc)
+  otherwise (no policy, block, service):  to_submit.append(desc)
+ to_submit += policy.dispatch()        # what fits in the free capacity
  submit(to_submit)

 task_callbacks(task, DONE | FAILED | CANCELED)
+  policy.release(uid, observed=task.get("observed"))   # frees a slot; _kick() if more can start

 _on_done(uid)
   release dependents
+  policy.release(uid)                   # no-op if already released
```

```text
radical.asyncflow.policies
  Policy          protocol: enqueue, dispatch, release(uid, ran=True, observed=None), runnable
  FifoPolicy      ready order, at most `capacity` in flight
  LineagePolicy   oldest lineage first: started workflows finish before new ones start
  FairPolicy      weighted fair share across workflow ids (CFS-style virtual runtime,
                  per-function runtime estimates corrected at completion)
```

```python
from radical.asyncflow import FairPolicy, WorkflowEngine

flow = await WorkflowEngine.create(
    backend=backend, policy=FairPolicy(64, weights={"interactive": 4})
)
```

- Blocks run inside the engine and services are long-lived, so both bypass the
  policy.
- The engine computes registration order (`_seq`) and lineage (`_lineage`) only
  when a policy is set.
- A task cancelled while queued cancels its dependents at once, as without a
  policy. When the policy later dispatches it, the engine releases it with
  `ran=False`, and `FairPolicy` refunds its charge.
- When the backend reports a task done, failed or cancelled, the policy gets
  the resource use the backend observed, from the `observed` key of the task
  description (for example peak memory). No released RHAPSODY backend reports
  it yet, so it is None today; a local RHAPSODY branch reports Dragon tasks'
  peaks. Resource-aware policies use it to learn memory demand.
- Any object with the four operations works as a policy. One that lacks them
  is rejected when the engine is created.
- The engine calls the policy on its event-loop thread, so a policy needs no
  locking.
- Resource-aware admission (stage S1 of the policy-layer design: resource
  vectors, pools, GPUs and memory) will be further policies behind this hook,
  not a second hook.

## Evidence

**Tests.** `tests/unit/test_policies.py`, 35 tests.

- The policies on their own: capacity validation, FIFO order and release,
  lineage order, fair share (alternation, 2:1 weights, weight keys by prefix,
  no credit banked while idle, refund of a task that never ran).
- The engine with a policy: capacity bounds the tasks in flight, `LineagePolicy`
  against `FifoPolicy` order, fair share across workflow scopes, cancellation
  while queued, blocks and services bypassing the policy, a custom (LIFO)
  policy, observed usage reaching the policy on DONE, FAILED and CANCELED, an
  object without the operations rejected, shutdown with tasks still queued.

```text
test_capacity_bounds_tasks_in_flight
  FifoPolicy(3); 20 independent tasks; the backend holds each task until the test finishes it
  expect: 3 submitted while the first 3 run; never more than 3 in flight; all 20 done

test_lineage_finishes_started_workflows_first
  capacity 1; a1 running; then b1 (independent) and a2 (depends on a1) registered
  expect FifoPolicy:    a1, b1, a2
  expect LineagePolicy: a1, a2, b1
```

The capacity, lineage, cancellation, bypass and observed-usage tests each fail
with the code they cover removed. Full unit suite with rhapsody-py 0.6.0 and
its telemetry extra: 202 passed (167 + 35) on macOS with Python 3.9 to 3.14,
and with `PYTHONASYNCIODEBUG=1` on Python 3.13, where asyncio reported no
non-thread-safe call. ruff and pre-commit are clean.

**Measurements.** AMD EPYC 7502P, Python 3.14, one pinned core.

With no policy, against #110 (medians of 7; K is the number of tasks parked in
flight):

| | #110 | This PR, no policy |
|---|---|---|
| Sequential chain, per task | 32.05 µs | 31.85 µs |
| 1,024 concurrent diamonds, K=0 | 62,700 tasks/s | 62,270 tasks/s |
| 1,024 concurrent diamonds, K=10k | 48,580 tasks/s | 48,760 tasks/s |

With each policy, on a simulated backend with 16 workers and a FIFO queue, as
ThreadPoolExecutor has, capacity 16, medians of 5. (Dragon's `Batch` turned out
not to be strictly FIFO; see the rust results below.)

512 concurrent diamond workflows, 2 ms tasks:

| | none | FIFO | Lineage | Fair |
|---|---|---|---|---|
| Makespan | 0.246 s | 0.250 s | 0.251 s | 0.238 s¹ |
| Mean workflow completion | 0.210 s | 0.213 s | 0.138 s | 0.203 s |
| First workflow done | 0.173 s | 0.176 s | 0.026 s | 0.167 s |
| Workflows in progress at peak | 512 | 512 | 16 | 512 |
| Backend queue at peak | 512 | 16 | 16 | 16 |

4,000 bulk 5 ms tasks beside an interactive chain of three 5 ms tasks every
50 ms (20 chains), bulk and chains in separate workflow scopes:

| | none | FIFO | Lineage | Fair |
|---|---|---|---|---|
| Chain latency p50 | 905 ms | 915 ms | 924 ms | 20 ms |
| Chain latency p95 | 1,357 ms | 1,366 ms | 1,379 ms | 22 ms |
| Bulk makespan | 1.394 s | 1.401 s | 1.417 s | 1.445 s |

¹ Fair's makespan varied more than the others' (0.236–0.257 s); its median below
"none" is not a speed-up.

- With no policy, the hook costs nothing measurable.
- The policies cost 1–4% of makespan with 2–5 ms tasks.
- Lineage finishes workflows a third sooner on average and keeps 16 in
  progress instead of 512.
- Fair keeps interactive chains at about 20 ms beside 4,000 bulk tasks,
  instead of about 0.9 s.

**On RHAPSODY and Dragon (rust cluster).** julia (2× AMD EPYC 9745): a job of
64 CPUs, 32 Dragon `Batch` workers, capacity 32 (the pool size), Fair without
weights; median of 3 runs. RHAPSODY 0.6.0 with `Batch`'s pool capped to the
job's CPUs. Two Dragons:

- **stock:** the dragonhpc 0.14.1 wheel. `Batch` reports a launch round only
  when the round's slowest task ends, and groups submissions that arrive within
  5 ms.
- **per-task reports:** a local Dragon branch that reports each task when it
  ends, without the 5 ms grouping. Not released; proposed to the Dragon team.

E1a: 1,000 bulk tasks of 1–5 s; beside them, an interactive chain of three
0.5 s steps every 6 s (12 chains), in a separate workflow scope.

| Dragon | | None | FIFO | Lineage | Fair |
|---|---|---|---|---|---|
| stock | Chain latency p50 | 26.9 s | 102.5 s | 100.2 s | **7.1 s**¹ |
| stock | Bulk makespan | 161.2 s | 139.8 s | 137.3 s | 141.5 s |
| per-task reports | Chain latency p50 | 1.86 s | | | **1.59 s** |
| per-task reports | Bulk makespan | 98.4 s | | | 99.1 s |

E1b: 256 diamond workflows t1 → (t2, t3) → t4 of 1 s tasks, registered at once.

| Dragon | | None | FIFO | Lineage | Fair |
|---|---|---|---|---|---|
| stock | Mean workflow completion | 19.2 s | 30.3 s | **18.7 s** | 30.1 s |
| stock | First workflow done | 5.3 s | 26.3 s | **3.2 s** | 26.3 s |
| stock | Workflows in progress at peak | 64 | 256 | **32** | 256 |
| per-task reports | Mean workflow completion | 26.4 s | | **18.4 s** | |
| per-task reports | First workflow done | 21.2 s | | **3.1 s** | |
| per-task reports | Workflows in progress at peak | 230 | | **32** | |

E3: a chain of 100 trivial tasks, then 2,000 independent trivial tasks.

| Dragon | | None | FIFO | Lineage | Fair |
|---|---|---|---|---|---|
| stock | Per dependent task | 57.1 ms | 57.2 ms | 57.2 ms | 57.1 ms |
| stock | Trivial tasks per second | 949 | 429 | 398 | 393 |
| per-task reports | Trivial tasks per second | 836 | | 837 | 853 |

¹ 2.3–9.9 s across runs; a rerun of all three gave 2.4 s.

- **Fair keeps interactive chains short:** 7.1 s against 26.9 s with no policy
  on stock Dragon, and 1.59 s against 1.86 s with per-task reports, where the
  chain's own work is 1.5 s. The bulk is at most 1% slower than with no policy.
- **Lineage finishes workflows sooner:** with per-task reports, 18.4 s on
  average against 26.4 s, the first after 3.1 s instead of 21.2 s, with 32 in
  progress instead of 230. On stock Dragon no policy already behaves almost like
  Lineage, by an accident of the round-at-a-time reports.
- **FIFO and Lineage make interactive chains slower than no policy.** A late
  chain step overtakes Dragon's backlog, but not a policy's queue. Use Fair when
  some work is interactive.
- **No latency cost per task:** 57.1–57.2 ms per dependent task with or without
  a policy. Most of it is Dragon's client waiting up to 50 ms before it passes
  on a completion. A local Dragon change that passes it on at once brings the
  step to 6.2–6.6 ms, with no policy, Fair or Lineage alike.
- **Set the capacity to the backend's pool size.**
  - On stock Dragon, a capacity of 32 halves the throughput of trivial tasks
    (393–429 against 949 per second): a slot comes back only when the slowest
    task of its launch round ends. With per-task reports, capacity 32 matches
    no policy. For tasks of 1–5 s (E1a), a policy is no slower than none on
    either Dragon.
  - A capacity above the pool size does not raise throughput, and Fair loses
    its latency: chains of 10–59 s at capacities from 64 to unlimited (measured
    with per-task reports, the policies also setting Dragon priorities).
- **Two nodes** (16 workers on each, per-task reports): Lineage keeps most of
  its gain (19.7 s against 26.3 s), and capacity 32 still matches no policy's
  throughput (333–354 against 311 per second). Fair's chains no longer beat no
  policy (2.17 s against 2.07 s): each refill crosses the slower two-node
  runtime.

**On 2 and 4 nodes (PSC Bridges-2).** RM nodes of 128 cores, 64 `Batch` workers
per node (128 and 256 in all), capacity equal to the pool, the same local Dragon
branch (per-task reports and the completion wake-up), Dragon over TCP on
InfiniBand. The workloads grow with the pool: 1,000 bulk tasks and 256 diamonds
per 32 workers. This PR's code, through the resource-policy branch at `22d4882`.
Median of 3 runs.

| | 2 nodes, none | 2 nodes, policy | 4 nodes, none | 4 nodes, policy |
|---|---|---|---|---|
| E1a chain latency p50 (Fair) | 45.5 s | **1.94 s** | 56.4 s | **2.09 s** |
| E1a bulk makespan | 98.5 s | 102.2 s | 100.0 s | 103.4 s |
| E1b first workflow done (Lineage) | 7.3 s | **3.2 s** | 25.2 s | **4.9 s** |
| E1b mean workflow completion | 19.3 s | 19.3 s | 35.8 s | **26.8 s** |
| E1b workflows in progress at peak | 280 | 128 | 1,592 | 256 |
| E1b makespan | 35.3 s | 36.6 s | 43.8 s | 50.2 s |
| E3 per dependent task p50 (Fair / Lineage) | 20.6 ms | 21.3 / 20.2 ms | 25.5 ms | 26.5 / 26.1 ms |
| E3 per dependent task p95 | 59 ms | 48 / 48 ms | 177 ms | 42 / 48 ms |
| E3 trivial tasks per second | 163 | 185 / 186 | 162 | 160 / 156 |

- **Fair keeps interactive chains at about 2 s on 2 and 4 nodes,** and the bulk
  takes 3–4% longer. Without a policy the chains wait 45–56 s here. On one
  Bridges-2 node they take 1.80 s with 32 workers, as on rust, and 20.6 s with
  64, against 1.62 and 1.57 s with Fair (the bulk grows with the workers). So no
  policy gets worse with scale on a single node too, and the site is not the
  cause. With 128 workers on one node they take 44.1 s, against 45.5 s over 2
  nodes, and 1.55 s with Fair: the worker count makes the wait, and spanning
  nodes adds little.
- **Lineage finishes the first workflow 2–5 times sooner** and keeps one pool's
  worth of workflows in progress. On 4 nodes it also finishes workflows 25% sooner
  on average; on 2 nodes the average is the same. The makespan is 4% longer on 2
  nodes and 15% longer on 4.
- **No latency cost at the median.** On 4 nodes the policies cut the p95 dependent
  step from 177 to 42–48 ms.
- **E3 on 4 nodes has 2 runs:** the first hung inside Dragon. The local Dragon
  branch hung twice on 4 nodes; stock Dragon never did. Throughput is Dragon's:
  raw `Batch` does 668 tasks/s on one node alone, and 237–295 on one node of a 2-
  or 4-node runtime.

## Merge Danger

**Door:** two-way until released

Opt-in: nothing changes without a policy, and reverting the commit restores
#110. Once released, `policy=`, the `Policy` protocol and the three classes are
public API.

**Blast Radius:** opt-in API

- **Without a policy:** a few `is not None` checks per task.
- **With a policy:** the engine holds ready tasks back; a policy bug can starve
  tasks. Blocks and services are never held.
- **On stock Dragon 0.14.1:** a policy at the pool size halves the throughput
  of millisecond tasks until Dragon reports each task when it ends (see the
  rust results). Tasks of seconds are not affected.
- **Merge order:** after #91, #100 and #110.

Generated with Matteo's hands and Claude Code

# Prompt reports, priorities and capacity on real Dragon: second rust campaign

Status: experiments, 2026-10-25. The jobs ran from 01:02 CEST to 02:17 CET, i.e. 03:17 CEST:
daylight saving time ended at 03:00 during the campaign, which Slurm's times show. The analysis
uses only relative times. Everything is local: three new local branches, nothing pushed, nothing
posted to GitHub. The deck's `.tex` is unchanged.

The first campaign is in [rust-experiments.md](rust-experiments.md). This one asks:
- can Dragon report tasks promptly and honour AsyncFlow's order;
- does a policy capacity above the pool size recover E3's throughput without losing E1's latency?

- Scripts: `design/presentations/scheduling-policies/experiments/`. The new ones are
  `submit_campaign2.sh`, `analyze2.py`, `diag_priority.py`, `build_dragon.sh` and
  `dragon_tests.sbatch`. The first campaign's scripts gained options that are off by default,
  so it reproduces unchanged.
- Raw results, one JSON file per run and configuration:
  `design/presentations/scheduling-policies/results/r2/raw/`.
- Tables (generated): `results/r2/summary.md`, `results/r2/summary.json`.
- Charts, PDF and PNG: `results/r2/*.pdf`, `results/r2/*.png`.
- Diagnostics and Dragon test logs: `results/r2/diag/`.
- The three branches' diffs: `results/r2/diffs/`.

## Question and answer

**Question.** Can Dragon report tasks promptly and honour AsyncFlow's priorities? Does a policy
capacity above the pool size recover E3's throughput without losing E1's latency?

**Answer.**

- **Prompt reports: yes, with one Dragon change, per-task reports (b).** Batch launches each
  round with one `map_async`, and `MapResult` calls back only when the whole round is done.
  - With `apply_async` per task, every task is reported within 0.05 s of its end.
  - E1a, 1,000 bulk tasks beside interactive chains on 32 workers:
    - chain p50 with no policy: 26.9 s → 1.9 s; Fair at 32: 2.4 s → 1.7 s;
    - bulk makespan 142–161 s → 98–99 s, for 94–95 s of work; workers 59–67% → 96% busy.
  - A group size of 1 alone (a, or the no-change patch) does not fix reporting in a tight loop.
    It still helps on stock Dragon when a policy refills one task at a time: Fair at 32 gets
    1.65 s and 107 s with no Dragon change.
- **Priorities: Dragon honours them (c), but they only matter above the pool size, and there
  Fair's go stale.**
  - The diagnostic shows priority ordering both ways.
  - At capacity 32 nothing changes.
  - At 64, 128 and ∞, FIFO keeps the ready order and Lineage keeps its depth-first gain (mean
    diamond completion 18.4–18.8 s against 26.4 s without a policy).
  - Fair's chain latency grows to 10.5, 28.2 and 59.2 s, ending at FIFO's 59.2 s.
    Fair's priority is its virtual runtime at dispatch, and a new chain then ranks behind
    every bulk task already inside Dragon.
- **Capacity above the pool size: no.** With per-task reports and one-task compiles, capacity 32
  already reaches no policy's throughput: Fair 853/s against 836/s for 2,000 trivial tasks.
  - So the first campaign's halving was Dragon's reporting and client linger, not the
    capacity. That report is corrected.
  - Above 32 throughput does not rise beyond the spread, and Fair loses its latency.
  - On stock Dragon with the patch, capacity 128 recovers throughput (901/s) but Fair's chain
    p50 becomes 37 s.
- **Submission rate with group size 1:** 5–9% fewer trivial tasks per second without a policy
  (966 → 920/s on stock; 922 → 836/s on the branch). Nothing measurable for tasks of seconds.
  In exchange, 5 ms less per dependent task (57.1 → 52.0 ms).
- **The launch budget (d) is already the pool size, so it changes nothing.** The "256" in the
  brief is also the pool size without RHAPSODY's cap.
- **One more explanation of the first campaign:** Batch breaks ties by the tuid **string**,
  so `"1-1031"` runs before `"1-532"`.
- **Everything ran as planned,** plus the (a)/(b) split. 150 configuration results, none failed;
  no job was preempted. Dragon's own tests pass on the branch, with the pool capped.

## What Dragon's source says, and what the diagnostics showed

All four points were checked in v0.14.1's source (`dragon/workflows/batch/batch.py`,
`dragon/native/pool.py`), then on julia with raw Batch (no RHAPSODY), 8 workers.

**1. Client grouping.** `_drain_client_task_batch(..., linger_sec=0.005, max_batch_size=256)` is
called without those arguments, so only its `__defaults__` can change them. Confirmed.

**2. Burst reporting: the cause is `map_async`, as suspected.**
- `Manager._launch_tasks` launches everything prepared in a round with one
  `pool.map_async(_do_task, ..., chunk_size=1, callback=...)`.
- `MapResult._set` calls the callback only when `_number_left == 0`, i.e. once every task of
  that call is done. So `chunk_size=1` does not report each task as soon as it ends, though
  the comment says it does.
- `diag_batch.py`, 6 sleeps of 1–6 s submitted in a tight loop:

  | Dragon | Each task reported when it ends? | All six reported at |
  |---|---|---|
  | Stock 0.14.1 (first campaign, job 24476) | no | +6.02 s |
  | Stock + no-change patch (`__defaults__ = (0.0, 1)`), job 24674 | no | +6.06 s |
  | Branch (a): `client_max_batch_size=1`, no linger, job 24675 | no | +6.06 s |
  | Branch (b): `report_each_task=True`, job 24676 | **yes**, within 0.05 s | – |
  | Branch (a)+(b), job 24677 | **yes**, within 0.05 s | – |

  - A group size of 1 is not enough on its own. Single-task compiles that reach the manager in
    the same loop iteration are still launched by one `map_async` and reported together.
  - Per-task `apply_async` (b) fixes it even with the stock grouping.
  - Submitted 50 ms apart, every variant reports each task on time.

**3. Priority exists but cannot be reached; with (c) it can.**
- Subnode managers keep ready tasks in a heap keyed by `(-weight, heft_topo_order, tuid)`.
  The weight is the HEFT critical path inside one compile group, which is 1 for everything
  AsyncFlow submits, since it submits only ready tasks.
- So the order is the position in the compile group, then the tuid **as a string**
  (`f"{client_id}-{seq}"`). Two consequences explain the first campaign's oddities:
  - A task submitted alone (position 0) overtakes a backlog submitted as one group. This is the
    "no policy" chains overtaking the bulk in E1.
  - Among equals, `"1-1031"` sorts before `"1-532"`. So in the spaced runs, tasks with
    sequence numbers of 1,000 and above started before older ones (bulk task 999 at 11 s, task
    500 at 49 s).
- With (c), `Batch.options(priority=n)` sets `TaskCore.priority`, and the key becomes
  `(-priority, -weight, heft_topo_order, tuid)`. `diag_priority.py`, 8 workers, sleeps of 2 s:

  | Dragon | Case A: 16 low + 1 high, one burst; the high one starts | Case B: 16 high, then 1 low 0.5 s later; the low one starts |
  |---|---|---|
  | Stock (no priority), job 24678 | 17th of 17 (last) | 16th of 17 (overtakes) |
  | Branch (c), job 24679 | 2nd (first wave) | 17th (last) |
  | Branch (c)+(d), job 24680 | 1st (first wave) | 17th (last) |
  | Branch (a)+(b)+(c)+(d), job 24681 | 9th, at the first free slot | 17th (last) |

  - A priority orders waiting tasks; it does not preempt running ones.
  - With (a), the 16 low tasks reach the manager one by one, and 8 were running before the high
    one arrived, so it took the first slot that freed.
- (c) works as described.

**4. The launch budget already equals the pool, so (d) changes nothing.**
- A subnode manager is built with `physical_cores_per_node=self._workers_per_node_in_pool`.
  Its pool has `len(pool_node_huids) * self._workers_per_node_in_pool` workers, with one node
  per pool. Its budget starts at `physical_cores_per_node`. Budget and pool are the same number:
  - 32 on a 64-CPU job with RHAPSODY's pool cap;
  - 256 without it, when the pool also has 256 workers.
- A function task reserves 1, so at most pool-size tasks reach the pool's FIFO queue, and the
  heap order holds.
- (d) caps the budget at the pool's workers, which is a no-op. The diagnostic behaved the same
  with and without it (jobs 24679, 24680). It is kept in the matrix, as asked.
- What does hold back the queue is point 2. The budget is released when a task's completion is
  handled, so with `map_async` a whole round's budget comes back only when its slowest task
  ends. That is why the waves left a third of the workers idle in the first campaign.

## Software

| Repository | Branch (local, from) | Commit | Diff |
|---|---|---|---|
| Dragon | `experiment/batch-reports-priority` (from `v0.14.1`, `0701b62`) | `dbf5405` | `diffs/dragon-dbf5405-vs-v0.14.1.diff` (batch.py +91/−10, tests +31) |
| RHAPSODY | `experiment/dragon-priority` (from `d2cbc58`) | `738288e` | `diffs/rhapsody-738288e-vs-d2cbc58.diff` |
| AsyncFlow | `experiment/policy-priority` (from `b002de3`) | `641ee65` | `diffs/asyncflow-641ee65-vs-b002de3.diff` |

- **Dragon** (worktree `~/github/dragonhpc/dragon-batch-reports-priority`; the shared clone is
  untouched). Every switch is off by default:
  - **(a)** `Batch(client_linger_sec=, client_max_batch_size=)`, passed to
    `_drain_client_task_batch`.
  - **(b)** `Batch(report_each_task=True)`, or `DRAGON_BATCH_REPORT_EACH_TASK=1`:
    `_launch_tasks` calls `apply_async(_do_task, (args,), callback=...)` once per task, and the
    callback queues `[completion]`.
  - **(c)** `Batch.options(priority=n)`. `BatchTaskOptions.priority` is set on `TaskCore.priority`
    before the task is enqueued, so it survives compile. The heap key is
    `(-priority, -weight, heft_topo_order, tuid)`. `Task.priority` is a property, which counts
    only before compile. The HEFT weight is untouched.
  - **(d)** `Batch(cap_launch_budget=True)`, or `DRAGON_BATCH_CAP_LAUNCH_BUDGET=1`: the budget
    is `min(physical_cores_per_node, num_workers)`.
  - New `TestClientPriorityLogic` (4 pure tests). Black (line length 120) passes.
- **RHAPSODY** (worktree `~/github/radical/rhapsody-dragon-priority`):
  - `task["priority"]` goes to `Batch.options(priority=...)` when the installed Batch accepts
    it, and is ignored otherwise.
  - `compile_each_task=True` applies the no-change patch (`__defaults__ = (0.0, 1)`) while the
    backend lives, and restores the defaults at shutdown.
  - The branch's Batch options travel through `batch_kwargs`.
  - 5 new Dragon-free tests; with the affinity tests, 13 pass. ruff and pre-commit pass.
- **AsyncFlow** (worktree in this session's scratchpad, `trees/af-policy-priority`).
  `FifoPolicy`, `LineagePolicy` and `FairPolicy` take `set_priority=True` (off by default)
  and then write `desc["priority"]` in `dispatch()` from their own state:
  - FIFO: dispatch order, which is the ready order (`-n` for the n-th dispatch);
  - Lineage: the lineage's age (`-_lineage`);
  - Fair: the group's virtual runtime at dispatch, in integer µs (`-round(vruntime * 1e6)`).

  5 new tests; the full unit suite passes (187). ruff and pre-commit pass.
- **Build.**
  - `build_dragon.sh` (job 24673, haskell, 218 s) follows the recipe in
    `dragon-rust-findings.md`: EESSI `GCCcore/13.2.0`, the compat-layer Python 3.11.4, and
    `devtools/clean_build base`.
  - It builds into `/home/mturilli/tmp/sched-exp/dragon-exp` with its own `_env`. The stock
    venv and the other session's `dragon-src` were not touched.
- **Dragon's own tests on the branch** (`dragon_tests.sbatch`, haskell, 16 CPUs). They pass:
  - default switches: 28 tests in 7 classes, including the new ones (job 24773);
  - `DRAGON_BATCH_REPORT_EACH_TASK=1`: 21 tests in 5 classes, among them the completion queue,
    argument dependencies, inter-compiled dependencies and fences (job 24774);
  - the stock wheel, the same way: 10 tests (job 24772).
  - Caveat: the tests build `Batch()` themselves, so on a 16-CPU slice they got 256 workers
    and failed on timing. Job 24769, without the cap, failed 3 completion-queue tests and
    timed out. The runs above cap the pool in a copy of the test file, as the RHAPSODY fix
    does.

## Setup of the campaign

- **Where.** julia, partition `cpu`, 64 CPUs, so 32 Batch workers (RHAPSODY caps the pool), as
  in the first campaign. One job at a time (`-J dj -d singleton`). Each configuration ran 3
  times, interleaved in a different shuffled order per repetition, and repetition 1 had twice
  the time limit.
- **Trees.** asyncflow `641ee65` and RHAPSODY `738288e` for every run. With `set_priority`,
  `compile_each_task` and the Batch options off, they behave as the first campaign's `b002de3`
  and `d2cbc58`. Dragon was either the stock 0.14.1 wheel or the branch.
- **Workloads.** Unchanged from the first campaign:
  - E1a: 1,000 bulk tasks of 1–5 s, beside 12 chains of 3 × 0.5 s every 6 s;
  - E1b: 256 diamonds of 1 s tasks;
  - E3: a chain of 100 trivial tasks, and 2,000 independent ones.
- **Configurations.** `<policy>[@capacity][+flags]` (`common.parse_spec`). Capacity 32 is the
  pool size, and ∞ is 10⁹. The flags:
  - `each`: the no-change patch, on the stock wheel;
  - `a`, `b`, `d`: the branch's switches;
  - `prio`: the policy writes priorities, i.e. (c).
- **The matrix:**
  - stock: no policy, and the best policy (Fair for E1a, Lineage for E1b; both in E3) at 32;
  - stock + no-change patch: no policy, and the policies at 32 and 128;
  - branch (a)+(b): no policy, and the policies at 32;
  - branch (a)+(b)+(d) with priorities: the policies at 32, 64, 128 and ∞, plus FIFO at 128
    in E1a and E3, to see whether priorities keep the ready order;
  - added after E1a's first repetition, to separate (a) from (b): branch (b) alone (no policy;
    Fair at 32; and in E3, Lineage at 32 and Fair at 128) and branch (a) alone (E3, no
    policy).
- **Cleanup.** `run.sbatch` now deletes only the `/dev/shm` files of this job (created since it
  started) and no longer calls `dragon-cleanup`. Nothing was left on julia after any job.
  - Cancelling Dragon test job 24770 on haskell skipped its epilogue and left 20 pool files
    (01:56–02:02 CEST).
  - I deleted exactly those, by time window, once no job of ours ran there. No processes were
    left.

## Results

Median [min–max] of 3 runs per row; every table is in `results/r2/summary.md`, with the job ids.
All 150 result files of the campaign and the split are complete: 75 E3 configurations (in 9
jobs), 42 E1a runs and 33 E1b runs, with no failure. The bulk of E1a needs 94–95 s of work on
32 workers, and a chain 1.5 s.

### What each change buys on its own

| Dragon | E1a chain p50, no policy | E1a chain p50, Fair 32 | E1a bulk makespan, Fair 32 | E3 throughput, Fair 32 | E3 throughput, no policy | Per dependent task (E3 chain) |
|---|---|---|---|---|---|---|
| Stock | 26.9 s [26.8–27.6] | 2.4 s [2.3–7.6] | 142 s (67% busy) | 403/s | 966/s | 57.1 ms |
| Stock + no-change patch | 13.1 s [12.7–14.7] | 1.65 s [1.64–1.69] | 107 s (89%) | 425/s | 920/s | 52.0 ms |
| Branch (a) one-task compiles | – | – | – | – | 902/s | 52.1 ms |
| Branch (b) per-task reports | 1.93 s [1.93–1.93] | 1.68 s [1.67–1.69] | 99 s (96%) | 533/s [518–567] | 922/s | 57.2 ms |
| Branch (a)+(b) | 1.86 s [1.86–1.91] | 1.59 s [1.59–1.63] | 99 s (96%) | 853/s [790–873] | 836/s | 52.1 ms |
| Branch (a)+(b)+(d), priorities | – | 1.60 s [1.59–1.67] | 99 s (96%) | 828/s [826–887] | – | 52.1 ms |

Charts: `r2_e1a_chain_latency.pdf`, `r2_e1a_bulk_makespan.pdf`, `r2_e3_throughput.pdf`.

- **(b) Per-task reports: the change that matters.**
  - E1a without a policy: chain p50 from 26.9 s to 1.9 s. The bulk takes 98 s instead of
    161 s, and the workers are 96% busy instead of 59%.
  - Fair at 32: 2.4 s to 1.7 s (and stock's spread of 2.3–7.6 s disappears), and 142 s to 99 s
    of bulk makespan.
  - E3 at capacity 32: 385–403 to 533–558 tasks/s.
  - It is the whole difference between the first campaign's "stock" and "spaced" rows, now
    without throttling submission.
- **(a) One-task compiles, and the no-change patch.**
  - Alone, they do not fix reporting in a tight loop (the diagnostic). Single-task compiles
    that reach the manager together still share a `map_async`.
  - But with a policy at capacity 32 they get most of (b)'s effect on stock Dragon: Fair's
    chain p50 is 1.65 s, and the bulk takes 107 s at 89% busy.
  - A likely reason: a policy refills one slot per completion, so its tasks reach Dragon one at
    a time and each lands in its own `map_async`. This was not checked inside Dragon. Without a
    policy, the patch only halves the chain latency (13.1 s).
  - With (b), (a) lifts E3 at 32 from 533 to 853 tasks/s, the no-policy level. Refills no
    longer wait in the client's linger, which keeps extending while submissions trickle in.
  - It also removes 5 ms from every dependent task (57.1 → 52.0 ms): the client's 5 ms linger.
- **(c) Priorities: nothing at capacity 32; above it, they keep FIFO's and Lineage's order but
  not Fair's.** See the next two sections.
- **(d) The launch budget: no effect**, as the source says (point 4).

**E1b, diamonds:**

| Dragon | No policy, mean completion | No policy, in progress at peak | Lineage 32, mean | Lineage 32, first done | Lineage 32, in progress |
|---|---|---|---|---|---|
| Stock | 19.2 s [19.2–19.3] | 64 | 18.8 s [18.7–18.8] | 3.3 s | 32 |
| Stock + no-change patch | 26.7 s [26.7–26.7] | 230 | 18.4 s [18.4–18.4] | 3.1 s | 32 |
| Branch (a)+(b) | 26.4 s [26.4–26.5] | 230 | 18.4 s [18.3–18.4] | 3.1 s | 32 |
| Branch (a)+(b)+(d), priorities | – | – | 18.4 s [18.3–18.4] | 3.1 s | 32 |

- Lineage is unaffected by the reporting: 18.4–18.8 s in every variant.
- Without a policy, prompt reports make diamonds *slower* (19.2 → 26.4 s; 230 in progress
  instead of 64). On stock Dragon the coalesced reports happened to work depth-first: completions
  came back in bursts, and each burst's t2/t3 overtook the queued t1s.
- With prompt reports the backlog runs breadth-first, so Lineage is worth −30% again
  (simulation: −34%).

### A capacity above the pool size

On the branch with (a)+(b)+(d) and priorities (chart `r2_capacity_tradeoff.pdf`):

| Capacity | E3 throughput, Fair | E3 throughput, Lineage | E1a chain p50, Fair | E1b mean completion, Lineage | E1b first done / in progress |
|---|---|---|---|---|---|
| 32 (pool) | 828/s [826–887] | 868/s [813–883] | 1.60 s [1.59–1.67] | 18.4 s [18.3–18.4] | 3.1 s / 32 |
| 64 | 840/s [819–886] | 794/s [771–820] | 10.5 s [10.3–10.8] | 18.8 s [18.5–18.9] | 5.1 s / 64 |
| 128 | 829/s [735–857] | 818/s [757–900] | 28.2 s [28.1–28.4] | 18.7 s [18.7–18.8] | 5.1 s / 64 |
| ∞ | 894/s [836–900] | 877/s [824–887] | 59.2 s [58.4–59.6] | 18.8 s [18.6–18.8] | 5.1 s / 64 |
| No policy, (a)+(b) | 836/s [791–866] | | 1.86 s | 26.4 s | 21.2 s / 230 |

On stock Dragon with the no-change patch, at 128 without priorities:
- E3: Fair 901/s and Lineage 865/s, against 425/s and 397/s at 32;
- E1a: Fair's chain p50 is 37.4 s [36.4–38.7], with the bulk at 152 s;
- E1b: Lineage's mean completion is 20.1 s, with 92 in progress.

- **No: a larger capacity does not recover E3's throughput without losing E1's latency.**
- **But with per-task reports there is nothing to recover.** At capacity 32 the policies
  already match no policy's throughput, 828–868 against 836 tasks/s. Above 32, throughput stays
  at 794–894/s, within or near the run-to-run spread. Fair's latency is lost: 10.5, 28.2 and
  59.2 s at 64, 128 and ∞.
- **On stock Dragon** (with the patch), capacity 128 does recover E3's throughput, at the cost
  of E1a's latency (37 s) and E1b's order.
- **For Lineage the cost is small:** mean completion +2%, but twice as many workflows in
  progress, and the first done after 5.1 s instead of 3.1 s. The extra capacity admits 64 roots
  at the start instead of 32.

### How much stale priorities matter

- **A priority is fixed at dispatch.** With capacity C above the pool's 32, C − 32 dispatched
  tasks wait inside Dragon, ordered by the priorities they got when they were dispatched.
- **FIFO and Lineage priorities do not go stale.** Ready order and lineage age do not change
  after dispatch.
  - FIFO at 128 keeps the ready order: chain p50 59.2 s, close to FIFO at 32 in the first
    campaign's spaced runs (60.7 s).
  - Lineage keeps its depth-first gain up to ∞ (18.8 s against 26.4 s with no policy).
- **Fair's do.**
  - A new chain's group starts at `min_vruntime`, the virtual runtime of the newest bulk task
    dispatched. So each chain step ranks behind every bulk task already inside Dragon, and
    waits for them to drain.
  - Waiting for step 1 is 3.0, 8.9 and 54.6 s at 64, 128 and ∞; the two gaps between steps add
    6.1 and 17.3 s.
  - At ∞, all 1,000 bulk tasks are dispatched at once, so Fair's priorities equal the dispatch
    order. Fair then behaves as FIFO (59.2 s against FIFO's 59.2 s at 128).
  - In Fair's own semantics (capacity 32) the chain gets the next free slot; once its decisions
    are frozen into Dragon's queue, it cannot.
- **So it matters a lot for Fair:** 1.6 s becomes 10.5 s with just 32 tasks queued in Dragon.
  This is a property of priorities fixed at submission, not of the workload. Any policy whose
  order depends on what happens after dispatch needs either capacity = pool, or a way to
  re-prioritise queued tasks.

### What the submission rate costs with group size 1

- **Without a policy, E3's 2,000 trivial tasks** went from 966/s on stock to 920/s with the patch
  (−5%). On the branch they went from 922/s with (b) to 836/s with (a)+(b) (−9%). (a) alone gave
  902/s.
- **E1's tasks of seconds pay nothing:** the bulk makespan is 98.3 s with (b) and 98.4 s with
  (a)+(b).
- **In exchange, every dependent task is 5 ms faster** (52.0 against 57.1 ms), and a policy at
  capacity 32 reaches full throughput.
- **On a slice of 32 workers, one-task compiles are a good trade.** At much higher rates the
  per-compile cost would grow in importance; that was not measured.

### Same-day baseline

Stock Dragon reproduced the first campaign on the same node:

| Measure | First campaign | This campaign |
|---|---|---|
| E1a chain p50, no policy | 26.9 s | 26.9 s |
| E1a chain p50, Fair 32 | 7.1 s [2.3–9.9] | 2.4 s [2.3–7.6] |
| E3 throughput, no policy / Fair / Lineage | 949 / 393 / 398 tasks/s | 966 / 403 / 385 tasks/s |
| E1b mean completion, no policy / Lineage | 19.2 / 18.7 s | 19.2 / 18.8 s |

The branch's source build does not change timings by itself: (b) alone gives the stock wheel's
57.1–57.2 ms per dependent task.

## Caveats

- **One node.** julia, 64 of 512 CPUs, 32 workers.
  - Another user's 8-CPU GPU job (userA, 24370) ran there the whole time. Other users' jobs
    used 0.4–0.8 CPUs on average and at most 1.1 in any 5 s sample.
  - Nothing here covers several nodes, where the scheduler manager and the placement across
    managers come in.
- **Synthetic tasks.** E1's tasks sleep; E3's are trivial. With CPU-bound tasks, (a)'s 5 ms and
  the throughput numbers would weigh differently.
- **Two Dragon builds.** The stock rows ran the PyPI wheel; the branch rows ran a source build
  (EESSI GCC 13, the compat-layer Python). The (b)-only rows match the wheel's per-task latency,
  so the build does not explain the differences.
- **The priority mappings are mine.** I followed the brief: Fair from the virtual runtime at
  dispatch, Lineage from the lineage, FIFO from the ready order. Other mappings for Fair were
  not tried.
- **What was not run.**
  - (b) alone in E1b.
  - FIFO is only in E1a and E3, at 128 with priorities.
  - Capacity 64 was kept: nothing had to be cut.
- **The split configurations ran last.** (b) alone and (a) alone were added after E1a's first
  repetition, so they ran after the main matrix, not interleaved with it. Their three
  repetitions agree closely (for example, E1a without a policy: 1.93 s in all three).
- **∞ is 10⁹.**
- **Dragon's tests ran on a capped copy** of the test file and on a subset of classes (7 with
  defaults, 5 with per-task reports).

## What this means

- **For Dragon.** Per-task reporting (b) is the request: `apply_async` per task, or a
  `MapResult` that reports per chunk. It turns 59–67% worker use into 96%, and makes every
  policy at capacity = pool work as designed. The client's linger (a) should be optional or
  adaptive for policies that refill one task at a time. The priority option (c) is useful
  only with priorities that do not go stale. The tie-break on tuid strings should be numeric.
- **For RHAPSODY, today, with stock Dragon.** `compile_each_task` plus a policy at capacity =
  pool gets most of the benefit without any Dragon change: Fair's chain latency is 1.65 s and
  the bulk takes 107 s, against 2.4 s and 142 s.
- **For AsyncFlow.** Keep capacity = pool size. Above it, Fair needs a different priority
  scheme, or the backend needs a way to re-prioritise queued tasks.

## Follow-ups

- Show the Dragon developers `diag_batch.py` and `diag_priority.py`, and the `MapResult`
  callback in `_launch_tasks`, before proposing any change. All three branches are local.
- If RHAPSODY's `compile_each_task` is wanted before Dragon changes, it needs a cleaner
  mechanism than patching `__defaults__`, which is process-wide.
- Fair above the pool size: a priority that tracks the group's share would need Dragon to
  re-prioritise queued tasks, or a different mapping. Not explored.
- Multi-node runs (scheduler manager, several subnode managers) and CPU-bound tasks.


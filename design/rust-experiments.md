# Scheduling policies on real Dragon: E1–E3 on rust

Status: experiments, 2026-10-24; the campaign's jobs ran from 19:59 to 23:59 CEST. Everything is
local: nothing pushed, nothing posted to GitHub. The deck's `.tex` is unchanged.

- Scripts: `design/presentations/scheduling-policies/experiments/`.
- Raw results, one JSON file per run: `design/presentations/scheduling-policies/results/raw/`.
- Tables (generated): `results/summary.md` and `results/summary.json`.
- Charts, PDF for Beamer and PNG: `results/*.pdf`, `results/*.png`.
- Diagnostic logs: `results/diag/`.

## Question and answer

**Question.** Do asyncflow's scheduling policies do on real Dragon and real GPUs what the
simulations showed, on stock dragonhpc 0.14.1 with no Dragon change, and what do they cost?

**Answer.**

- **Dragon 0.14.1 changes the picture more than the policies do.** Batch coalesces submissions
  that arrive within 5 ms, and reports such a burst only when all of it is done (see below).
  Batch also does not queue strictly FIFO: a small late submission overtakes a backlog. The
  simulation assumed a FIFO queue that reports each task as it ends.
- **E1, ordering: Fair keeps interactive chains short on real Dragon.**
  - 3-step chains (1.5 s of work) beside 1,000 bulk tasks on 32 workers:
    - stock: Fair's chain latency p50 is 7.1 s, against 26.9 s with no policy and about 100 s
      with FIFO or Lineage;
    - with prompt completion reports (submissions 8 ms apart, no Dragon change): 1.69 s against
      4.0 s and about 60 s.
  - The bulk pays about 1% of makespan against FIFO, and nothing against no policy.
  - Lineage: mean diamond completion −31% against no policy (simulation: −34%), and 32
    workflows in progress instead of 229, for +3% makespan. On stock Dragon no policy already
    behaves almost like Lineage, so the gain there is 3%.
  - FIFO and Lineage make chains slower than no policy. They impose the FIFO order that Dragon
    itself does not keep.
- **E2, GPUs: node mode and RHAPSODY's placement work under load.**
  - On haskell's 2 GPUs, with 12 IMPRESS-like pipelines per GPU:
    - node mode: 0 CUDA OOMs in 6 runs, and every one of 1,791 tasks ran on the GPU asyncflow
      chose;
    - no admission: 4–6 of 24 pipelines lost to OOMs per 12-minute run;
    - aggregate: 0 OOMs on stock, and 1 in 3 runs with prompt reports, when the application's
      pinning put 10 tasks on one GPU.
  - Admission did not raise throughput on one node. No admission completed 3–7% more tasks per
    hour, despite its lost pipelines.
  - On stock Dragon, a kernel ran 68–73% of the time (`nvidia-smi`). With prompt reports it ran
    93–94% of the time, and 34–42% more tasks completed per hour.
  - julia's GPU (an RTX PRO 6000 Blackwell Server Edition) was taken by another user all
    evening, so the 8-node failure of aggregate mode could not be tested: it needs Dragon to pick
    among GPU nodes.
- **E3, cost: no measurable latency cost.**
  - 57.1 ms per dependent task with every policy, as with none.
  - A capacity equal to the pool size halves the throughput of trivial tasks: 390–515 against
    950–990 tasks/s. That is not the policy's own computation. **Corrected by the second
    campaign:** two Dragon behaviours hold back a capacity-limited policy's refills.
    - Dragon reports each dispatch round only when all of it is done. Per-task reports alone
      (a local Dragon branch) bring Fair at 32 to 533 tasks/s.
    - Batch's client lingers 5 ms to group submissions. One-task compiles as well bring Fair
      to 853 tasks/s, as many as no policy (836/s).

    See [rust-experiments-2.md](rust-experiments-2.md).
  - Placed functions cost 693 ms each, because each is a Dragon process.
- **For the next Dragon conversation**, the first request is to report each task when it
  finishes, or to let a client turn coalescing off. Here it is worth 34–42% more GPU tasks per
  hour, and Fair's chain latency drops from 7.1 s to 1.69 s.
- Every configuration ran 3 times. One run failed inside Dragon (a placed function, job 24549)
  and was rerun. No job was preempted.

## Found first: Dragon reports a burst of tasks only when all of it is done

This shapes every result below, so it comes first.

- **What happens.** Dragon 0.14.1's `Batch` client coalesces submissions that arrive within 5 ms
  of each other, up to 256, into one compiled batch (`client_task_batch_linger_sec = 0.005`,
  `client_task_batch_maxsize = 256` in `dragon/workflows/batch/facts.py`). `Batch.poll()`
  reports the tasks of a compiled batch only when all of them have finished.
- **Raw Batch, no RHAPSODY** (`diag_batch.py`, job 24476, haskell, 8 workers). 6 sleep functions of
  1–6 s:
  - Submitted in a tight loop: each ended at +1.01 … +6.01 s, and `poll()` returned every one
    at +6.02 s.
  - Submitted 50 ms apart: `poll()` returned each one within 0.05 s of its end.
- **Through RHAPSODY and asyncflow** (`diag_completion.py`, job 24473, julia), the same 6 tasks with
  no policy: asyncflow saw all of them at +6.07 s (functions) and +6.38 s (executables), whatever
  their own end time.
- **Why it matters for policies.** asyncflow submits every ready task it has at once: all of
  them without a policy, and whatever the policy dispatches in one round with one. Those land
  in one compiled batch, so a task's completion waits for the slowest task submitted with it.
  A capacity-limited policy then refills its slots in waves: it learns that slots are free only
  when a whole wave ends, and the workers that finished early sit idle until then.
- **A variant with no Dragon change.** `common.make_backend(spaced_ms=8)` wraps RHAPSODY's
  `DragonExecutionBackend` so that every task is submitted at least 8 ms after the previous one.
  Each task is then its own compiled batch and is reported when it ends. It caps submission at
  about 125 tasks/s, so it is a diagnostic, not a fix. E1 and E2 ran both ways:
  - **stock:** RHAPSODY and Dragon as they are, which is what users get today;
  - **spaced:** submissions 8 ms apart, which shows what the policies do once completions are
    reported when tasks end.

## Setup

### Cluster

- **rust**, Slurm 24.11.5, Proxmox VMs, kernel 6.17.2-1-pve. There is no HSTA fabric, so Dragon runs
  over TCP (`dragon -w slurm -t tcp`, `--network-prefix` including `vmbr`).
- **haskell:** AMD EPYC 9755 (2 × 128 cores, SMT 2, 512 CPUs), 440 GiB. 2 × NVIDIA RTX 6000 Ada
  Generation, 49,140 MiB each, PCI `11:00.0` and `61:00.0`, driver 580.126.09 (CUDA 13.0).
  The CUDA driver reports 47.37 GiB per GPU (`cuMemGetInfo`).
- **julia:** AMD EPYC 9745 (2 × 128 cores, SMT 2, 512 CPUs), 502 GiB. 1 × NVIDIA RTX PRO 6000
  Blackwell Server Edition (`lspci`: GB202GL, `10de:2bb5`; Slurm calls it `rtx6000bb`). Its memory
  was not measured: the GPU was held by another user's job all evening (see E2).
- **QOS `normal`:** at most 128 CPUs and 2 GPUs per user, in total over running jobs. Every run
  holds part of a node. Partitions `cpu`, `gpu` and `interactive` have `OverSubscribe=FORCE:2`
  (another job may share cores), and `gpu` has `PreemptMode=REQUEUE`. None of my jobs was
  preempted or requeued (`sacct -D`).

### Software

All runs used the stock **dragonhpc 0.14.1** wheel from PyPI (Python 3.11.4 from EESSI,
`/home/mturilli/tmp/dragon-rust/venv`). No Dragon branch was used. The code trees were exported
with `git archive` and copied to `/home/mturilli/tmp/sched-exp/trees/`; each has a `COMMIT` file,
and every result file records the commits it ran with.

| Tree | Branch | Commit | Used for |
|---|---|---|---|
| `af-ordering` | asyncflow `feature/ordering-policies` | `b002de3` | E1; E3 none, FIFO, Lineage, Fair |
| `af-resource` | asyncflow `feature/resource-policy` | `6285975` | E2; E3 ResourcePolicy |
| `rh-fix` | RHAPSODY `fix/dragon-affinity-sizing` | `d2cbc58` | E1; E3 (Batch pool capped to the job's CPUs) |
| `rh-merged` | RHAPSODY `integration/placement-affinity` | `bd777d1` | E2; E3 with placement honoured |

`bd777d1` is a new local branch: `d2cbc58` with `feature/dragon-placement` (`762c5b3`) merged in, in
the worktree `scratchpad/trees/rhapsody-merged` of this session. The only conflicts were the
ones the Dragon findings predicted: the import block (kept `Policy`, `Node`, `System`) and
`CHANGELOG.md` (kept both entries). Both Dragon-free test files pass on the merge (28 tests).

### Jobs

- Two job streams, one per node, run one job at a time with `--dependency=singleton`:
  - julia, partition `cpu`, 64 CPUs, so Batch starts 32 workers: E3 and E1.
  - haskell, partition `gpu`, `--gres=gpu:2`, 56 CPUs, so 28 workers: E2.
  - Together 120 CPUs and 2 GPUs, inside the QOS limit.
- **Two Dragon jobs of one user must not share a node.** In the first smoke tests, job 24469
  failed at once and ran its epilogue, `srun dragon-cleanup`. Job 24470, started on the same node
  at the same second, was SIGKILLed 5 s later. Hence one stream per node.
  - A direct test confirmed the cause (jobs 24663 and 24664, logs in `results/diag/`).
    `srun dragon-cleanup` in one job killed a running Dragon runtime in another job of mine on
    the same node: Slurm cancelled its step, and my Dragon processes there went from 11 to 0.
  - Inside a Slurm job, `dragon-cleanup` also tries to SSH to the job's nodes. That fails ("No
    authentication methods available"), yet it exits 0. So it kills processes but does not
    clean `/dev/shm`.
  - The empty `/dev/shm` after each of my runs came from Dragon's own clean shutdown, not from
    the epilogue.
  - `dragon-cleanup -s` (local only) also tries to restart the `nvidia-cuda-mps-control` daemon.
    In my check (job 24662, julia, no GPU in the job) that failed with status 100 and changed
    nothing, but it is not a command to run on a GPU node other users share.
- Every configuration ran 3 times. The configurations of a stream were interleaved, in a
  different shuffled order in each repetition.
- `run.sbatch` records, for every run: the jobs on the node at start and end (`squeue -a`), and
  every 5 s the load average, the CPUs busy on the node (`/proc/stat`) and the CPUs this job used
  (its cgroup), so the difference is what other jobs used. With GPUs, it also samples
  `nvidia-smi` once a second. `merge_meta.py` adds all of it to the run's result file.
- The first 5 jobs (24479–24482 and 24536) started before two recorder bugs were fixed:
  `squeue` without `-a` hid other users' jobs in hidden partitions, and the load sampler used
  `load`, a gawk builtin, as a variable name and wrote nothing. For those runs, the
  co-running jobs come from Slurm accounting (`corunners.py` → `raw/corunners.json`, done for
  every run), and there are no load samples. Job 24549 (failed, cancelled) has none either.

## E3: what policies cost on real Dragon

**Setup.** julia, 64 CPUs, 32 Batch workers. Each job ran the configurations of one tree pair, in
a shuffled order, each with a fresh `DragonExecutionBackend` and `WorkflowEngine` in the same
Dragon runtime. Per configuration: a warm-up of 32 trivial tasks; then a chain of 100 trivial
function tasks, each taking the previous result, registered at once; then 2,000 independent
trivial tasks registered at once. Policy capacity is 32. `ResourcePolicy` gets one node of 32
CPUs and every task asks for 1 CPU, so it also keeps 32 in flight. In node mode it writes
`task["placement"]`, which only the placement tree (`rh-merged`) honours: a placed function then
runs as its own Dragon process instead of in a pool worker.

| Trees | Policy | Per-task latency (chain of 100) | Throughput (2,000 tasks) |
|---|---|---|---|
| ordering + rh-fix | No policy | 57.1 ms [57.1–57.2] | 949/s [916–969] |
| ordering + rh-fix | FIFO | 57.2 ms [57.1–57.4] | 429/s [421–455] |
| ordering + rh-fix | Lineage | 57.2 ms [57.0–57.3] | 398/s [396–400] |
| ordering + rh-fix | Fair | 57.1 ms [57.1–57.1] | 393/s [374–420] |
| resource + rh-fix | No policy | 57.1 ms [57.0–57.4] | 970/s [966–990] |
| resource + rh-fix | Resource, aggregate | 57.1 ms [57.1–57.6] | 515/s [500–518] |
| resource + rh-fix | Resource, node (placement ignored) | 57.1 ms [57.1–57.1] | 388/s [367–532] |
| resource + rh-placement | No policy | 57.1 ms [57.1–57.1] | 955/s [950–986] |
| resource + rh-placement | Resource, node, placed | 693.2 ms [686.8–696.6] | 28/s [26–29] |

Median [min–max] of 3 runs. Jobs: 24479–24482, 24546–24548, 24550, 24613 (`summary.md` has them
per row). Chart: `e3_throughput.pdf`.

- **Latency: no measurable cost.** A dependent task takes 57.1 ms end to end with every policy,
  as without one. That is the round trip through asyncflow, RHAPSODY and Dragon. In the deck's
  microbenchmarks, `ResourcePolicy` costs 15–28 µs per task and the engine itself about 31 µs:
  three orders of magnitude less, and invisible here.
- **Throughput: capacity = pool size halves it for trivial tasks.** It reached 390–515/s,
  against 950–990/s without a policy, when Dragon pipelines the whole backlog. This is not the
  policy's own computation.
  - I first explained it by Little's law: 32 tasks in flight over a 57 ms round trip,
    ≤ 560 tasks/s. **The second campaign showed that the capacity is not the limit.** On the
    local Dragon branch `dbf5405`, Fair at capacity 32 reaches:
    - 533 tasks/s [518–567] with per-task reports alone;
    - 853 tasks/s [790–873] once the client compiles each task on its own, against 836/s with
      no policy on the same branch.
  - So two Dragon behaviours held back a capacity-limited policy's refills: a dispatch round
    reported only when all of it is done, and Batch's client lingering 5 ms to group
    submissions.
  - On the stock wheel with one-task compiles, capacity 128 also recovers it (901/s), but at a
    large cost to E1's latency ([rust-experiments-2.md](rust-experiments-2.md)).
- **Placed functions are expensive.** Node mode with RHAPSODY's placement runs every function as
  its own Dragon process: 693 ms per task (12 times the pool path) and 28 tasks/s. Placement
  suits GPU and executable tasks (E2), not trivial functions.
- **One failure.** In job 24549 (placed node mode, rep 3), one of the 2,000 placed functions
  failed inside Dragon: `Batch` `task.get()` raised `TypeError: 'NoneType' object is not
  subscriptable`. The script then hung at exit, holding 35 CPUs, and I cancelled it after 16 min.
  It left 20 `/dev/shm` files (about 14 GB) on julia, which I removed by modification time
  without touching the next job. Job 24613 replaced the run. The three other placed runs
  (24480, 24546, 24613), 6,396 placed functions in all, had no failure. The scripts now record
  such a failure and exit instead of hanging.

## E1: ordering on real Dragon

**Setup.** julia, 64 CPUs, 32 Batch workers, asyncflow `b002de3` with RHAPSODY `d2cbc58`, one
policy per job. Policy capacity 32 (the pool size). `FairPolicy` has no weights, as in the
simulation. Every task sleeps for its duration in a Batch worker and returns the worker's start
and end times. Durations are drawn from a seed per repetition, so all policies see the same
tasks. Each configuration ran stock and spaced (see above), 3 times each, 48 jobs in all
(24551–24598, all completed).

### E1a: bulk tasks beside interactive chains (ROSE-like)

1,000 bulk tasks of uniform(1, 5) s in a `bulk` workflow scope, registered at once. Every 6 s
(12 times, from 6 s to 72 s), an interactive chain of 3 tasks of 0.5 s, each in its own
`interactive:<i>` scope. With 32 workers the bulk needs 94.0–95.1 s; a chain needs 1.5 s.

| Submission | | No policy | FIFO | Lineage | Fair |
|---|---|---|---|---|---|
| stock | Chain latency, p50 | 26.9 s [26.9–27.4] | 102.5 s [101.9–103.6] | 100.2 s [98.7–103.4] | **7.1 s [2.3–9.9]** |
| stock | Chain latency, p95 | 28.9 s [28.6–29.0] | 134.9 s [131.8–137.0] | 132.5 s [131.9–132.8] | **12.1 s [11.3–14.8]** |
| stock | Bulk makespan | 161.2 s [161.1–161.3] | 139.8 s [139.6–141.9] | 137.3 s [136.7–141.1] | 141.5 s [139.5–141.6] |
| stock | Workers busy (bulk) | 59% | 67% | 68% | 67% |
| spaced | Chain latency, p50 | 4.0 s [2.1–5.6] | 60.7 s [59.9–60.9] | 60.6 s [59.8–60.9] | **1.69 s [1.68–1.71]** |
| spaced | Chain latency, p95 | 7.6 s [7.4–9.3] | 90.4 s [89.7–90.7] | 90.3 s [89.7–90.5] | **1.85 s [1.76–2.04]** |
| spaced | Bulk makespan | 105.6 s [104.3–110.8] | 99.1 s [98.0–99.2] | 99.0 s [97.9–99.2] | 99.7 s [98.5–100.0] |
| spaced | Workers busy (bulk) | 89% | 96% | 96% | 95% |

Where a chain's time goes (mean over a run's 12 chains, median of 3 runs; chart
`e1_chain_breakdown.pdf`; the parts are medians over runs, so their sum can differ slightly
from the total):

| Submission | Policy | Mean chain latency | Waiting for step 1 | Between steps (2 gaps) | Steps running | Done, not yet reported |
|---|---|---|---|---|---|---|
| stock | No policy | 26.8 s | 2.4 s | 18.5 s | 1.5 s | 4.4 s |
| stock | FIFO | 99.7 s | 97.2 s | 1.0 s | 1.5 s | 0.0 s |
| stock | Lineage | 97.0 s | 95.0 s | 0.4 s | 1.5 s | 0.0 s |
| stock | Fair | 5.4 s | 0.8 s | 2.0 s | 1.5 s | 1.2 s |
| spaced | No policy | 4.0 s | 0.6 s | 1.7 s | 1.5 s | 0.3 s |
| spaced | FIFO | 57.7 s | 56.0 s | 0.1 s | 1.5 s | 0.0 s |
| spaced | Lineage | 57.6 s | 56.0 s | 0.1 s | 1.5 s | 0.0 s |
| spaced | Fair | 1.7 s | 0.1 s | 0.1 s | 1.5 s | 0.0 s |

What it shows:

- **Fair gives the shortest chains on real Dragon, in both variants.** p50 7.1 s on stock (4
  times shorter than no policy), and 1.69 s with prompt completion reports: 0.19 s above the
  chain's own 1.5 s, beside 1,000 bulk tasks. The bulk pays about 1% of makespan against FIFO
  (+1.2% stock, +0.6% spaced), and nothing against no policy.
- **Dragon's queue is not the FIFO queue the simulation assumed.** With no policy, a chain's
  first step, submitted while hundreds of bulk tasks were still waiting, started on average
  0.6 s (spaced) or 2.4 s (stock) later. The bulk tasks themselves started mostly in submission
  order, with exceptions (spaced run 24557: bulk task 999 started at 11 s, task 500 at 49 s). So
  without a policy, a small late submission overtakes the backlog. In the simulation, no policy
  left the chains waiting behind most of the bulk (905 ms beside a 1.39 s bulk makespan); on
  Dragon it does not.
- **FIFO and Lineage make chains slower than no policy.** They hold the backlog in asyncflow in
  ready order, so a chain waits behind the bulk: 56–97 s before its first step starts. Lineage
  cannot help here, because every bulk task is its own lineage and older than the chains. This
  matches the simulation, where FIFO and Lineage did not shorten chains either; but there "no
  policy" was as slow, and on Dragon it is not.
- **Stock Dragon's completion reporting dominates the stock rows.** With no policy, a chain
  loses 18.5 s between its steps (mostly waiting for the previous step's completion to be
  reported) and 4.4 s after its last step; only 59% of the workers' time is used, and the bulk
  takes 161 s for 94 s of work. With a policy, the waves of 32 leave a third of the workers'
  time unused (67% busy, 140 s). Fair's stock latency varies most (2.3–9.9 s), because a chain step is dispatched with whatever
  bulk tasks free up in the same round and is reported when the slowest of them ends.
- **The bulk.** Spaced, every policy finishes the bulk in 99–100 s, 4–5% above the 94–95 s of
  work; no policy takes 105.6 s.
- **Against the simulation** (16 workers, 5 ms tasks, FIFO backend): there Fair cut chain p50 from
  905 ms to 20 ms (45 times) for +3.7% bulk makespan. On Dragon, with prompt reports, Fair cuts it
  from 4.0 s to 1.69 s (2.4 times) against no policy, and from 60.7 s to 1.69 s (36 times) against
  FIFO, for +0.6% makespan against FIFO and −6% against no policy.

### E1b: concurrent diamond workflows

256 diamonds t1 → (t2, t3) → t4, 1 s per task, all registered at once (1,024 tasks, 32 s of work
on 32 workers). "In progress" counts diamonds whose t1 is done and t4 is not, as in the
simulation.

| Submission | | No policy | FIFO | Lineage | Fair |
|---|---|---|---|---|---|
| stock | Mean workflow completion | 19.2 s [19.2–19.2] | 30.3 s [30.1–30.3] | **18.7 s [18.6–18.8]** | 30.1 s [30.1–30.4] |
| stock | First workflow done | 5.3 s [5.3–5.3] | 26.3 s [26.2–26.3] | **3.2 s [3.2–3.2]** | 26.3 s [26.3–26.4] |
| stock | Makespan | 34.0 s [34.0–34.0] | 34.2 s [33.8–34.2] | 35.3 s [35.0–35.8] | 33.9 s [33.8–34.1] |
| stock | In progress at peak | 64 | 256 | **32** | 256 |
| spaced | Mean workflow completion | 26.5 s [26.5–26.5] | 29.6 s [29.6–29.7] | **18.4 s [18.4–18.5]** | 29.6 s [29.5–29.6] |
| spaced | First workflow done | 21.4 s [21.4–21.5] | 25.8 s [25.8–25.9] | **3.1 s [3.1–3.1]** | 25.8 s [25.6–25.8] |
| spaced | Makespan | 33.5 s [33.5–33.6] | 33.6 s [33.5–33.6] | 34.5 s [34.4–35.6] | 33.4 s [33.4–33.5] |
| spaced | In progress at peak | 229 [229–230] | 256 | **32** | 256 |

Chart: `e1_diamonds_completion.pdf`.

- **Lineage reproduces the simulation.** Spaced: mean completion −31% against no policy (18.4
  against 26.5 s; the simulation had −34%), the first workflow done at 3.1 s instead of 21.4 s,
  and 32 workflows in progress at most instead of 229 (the simulation: 16, its capacity, instead
  of 512). Makespan +3% (34.5 against 33.5 s).
- **On stock Dragon, no policy already behaves almost like Lineage** (19.2 s mean, 64 in
  progress). The first diamond finished at 5.3 s, before the 256 t1s could all have run (8 s on
  32 workers): its t2, t3 and t4, submitted later in small groups, overtook the queued t1s, as
  the chain steps did in E1a. Lineage still gets the first workflow done sooner (3.2 against
  5.3 s) and halves the peak, but the mean improves by only 3%.
- **FIFO and Fair are breadth-first here** (256 in progress, first done after 26 s): Fair shares
  the slots among 256 equal lineages, and FIFO serves the t1s first.

## E2: packing GPUs by memory

**Setup.** haskell, partition `gpu`, `--gres=gpu:2`, 56 CPUs (28 Batch workers), asyncflow
`6285975` with RHAPSODY `bd777d1` (placement + pool cap). Only haskell's 2 GPUs: julia's GPU was
held all evening by another user's priority job (userA, job 24370, `group1-priority`, until
2026-10-25 16:07), with another user's job queued for it, and the QOS allows 2 GPUs per user.

- **Workload.** 24 IMPRESS-like pipelines, 12 per GPU, which is 1.5 times IMPRESS's operating point
  of 8 per GPU. Each pipeline loops over its GPU tools one task at a time, as IMPRESS's adaptive
  chain does: rfd3 → mpnn (again with p = 0.70) → boltz (p = 0.32) or rfd3; boltz → rfd3. The
  CPU-only steps are left out. Each run has a 12-minute window. After it, pipelines register no
  new tasks. Tasks already registered still run (the drain), including some still waiting for
  admission. Only completions inside the window count; OOMs count whenever they happen.
- **Tasks** (from `impress_model.py` of the S1 prototype; durations halved):

  | Tool | Median duration | True peak, incl. context | Declared | GPU share |
  |---|---|---|---|---|
  | rfd3 | 37.2 s | N(5.0, 0.4) GB | 6 GB = 5.59 GiB | 1/8 |
  | mpnn | 5.6 s | N(1.5, 0.2) GB | 2 GB = 1.86 GiB | 1/8 |
  | boltz | 23.9 s | N(4.0, 0.8) GB | 6 GB = 5.59 GiB | 1/8 |

  Durations are lognormal (σ 0.25). True peaks are clipped to [0.5 GB, declared]. Every task is
  `gpu_task.py`, an executable task, so a Dragon process. It creates a CUDA context (0.42 GiB,
  measured), allocates the rest of its true peak in one block, and holds it for its whole run.
  Every 2 s it runs a 0.5 s burst of a hand-written PTX kernel (0.5 s on an idle GPU; 79,000
  iterations per ms, measured on the RTX 6000 Ada), then sleeps 1.5 s. The 25% GPU share comes
  from the model. On a shared GPU the bursts take longer, so the task does too.
- **CUDA OOM.** `cuCtxCreate` or `cuMemAlloc` returning `CUDA_ERROR_OUT_OF_MEMORY` exits with code
  3. RHAPSODY reports the task FAILED and the pipeline ends, as in IMPRESS.
- **Admission modes:**
  - **No admission:** no policy. Pipeline p pins GPU p mod 2 through `CUDA_VISIBLE_DEVICES`, as
    IMPRESS pins pipeline N to GPU (N−1) mod 4.
  - **Aggregate:** `ResourcePolicy(Pool([NodeShape(cpus=28, gpus=2, gpu_mem=47.0,
    name="haskell")], mode="aggregate"))`. Every task declares `{"cpus": 1, "gpus": 0.125,
    "gpu_mem": <declared>}`. The pipeline still pins the GPU.
  - **Node:** the same pool in node mode. The policy picks the GPU, and RHAPSODY's placement
    branch runs the task with a Dragon `Policy(host_name="haskell", gpu_affinity=[g])`. Dragon
    sets `CUDA_VISIBLE_DEVICES`.
- **Placement verified on every task:** each task records the PCI bus id of the GPU it used. In
  every run of every mode, it matched the GPU its `CUDA_VISIBLE_DEVICES` named (pinned by the
  pipeline or placed by Dragon).
- **Measured:**
  - CUDA OOMs and pipelines lost.
  - Tasks completed in the window, per hour.
  - GPU-seconds per completed task: 2 GPUs × 720 s / completed tasks.
  - GPU utilisation and memory used, from `nvidia-smi` once a second over the window.
- **Batch's pool caps GPU tasks too.** A Dragon process task holds a Batch worker until its
  process exits (`JobCore.run` joins its `ProcessGroup`), so at most 28 GPU tasks run at once,
  more than the 24 pipelines.

**Results.** Median [min–max] of 3 runs per row, 18 runs in all (jobs 24536, 24599–24606,
24614–24622, all completed). Charts: `e2_cuda_ooms.pdf`, `e2_gpu_seconds_per_task.pdf`,
`e2_gpu_memory_stock.pdf` and `e2_gpu_memory_spaced8ms.pdf`.

| Submission | Admission | CUDA OOMs | Pipelines lost (of 24) | Tasks done per hour | GPU-seconds per task | GPU busy | GPU memory peak |
|---|---|---|---|---|---|---|---|
| stock | None (app pins GPU) | 5 [4–5] | 5 [4–5] | 1,240 [1,070–1,280] | 5.8 [5.6–6.7] | 73% [68–75] | 46.7 GiB [45.5–47.0] |
| stock | Aggregate (app pins GPU) | **0 [0–0]** | **0** | 1,190 [1,095–1,235] | 6.1 [5.8–6.6] | 71% [65–72] | 37.3 GiB [37.0–38.3] |
| stock | Node (asyncflow places) | **0 [0–0]** | **0** | 1,160 [1,060–1,180] | 6.2 [6.1–6.8] | 68% [65–69] | 37.3 GiB [37.0–38.3] |
| spaced | None (app pins GPU) | 4 [4–6] | 4 [4–6] | 1,690 [1,545–1,725] | 4.3 [4.2–4.7] | 94% [93–94] | 46.2 GiB [46.0–46.9] |
| spaced | Aggregate (app pins GPU) | 0 [0–1] | 0 [0–1] | 1,600 [1,570–1,680] | 4.5 [4.3–4.6] | 93% [93–93] | 43.0 GiB [43.0–44.2] |
| spaced | Node (asyncflow places) | **0 [0–0]** | **0** | 1,645 [1,605–1,695] | 4.4 [4.2–4.5] | 94% [93–95] | 38.4 GiB [38.3–38.5] |

"GPU busy" is `nvidia-smi`'s utilisation, averaged over both GPUs and the window.
GPU-seconds per task = 2 GPUs × 720 s / tasks done.

What it shows:

- **Node mode had no CUDA OOM in 6 runs.** It ran 1,791 tasks. Every one ran on the GPU asyncflow
  chose: its PCI bus id matched the `CUDA_VISIBLE_DEVICES` that Dragon set from the placement.
  This is the first run of asyncflow's placement through RHAPSODY to Dragon under GPU load.
  Across the 18 runs, all 5,429 tasks that opened a context used the GPU their environment named.
- **No admission lost 4–6 of 24 pipelines per run** to CUDA OOMs.
  - 27 of the 28 OOMs came in the first second, when 12 first tasks per GPU (rfd3, 4.7 GiB on
    average) allocated together.
  - One came later: spaced run 24619 at 320 s, with 9 tasks holding 43.6 GiB on that GPU.
- **Aggregate admission failed once in 6 runs.**
  - Spaced run 24620, at 727 s (a task admitted during the drain). The pipelines' pinning had put
    10 tasks on GPU 1, holding 46.6 GiB, and the 11th asked for 1.2 GiB. The pool's totals
    still fit.
  - It did not happen on stock Dragon, where aggregate mode's memory peaked at 37–38 GiB (43–44
    GiB spaced). A likely reason, not checked: the lockstep waves make many pipelines ready
    together, in pipeline order, so oldest-first admission alternates GPU 0 and GPU 1.
  - So the simulation's point holds even on one node: totals that fit can still overload one GPU
    when the application picks it. It is rarer here than on 8 nodes, where Dragon also picks the
    node.
- **Admission did not raise throughput on one node.**
  - No admission completed 3–7% more tasks per hour than node mode, even after losing a fifth of
    its pipelines in the first second. It ran up to 12 tasks per GPU; node mode at most 8.
  - In node mode the declared share (1/8 GPU, from the S1 model) bound before memory: its peak
    was 37–38.5 GiB of 47.4. The declared memory (5.59 GiB for rfd3 against a true 4.66 GiB on
    average) is conservative too.
  - The simulation's gain (28% more passing folds than the staggered start, on 8 nodes) was
    measured over 4 hours, on 8 nodes, against a staggered start. None of the three was
    reproduced here, so the throughput numbers do not compare, and nothing was extrapolated.
- **Stock Dragon's reporting leaves the GPUs without a kernel about 30% of the time.**
  - Pipelines advance in lockstep: a task's completion waits for the slowest task submitted with
    it. Both GPUs drop to nearly empty between waves (`e2_gpu_memory_stock.pdf`).
  - Average GPU busy is 68–73% on stock and 93–94% spaced. Tasks per hour rise by 34–42% in
    every mode with prompt reports.

## What each experiment shows, and does not

| | Shows | Does not show |
|---|---|---|
| E1 | Fair shortens interactive chains on real Dragon, beside real queueing, at about 1% makespan. With prompt completion reports, Lineage finishes workflows a third sooner, with 32 in progress instead of 229 (on stock Dragon, 3% sooner). On Dragon, FIFO and Lineage policies make chains slower than no policy. | Chains beside CPU-bound work (tasks sleep). Weights for Fair. More than one node. A capacity above the pool size (prefetch). |
| E2 | asyncflow's node-mode placement reaches the GPU through RHAPSODY and Dragon (bus ids checked on every task), with no CUDA OOMs at 12 pipelines per GPU. No admission loses pipelines. Aggregate admission with application pinning can overload one GPU even on one node (once in 6 runs). | The 8-node aggregate failure (one GPU node available). A throughput gain from admission (none on one node in 12 minutes). Learned memory (no peaks reported). Real IMPRESS tools; the CPU steps; host memory pressure. |
| E3 | Policies add no measurable latency per task on Dragon (57.1 ms in every configuration). Capacity = pool size halves trivial-task throughput on stock Dragon (second campaign: because of the reporting, not the capacity); placed functions cost 693 ms each. | The policies' CPU cost in isolation (the microbenchmarks cover it). Throughput with prefetch, or with prompt completion reports (both in the second campaign). |

## Follow-ups

- **Dragon:** ask that `Batch.poll()` report each task when it finishes, or that a client can
  turn off coalescing (`client_task_batch_linger_sec`/`client_task_batch_maxsize` are module
  constants, bound as defaults at import). Show them `diag_batch.py`. The same note covers
  the non-FIFO dispatch seen in E1.
- **RHAPSODY or asyncflow, until Dragon changes:** decide whether `DragonExecutionBackend`
  should submit tasks in separate compiled batches when a policy is set. The spaced wrapper
  here caps submission at 125 tasks/s, so a real fix needs a way that does not throttle
  submission; not explored.
- **Capacity:** measure a prefetch (capacity above the pool size) for short tasks (E3) and for
  Fair (E1).
- **Two GPU nodes:** repeat E2 aggregate against node mode on haskell + julia when julia's GPU
  is free. With the 2-GPU QOS limit that means 1 GPU per node.
- **Learned memory:** rerun E2 node mode with the RHAPSODY peaks branch
  (`feature/dragon-observed-peaks`) and a `MemoryModel`, to see whether it packs more than the
  declared 1/8 share allows.
- **Placed functions:** the `TypeError` inside Dragon (job 24549) is worth a reproducer before
  the placement PR. It happened in 1 of 4 runs of about 2,100 placed functions each.

## Caveats

- **Shared nodes.**
  - julia ran another user's 8-CPU GPU job (userA, 24370) the whole time, plus at times a
    2-CPU job (userC, 24608, 24625). haskell had a 2-CPU interactive job (userC, 24607).
  - In the 5 s samples, other users' jobs used 0.68 CPUs on julia (mean over 63 runs) and 0.07 on
    haskell, and never more than 1.11 CPUs in any sample.
  - `OverSubscribe=FORCE:2` would have allowed core sharing; with so little foreign load it
    cannot explain the differences reported here.
  - Every run's co-runners are listed in `summary.md`.
- **Partial nodes.**
  - Each run held 64 of julia's 512 CPUs, or 56 of haskell's. The Batch pool was capped to them
    by RHAPSODY `d2cbc58` (32 and 28 workers).
  - Whole nodes are impossible under the QOS, and nothing here says how Dragon behaves with 256
    workers.
- **One node per experiment.**
  - Dragon never chose between nodes. The failure the 8-node simulation found for aggregate
    admission (Dragon picks the node, the application picks the GPU) needs at least two GPU
    nodes, and rust offered one.
  - julia's GPU was taken, and the QOS allows 2 GPUs per user.
- **Synthetic tasks.**
  - E1 tasks sleep, so they hold a worker but no CPU. E3 tasks are trivial.
  - E2 tasks are synthetic CUDA processes: a fixed footprint allocated at start and held, and
    fixed GPU bursts.
  - Their footprints and durations come from the S1 IMPRESS model, which is itself estimated
    for GPU memory. Durations are halved, and the CPU-only IMPRESS steps are left out.
  - No real model ran.
- **Two nodes, two CPU models.** E1 and E3 ran on julia (EPYC 9745), E2 on haskell (EPYC 9755).
  E1's spaced and stock variants ran on the same node, interleaved, so they compare directly.
- **The spaced variant is a diagnostic, not a proposal.**
  - It changes the client only: Dragon is the stock wheel.
  - It caps submission at about 125 tasks/s, which is why E3 did not use it.
- **Timing.**
  - Every time comes from the scripts' own clocks: `time.time()` in workers and processes, and
    `time.perf_counter()` in the client.
  - Each experiment ran on one node, so there is no clock skew. `nvidia-smi` timestamps are
    local time (+02:00, recorded per run).
- **Policy settings.**
  - Capacity equals the pool size, with no prefetch.
  - `FairPolicy` has no weights, as in the simulation.
  - `ResourcePolicy` used declared memory only: RHAPSODY's Dragon backend reports no peaks on
    this stack, so learned memory was not tested.
- **Recorder gaps.**
  - 5 early jobs and the failed job 24549 have no load samples (see Jobs). Their co-runners come
    from `sacct`.
  - Job 24549 has a hand-written failure record (`raw/e3_res-node_r3_24549.json`).
- **Not repeated across days.** All runs were on one evening. The spread between repetitions is
  small (see the min–max ranges), but nothing shows how much day-to-day variation there is.

## Files

- `experiments/`:
  - `common.py`: backend, policies, run records, and the spaced-submission wrapper.
  - `e1_ordering.py`, `e2_gpu.py`, `e3_overhead.py`: the experiments.
  - `gpu_task.py`: the synthetic CUDA task; `--calibrate` measures the kernel rate and the
    context size.
  - `diag_batch.py`, `diag_completion.py`: the Batch reporting diagnostics.
  - `run.sbatch`: environment, node recording, Dragon, cleanup.
  - `merge_meta.py`: adds the node state to the result files.
  - `submit_campaign.sh`: every job, in order.
  - `corunners.py`: co-runners from `sacct`.
  - `analyze.py`: `summary.md`, `summary.json` and the charts. It needs matplotlib.
- `results/raw/`: 94 JSON files, one per run and configuration (including the hand-written
  failure record of job 24549), plus `corunners.json`. Each file has:
  - the trees' commits, the Slurm job id and node, and the parameters;
  - per-task worker times (E1), per-step times (E3), or every GPU task record with its bus id
    (E2);
  - the node state (co-runners, load samples, `nvidia-smi` samples).
- `results/diag/`: the logs of the two Batch diagnostics (jobs 24473 and 24476), and of the
  `dragon-cleanup` check (jobs 24662–24664), with its scripts.
- On rust, `/home/mturilli/tmp/sched-exp/`:
  - `trees/`, `experiments/`, `results/`, `logs/`;
  - `runs/<job>/`: load and GPU samples, Dragon's runinfo, and each E2 task's record.

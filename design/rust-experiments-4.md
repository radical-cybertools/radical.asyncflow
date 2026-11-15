# Completions without Batch's 50 ms wait: fourth rust campaign

Status: experiments, 2026-10-25, on julia. Everything is local: one new local Dragon commit,
nothing pushed, nothing posted to GitHub. The deck's `.tex` is unchanged.

The third campaign ([rust-experiments-3.md](rust-experiments-3.md), diagnostic D) found that
Batch's `poll()` reports a lone task about 47 ms after it ends. This campaign removes that wait
behind a switch, (e), and reruns E3 and E1a through AsyncFlow + RHAPSODY with and without it.

Files:
- Scripts: `design/presentations/scheduling-policies/experiments/`. New: `submit_campaign4.sh`,
  `analyze4.py`; `common.py` gained the spec flag `e`.
- Raw results: `results/r4/raw/` (30 results and `corunners.json`).
- Tables: `results/r4/summary.md` and `summary.json`.
- Charts: `results/r4/r4_e3_steps` and `r4_e3_e1a`, PDF and PNG.
- Dragon tests and the raw check: `results/r4/diag/`.
- Diffs: `results/r4/diffs/dragon-0a7d87d-vs-dbf5405.diff` (switch e alone) and
  `dragon-0a7d87d-vs-v0.14.1.diff` (every switch).

## Question and answer

**How much of the 52 ms dependent step through AsyncFlow + RHAPSODY is Batch's 50 ms wait, and
what is left?**

- **About 46 ms of it.** With (e), E3's dependent step is 6.2–6.3 ms at the median and
  7.3–7.4 ms at p95, against 52.0–52.2 and 52.3–52.4 ms without it. The result is the same with no
  policy, Fair and Lineage.
- **What is left, about 6 ms,** is Dragon's own edge (3–4 ms with (b), diagnostic C) plus about
  2–3 ms in AsyncFlow and RHAPSODY.
- **Throughput and E1a barely move.**
  - E3 throughput for 2,000 trivial tasks: 794–858 tasks/s with (e), against 828–844 without,
    within the spread.
  - E1a chain p50: 1.86 → 1.75 s with no policy, 1.62 → 1.59 s with Fair.
  - Bulk makespan is 98–99 s either way.
  - Many tasks in flight already kept the client worker awake, and E1a's chain steps are 0.5 s.

**Is submitting ahead with Batch Task arguments still worth building once (e) is in?** Probably
not for now.
- With (e), a dependent step costs about 6 ms through AsyncFlow + RHAPSODY.
- Submitting ahead could bring it to Dragon's own edge, 3.05 ms in diagnostic C: 3 ms saved per
  step, which matters only for long chains of tasks of a few milliseconds.
- Switch (e), a small change inside Dragon, removes 46 of the 52 ms with no AsyncFlow change.

## Switch (e)

Dragon branch `experiment/batch-reports-priority`, new commit `0a7d87d` on top of `dbf5405`,
local only, in the worktree `~/github/dragonhpc/dragon-batch-reports-priority`. The rust build in
`/home/mturilli/tmp/sched-exp/dragon-exp` got the new `batch.py` (the build is an editable
install, and the change is Python only). The previous `batch.py` is kept beside it as
`dragon-exp-dbf5405-batch.py`.

- **The problem** (third campaign, finding 11): `_run_client_request_worker` drains the return
  queue `ret_q` once per loop, then blocks on `self._client_request_q.get(timeout=0.05)`. A
  completion that arrives while it waits reaches `cq`, and `poll()`, after the timeout or the next
  submission. That is `batch.py:6256/6263` in 0.14.1 and `:6326/:6333` in dbf5405.
- **The fix, signal on completion:**
  - `Batch(client_wake_on_completion=True)`, or `DRAGON_BATCH_CLIENT_WAKE=1`, starts a second
    client thread, `_run_client_completion_worker`. It blocks on `ret_q` and publishes each
    completed tuid to `cq`, or to the overflow buffer, as it arrives.
  - The request worker then no longer drains `ret_q`, and handles submissions only.
  - Off by default.
- **Why that approach, and why it is safe:**
  - `ret_q`, `_completion_overflow` and the writes to `cq` were touched only by the request
    worker. With (e) on, they are touched only by the completion thread: still one writer, and
    no lock needed.
  - `cq` is a thread-safe queue read by `poll()`. `_mark_task_log_complete` already takes its own
    lock.
  - Client registration reads `ret_q` before either thread starts.
  - Shutdown: the thread checks a stop event, `closed`, `destroyed` and `ret_q is None` each time
    round. Its 50 ms `get` timeout only bounds how long a stop takes to be noticed.
    `_stop_client_request_worker` sets the event and joins the thread.
  - An error on `ret_q`, such as a `ManagerException`, is stored where the request worker stored
    it, so `poll()` raises it as before.
  - The timeout fallback (`get(timeout=0.001)`) wasn't needed, so there is no near-spin and no
    client CPU cost to measure.
- **Dragon's own Batch tests, with the pool capped as before:** 7 classes, 28 tests, on julia.
  - First run on `cea1397`: one error, `test_poll_is_client_local_for_cloned_handles`, because
    the new `threading.Event` was pickled with cloned handles (24948, 24949).
  - Fix: strip it in `__getstate__`; `_set_unique_attrs()` recreates it on unpickle. That
    commit was amended to `0a7d87d`.
  - Then all 28 tests pass with (e) on (24952), with (e) and (b) (24953), and with neither
    (24954). Logs: `results/r4/diag/`.
- **Raw check** (`diag_launch.py --mode batch`, trivial functions, branch with (a)+(b), jobs 24950
  and 24951, on `cea1397`, which differs from `0a7d87d` only in that pickling fix):

| In flight | Report p50, (a)+(b) | Report p50, (a)+(b)+(e) | Tasks/s, (a)+(b) | Tasks/s, (a)+(b)+(e) |
|---|---|---|---|---|
| 1 | 46.7 ms | 0.9 ms | 19.7 | 227 |
| 8 | 48.2 ms | 1.2 ms | 150 | 1,444 |
| 32 | 45.1 ms | 1.5 ms | 644 | 1,579 |

## Setup

- **Node:** julia, `-p cpu -c 64 --mem=64G`. RHAPSODY's pool cap gives 32 Batch workers, and
  every policy runs at capacity 32, as in the second campaign.
- **Trees:** AsyncFlow `af-prio` (641ee65); RHAPSODY `rh-place-prio` (14faf2f). Dragon `0a7d87d`
  with (a) `client_linger_sec=0, client_max_batch_size=1` and (b) `report_each_task=True`, and
  with (e) on or off.
- **E3:** a chain of 100 trivial function tasks registered at once, each taking the previous
  result, so AsyncFlow submits each one when its parent completes. Then 2,000 independent trivial
  tasks. No policy, Fair and Lineage, in one job per variant.
- **E1a:** 1,000 bulk tasks of 1–5 s, plus 12 chains of 3 × 0.5 s, one every 6 s. No policy and
  Fair.
- **Matrix:** 3 repetitions. The 6 jobs of each repetition are shuffled with a fixed seed, so the
  two variants interleave (jobs 24955–24972).
- **Outcome:** all 18 jobs exited with Dragon rc 0. No other users' jobs ran on julia during them.
  julia was left with 0 Dragon processes and 0 /dev/shm files of ours.

## Results

Median [min–max] over 3 runs.

### E3: the dependent step and throughput

| Dragon | Policy | Per-task latency, chain of 100 | Step p50 | Step p95 | Steps pooled p50 / p95 / p99 | Throughput |
|---|---|---|---|---|---|---|
| (a)+(b) | none | 52.1 ms [52.1–52.2] | 52.1 ms | 52.3 ms | 52.1 / 52.3 / 52.7 ms | 844/s [820–873] |
| (a)+(b)+(e) | none | 6.3 ms [6.1–6.5] | 6.2 ms | 7.4 ms | 6.3 / 7.2 / 7.7 ms | 794/s [757–922] |
| (a)+(b) | Fair | 52.1 ms [52.1–52.1] | 52.1 ms | 52.3 ms | 52.1 / 52.3 / 52.4 ms | 842/s [814–877] |
| (a)+(b)+(e) | Fair | 6.6 ms [6.4–6.7] | 6.3 ms | 7.4 ms | 6.3 / 7.4 / 21.7 ms | 846/s [845–936] |
| (a)+(b) | Lineage | 52.2 ms [52.1–52.2] | 52.2 ms | 52.4 ms | 52.1 / 52.4 / 52.5 ms | 828/s [716–836] |
| (a)+(b)+(e) | Lineage | 6.4 ms [6.1–6.5] | 6.3 ms | 7.3 ms | 6.2 / 7.3 / 7.5 ms | 858/s [844–862] |

- **Without (e), every step is 50–54 ms**, a narrow peak: the 50 ms wait plus about 2 ms
  (chart `r4_e3_steps`). With (e), steps are 4–8 ms; the longest was 23 ms, under Fair.
- **Most of the remaining 6.3 ms is Dragon's own edge.** In diagnostic C, raw Batch with (b)
  gave 3.96 ms per edge for sequential submission, which is what AsyncFlow does here. That
  leaves about 2–3 ms for AsyncFlow's dependency release, the policy, RHAPSODY's submission and
  its monitor thread.
- **Throughput doesn't change.** With 2,000 tasks in flight, submissions keep the client worker
  awake, so the 50 ms wait rarely applied.

### E1a: chains beside the bulk

| Dragon | Policy | Chain p50 | Chain p95 | Bulk makespan | Bulk worker busy |
|---|---|---|---|---|---|
| (a)+(b) | none | 1.86 s [1.81–1.91] | 2.06 s | 98.4 s | 97% |
| (a)+(b)+(e) | none | 1.75 s [1.70–1.83] | 1.93 s | 98.3 s | 97% |
| (a)+(b) | Fair | 1.62 s [1.58–1.64] | 1.88 s | 99.1 s | 96% |
| (a)+(b)+(e) | Fair | 1.59 s [1.57–1.59] | 1.82 s | 98.4 s | 97% |

- A chain is 3 × 0.5 s of work, so 1.5 s is its floor.
- With (e), no policy saves about 0.1 s per chain. Fair, already within 0.12 s of the floor,
  saves 0.03 s.
- The bulk is unchanged. With 32 tasks of 1–5 s always running, completions arrive often and
  submissions follow them, so the client worker was rarely idle long.

Chart `r4_e3_e1a`: E3 throughput, E1a chain p50 and bulk makespan, for both variants.

## Caveats

- One node (julia) and one AsyncFlow + RHAPSODY configuration. The two-node runtime's throughput
  loss (third campaign) is a separate problem, which (e) is not expected to fix.
- (e) is measured on the branch together with (a)+(b), not on stock Dragon. On stock, the 10 ms
  per-edge floor found in diagnostic C would still apply.
- The raw check ran on `cea1397`, before the pickling fix. That fix only strips an attribute when
  a Batch handle is pickled, which the check doesn't do.

## What this means

- **The 52 ms dependent step was almost entirely Batch's client worker waiting.** A
  completion thread removes it, and Dragon's tests still pass.
- **This is the change to propose to the Dragon team, before anything in AsyncFlow.** With it, a
  dependent step costs about 6 ms end to end.
- **Submitting ahead in AsyncFlow would now save about 3 ms per step.** That is not worth the
  complexity unless workloads have long chains of millisecond tasks.

## Follow-ups

- **Propose (e) to Dragon,** with (b) and the per-task reports; the user decides how it reaches
  HPE. The dragon-report notes have the finding (`dragon-report/rust-experiments.md`, entry 11).
- **Profile the remaining 2–3 ms** in AsyncFlow and RHAPSODY: dependency release, policy dispatch,
  RHAPSODY's submit and its `poll()` loop (`timeout=0.05`, now woken at once).
- **Rerun the two-node E3 with (e),** to see whether any of that throughput loss came from the
  same wait.

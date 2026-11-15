# Mixed kinds at once: design of the `mixed-kinds` kit

2026-11-02. For the coverage table's row "Mixed kinds at once" (`analysis/coverage.json` in the
experiments repo). Today the row reads "not measured" for both backends: E1a mixed bulk and
chains of functions only.

**Question.** Can one AsyncFlow workflow run every kind of task at the same time, through each of
RHAPSODY's Dragon backends, with every task correct and no kind starving or breaking another?

**Code under test.** dragonhpc 0.15.0, AsyncFlow `437a10d`, RHAPSODY
`kits/mpi-launch/vendor/rhapsody-native-mpi-pmi-757465d/rhapsody_py-0.6.0.tar.gz` (sha256
`17efeffc6c18…`): Batch's job PMI (rhapsody#114) and the native backend's group PMI.

## A new kit, not presets

Every existing kit runs one kind per runtime or per step. `mpi-launch` runs one MPI case per
Dragon runtime, `gpu-place` runs its checks one after another, and `throughput` and
`stack-bench` run functions only. Mixing needs one harness that drives several streams of tasks at
once on one engine, so a preset of an existing kit can't express it. The new kit,
`kits/mixed-kinds`, reuses:
- `mpi-launch`'s environment (`KIT_ENV=mpi`, the same `KIT_PIP`);
- its `mpi_hello.c`, used from `kits/mpi-launch`, with its sha256 recorded in `build.txt`;
- its build steps and site settings (`MPI_CC`, `MPI_CFLAGS`, `MPI_LDFLAGS`, `MPI_PMI`,
  `MPI_LD_LIBRARY_PATH`);
- `gpu-place`'s libcuda UUID check;
- `dragonlib.sh`'s cleanup between runtimes.

## What one runtime does

There are two variants, `batch` (`DragonExecutionBackend`) and `native`
(`DragonNativeExecutionBackend`). Each runs in its own Dragon runtime over the allocation, and the
order alternates by repetition. Each runtime has one backend and one `WorkflowEngine` with no
policy, with W pool workers per node (default 8).

| Kind | AsyncFlow task | Description | Correct when |
|---|---|---|---|
| `pool` | function | none; busy loop of `--bulk-ms` (10 ms) | returns its record |
| `proc` | function | `process_template: {}`, its own process | its record file is there (Batch returns the exit code) |
| `exe` | executable | `/bin/sh -c` writing a record | exit 0 and the record |
| `mpi` | executable `mpi_hello` | `process_templates` split over 2 nodes, the site's `pmi` | every rank wrote; size and sum right |
| `mpi4py` | function | the same, using mpi4py | the same |
| `group` | function | `process_templates` 2 + 2, `pmi: "none"` | every process wrote |
| `gpu` | function | `process_template` with host and `gpu_affinity`, one per GPU | libcuda shows the GPU asked for |
| `long` | executable | `sleep` for `--long-s` (20 s), one per node | exit 0, lasted that long |

Every task except `pool` gets a `timeout` (120 s). Every record has the host, pid, start and
end, and allowed CPUs.

Phases, in one engine:
1. **alone:** each kind on its own. The bulk is a pump that keeps 2 tasks per worker in flight
   for 10 s. The other kinds each run as a stream of n tasks one after another (n = 5; 3 for
   `mpi`, `mpi4py` and `group`).
2. **mixed:** all streams, the pump and the `long` tasks at once. Each stream keeps going until it
   has n samples and the `long` tasks have ended, so MPI jobs overlap the long tasks.
3. **faults:** the mixed workload again, plus five faults, each a submission of its own, 1 s apart
   from 2 s in (Batch reports tasks launched together only when all have ended, so faults submitted
   at once would hide each other). Each fault but the dead rank first writes when it started. The
   streams keep going until every fault has resolved:
   - an MPI job whose rank 1 exits after `MPI_Init`, with a 30 s timeout;
   - a function that raises;
   - a function that kills its worker with `os._exit(9)`, with a 30 s timeout;
   - a function sleeping 60 s, with a 3 s timeout;
   - an executable (`/bin/sh`, whose child sleeps 60 s), with a 3 s timeout.
4. **after:** one task of each kind, and 5 s of pump. Is the backend still whole?

Every phase has a deadline (180 s). A stream not done by then is reported as starved. Every task
also has a guard of its timeout plus 60 s. If no task ends for 90 s, the phase is stopped as
stalled, and the tasks still in flight are listed as lost.

## Measured

- **Correct:** per phase and kind, how many tasks were correct, plus the first wrong records.
- **Latency:** per kind, submission to result, p50 and max: mixed and faults against alone.
- **Bulk throughput:** tasks per second alone, mixed, mixed while an MPI job runs, faults, after.
- **Placement:** per kind, tasks per host; per MPI job, ranks per host; per host, the CPUs each
  kind was allowed (do MPI ranks share CPUs with pool workers?).
- **Faults:** per fault, the outcome against the expectation (an error naming the fault, within
  its timeout plus slack), whether the bystanders in that phase were all correct, and the bulk
  rate before and after (does the native pool replace the killed worker? does Batch?).

## Where and how big

- **three:** 1 node, W = 8, the `mpi-launch` settings for Open MPI 5. No GPUs, so `gpu` is
  skipped.
- **rust:** haskell and julia, 16 CPUs each (32 in all), W = 8, EESSI 2023.06 Open MPI 4.1.6 with
  PMIx. Run from a job script of its own, built on `mpi-check/mpi2.sbatch`:
  - `-J d2 -d singleton`, since the kit runner's rust profile asks for 64 CPUs per node;
  - one Dragon runtime per variant, with the same cleanup;
  - CPU in partition `cpu`, then the `gpu` kind in partition `gpu` with `--gres=gpu:1`.

  The kit places GPU tasks only on GPUs Dragon lists, and says so when it lists none; on rust
  that needs the mpi-gpu session's NVIDIA library folder on `LD_LIBRARY_PATH`.
- **Perlmutter (CPU):** `./kit go perlmutter mixed-kinds`, 2 nodes, the same W and sizes as rust
  for comparison, one job of at most 30 minutes (2 repetitions).

On rust a runtime took about 2.3 minutes with native and 4 with Batch.

## Results (2026-11-02)

The final kit ran on:
- **rust:** job 26152, CPU, 2 repetitions, `~/tmp/mixed-kinds/runs/26152`. GPU jobs 26149 (native)
  and 26151 (Batch), 1 repetition each; in each, the other backend's runtime failed to start (below).
- **three:** run `20261102T083821Z-three-mixed-kinds-62994e1` in `~/tmp/radical-runs`, 2
  repetitions, through `./kit run`.

Earlier versions of the harness ran too: rust 26147 and three `20261102T080344Z`. The only change
since is the fault start marks, plus `exe-timeout` now runs `sleep` as a child of `/bin/sh`. Probes
are in `three:~/tmp/mixed-kinds-proto`.

**Native: yes.** Every task of every kind was correct in every phase, on both sites:
- **Faults phase:** 66,880 of 66,880 bystanders correct on rust, 57,395 of 57,395 on three.
- **GPU:** 5/5 alone, 2,707/2,707 mixed and 2,155/2,155 under faults, each on the GPU it asked for
  (rust 26149).
- **Latency:** mixed, each kind took 1.1–1.8× its time alone on rust (32 CPUs, with 16 busy pool
  workers and the ranks on the same CPUs), and at most 1.2× on three (64 CPUs).
- **Bulk:** 1,379/s alone, 1,033/s mixed, 813/s with faults, 1,394/s after.
- **Faults:** all five failed only their own task, in time, in every run:
  - dead rank: TimeoutError at 32–34 s, exit codes `[-15, 7, -15, …]`;
  - raise: 0.01–0.04 s;
  - killed worker: 0.2–1.0 s, "Dragon worker N exited with code 9";
  - function past its timeout: 3.0–3.1 s, "its worker was killed";
  - executable past its timeout: 3.0–3.5 s.

**Batch: every kind runs at once, but its reports couple the kinds, and a fault can take other
tasks down.**
- **Correctness:** on rust, 5,999 of 6,000 bystanders were correct. The other was a 20 s executable
  that ran its full time and was reported failed with "ProcessGroup manager is not in state
  State.DEAD". GPU tasks were 5/5, 5/5 and 20/20 on the right GPU (rust 26151).
- **Coupling:** tasks launched in the same round are reported only when the slowest of them ends.
  - Bulk on rust: 149/s alone, 35/s mixed, 29/s with faults, 143/s after.
  - Bulk on three: 180/s alone, 8.6/s mixed.
  - Each kind took 1.1–1.7× its time alone on rust, and up to 2.9× on three.
- **Function that kills its worker:** never reported, not even at its 30 s timeout, in every run.
- **Function past its 3 s timeout:** reported only when it returned, at 60 s.
- **Executable past its 3 s timeout:**
  - when it has a child (`sh` → `sleep 60`): reported at 61 s as "RuntimeError: failed with
    unknown error";
  - a bare `sleep 60` (26147): ended at 3.6 s, with the same error.
- **Dead rank:** reported at 65 s with Open MPI's errors on rust, against 33 s in 26147.
- **On one node with all workers busy (three):** the five faults started together 15–18 s after
  submission, when the 20 s task freed workers. None was ever reported, and 3 bulk tasks and 1 MPI
  job went with them in each repetition.

**Why Batch behaves so.** Dragon 0.15.0's Batch manager starts the tasks that are waiting at a
moment as one `pool.map_async` round (`batch.py`, `_launch_tasks`). Its callback fires only when
the whole round is done. This is the 0.14.1 behaviour found on rust in October, still there.
- One task that never ends (its worker was killed) leaves its whole round unreported.
- Rounds are large when workers are scarce (three) or when tasks are submitted within the client's
  5 ms linger.
- Probes on three, with `fn-exits` submitted together with one other kind:
  - every task submitted with it was lost, whatever the kind;
  - when an MPI job was among them, the next MPI job never ended either;
  - with every kind among them (the first test run), Batch never reported another task.

  The runtime still shut down (rc 0), but Batch's stop took 17–22 s, against 0.2 s for native.

**Placement.**
- **MPI jobs:** went 4 + 4 or 2 + 2 by host, as asked, on both backends.
- **Unplaced tasks:** spread over both nodes: native pool 42/58 %, Batch pool 48/52 %; Batch put 8
  of 10 executables on julia.
- **CPU pinning:** native pins each pool worker to CPUs in list order. On rust's slice (CPUs 0–7 and
  their SMT siblings 256–263), workers get 0–1 … 256–257, so two workers share the same two
  physical cores. Process tasks and job ranks run on all 16 CPUs on both backends, as do Batch's
  pool workers.

**rust, other findings.**
- **GPU list:** Dragon 0.15 lists no GPUs there without the mpi-gpu session's NVIDIA library folder
  (job 26148). With it, it lists GPU 0 on each node.
- **First runtime failures:** in all three GPU-partition jobs, the first Dragon runtime failed to
  start: "Could not attach to FLI adapter", `DRAGON_CHANNEL_NO_GATEWAYS` or `DRAGON_MEMORY_ERRNO`,
  then a hang to the 480 s limit or an abort at 20 s. The second runtime in the same job always
  started. CPU jobs never did this. Not investigated.

## Out of scope

- MPI with GPUs (the `mpi-gpu` session);
- services, per-task resource usage, and AsyncFlow's policies;
- scale: two nodes only.

# One Dragon runtime over two nodes: third rust campaign

Status: experiments, 2026-10-25, 05:55–11:23 CET. Everything is local: one new local RHAPSODY
branch, nothing pushed, nothing posted to GitHub. The deck's `.tex` is unchanged.

The first two campaigns ([rust-experiments.md](rust-experiments.md),
[rust-experiments-2.md](rust-experiments-2.md)) ran Dragon on one node. This one runs one Dragon
runtime over two:
- **Part A:** E2 on two GPU nodes, one GPU each, of different memory.
- **Part B:** E1a, E1b and E3 on two CPU nodes, with the one-node results as reference.

Files:
- Scripts: `design/presentations/scheduling-policies/experiments/`. New: `run2n.sbatch`,
  `srun-hetjob`, `node_sampler.sh`, `submit_campaign3.sh`, `analyze3.py`, `smoke_twonode.py`,
  `hetsmoke.sbatch`, `hetprobe.sbatch`, `cudaprobe.sbatch`, `diag_twonode.py`. Earlier scripts
  gained options and records that are off or unused on one node, so the first two campaigns
  reproduce unchanged.
- Raw results, one JSON file per run and configuration: `results/r3/raw/` (60 results, one
  `noresult` file, `corunners.json`).
- Tables (generated): `results/r3/summary.md`, `results/r3/summary.json`.
- Charts, PDF and PNG: `results/r3/r3_*`.
- Logs of the smoke tests, probes, the failed run and the diagnostics: `results/r3/diag/`.
- The new RHAPSODY branch's diff: `results/r3/diffs/rhapsody-14faf2f-vs-738288e.diff`.

## Question and answer

**Part A. When the GPU memory totals fit, does Dragon's node choice overload one GPU? Does node
mode avoid it, and at what throughput and GPU busy?**

- **Yes, Dragon's node choice overloads the smaller GPU.**
  - The setup: 24 pipelines on haskell's RTX 6000 Ada (47.4 GiB) and fortran's RTX PRO 6000
    Blackwell (95.0 GiB).
  - With aggregate admission, at most 134 GiB is declared against 141.5 GiB of GPU memory, so
    the totals never refuse a task.
  - Batch still sends half the tasks to each node, and haskell's GPU fills.
  - The result: 13–17 CUDA OOMs per 12-minute run, with aggregate admission or without
    admission, on stock Dragon and on the branch. All but 2 of the 180 OOMs were on haskell.
  - Each OOM ends a pipeline, so more than half the pipelines are lost.
- **Node mode avoids it, with 0 OOMs in 6 runs, and does more work.** It places each task on a
  node and GPU where its declared memory fits.
  - It completed 12–15% more tasks: 370 against 322–326 on the branch, 357 against 311–320 on
    stock.
  - The GPUs were busier: 93% and 97% on the branch, against 75–86% without node mode.
  - Every task ran on the node and GPU the policy chose, 2,302 of 2,302.
- **Running it took a wrapper.** A Slurm heterogeneous job was needed, because haskell and fortran
  share no partition. Dragon spans its components only through `srun-hetjob`, which rewrites
  Dragon's `srun`. fortran also needed its CUDA driver loaded by path.

**C. Dragon's own dependency edge** (raw Batch chain of 100 on julia): 10.1–10.2 ms on stock
0.14.1, in every variant; 3.0–4.0 ms with per-task reports (b). Submitting the chain ahead in
one compile is the fastest, at 3.05 ms. The 52 ms step through AsyncFlow + RHAPSODY is mostly
outside Dragon's edge, so submitting ahead is worth building.

**D. One Dragon process per task (V1-style)** costs about 130 ms to start, 170–200 ms at 32 in
flight; global services start about 160 processes/s. Batch functions start in 4–15 ms and run
trivial tasks 4.5× faster, but for executables V1 beats Batch's `process()` (161 against
59 tasks/s). Batch's `poll()` reports a lone task about 47 ms after it ends: its client worker
waits up to 50 ms before draining completions.

**Part B. With 32 workers over haskell and julia instead of 32 on one node:**

- **Per-task reporting holds through the scheduler manager and two subnode managers.**
  - E1a chain p50 on the branch (a)+(b), two nodes against one:
    - no policy: 2.07 s against 1.86 s;
    - Fair: 2.17 s against 1.59 s;
    - stock with the patch: 16.8 s with no policy, 4.65 s with Fair.
  - Bulk workers were 88–94% busy on the branch.
- **Capacity = total pool (32) reaches no policy's throughput, but two nodes cut throughput itself
  by about 60%.**
  - On the branch, the policies at 32 match no policy: 333–354 tasks/s, against 311.
  - On one node the same configurations gave 836–853/s.
  - Raw Batch inside the two-node runtime does 343–467/s even when it uses one node. So the
    loss comes with Dragon running over two nodes, not with spreading the tasks; this campaign
    did not find its cause.
- **Batch spreads tasks evenly at random.** 48–52% of the trivial and bulk tasks ran on haskell.
  About half the steps of a dependent chain changed node.
- **A dependent task costs the same across nodes:** a median step of 52.5–54.1 ms, whether the
  task runs on its parent's node or the other. Over a whole chain of 100, the per-task latency
  rises from 52 ms on one node to 53–62 ms. E1b's
  Lineage keeps its gain on two nodes: 19.7–19.8 s mean diamond completion, against 26.3–26.8 s
  with no policy (18.4 s on one node).

## Running one Dragon runtime over two nodes on rust

### E2: a heterogeneous job over haskell and fortran

haskell is in the partitions `cpu`, `gpu` and `interactive`; fortran only in `group2-fortran`
and the hidden `staging` (MaxTime 1 day, OverSubscribe FORCE:1, PreemptMode=REQUEUE). They share no partition, so E2 runs as a Slurm heterogeneous job:
component 0 `-p gpu -w haskell --gres=gpu:1 -c 28 --mem=64G`, component 1 `-p staging -w fortran
--gres=gpu:1 -c 28 --mem=64G`.

What we found, in order (logs in `results/r3/diag/`):

1. **`srun` inside the heterogeneous job hangs unless it has `--overlap` and an explicit `-n`.**
   The step shows RUNNING, but its task never starts. `srun` then ignores SIGTERM, so `timeout`
   needs `-k`, and `scancel <job>.<step>` answers "Invalid job id". Jobs 24780, 24782 and 24786;
   probe 24784. With `--overlap -n1`, steps run in either component, or in both with
   `--het-group=0-1`.
2. **Dragon as `run.sbatch` starts it sees only component 0.** In a heterogeneous job
   `SLURM_JOB_NUM_NODES=1`. Dragon's Slurm launcher reads it and starts its network-config helper
   and backends with one `srun --nodes=1 --ntasks=1`. Job 24788, attempt 3: one node, haskell,
   14 workers.
3. **With a wrapper, one Dragon runtime spans both components.** `experiments/srun-hetjob` sits
   first on PATH for the `dragon` command only. It rewrites each of Dragon's launches
   (`srun --nodes=N --ntasks=N [--nodelist=...] cmd`) into
   `srun --overlap --het-group=0-1 --nodes=1 --ntasks=1 cmd`, one task per component.
   - Every other `srun` passes through unchanged.
   - The wrapper starts nothing over SSH or outside the allocation. Dragon itself tried SSH
     once: its abnormal-exit cleanup in the failed run 24815, after another job's
     dragon-cleanup removed its pools; the SSH attempt failed (below).
   - Dragon then lists both nodes, fortran first, and Batch starts a pool of 14 workers on each.
   - In 24788 and 24792, function tasks ran on both nodes (27/37 and 34/30), and so did
     unplaced process tasks.
   - A process placed with `Policy(HOST_NAME)` ran on the host it named, and saw that node's
     GPU as device 0.
   - Dragon's launcher prints "Network Config Error Detected" followed by the wrapper's own
     stderr line. That is the wrapper's message, read from the helper's stderr; the network
     configuration completes with both nodes.
4. **fortran needs the CUDA driver loaded from its path.**
   - The EESSI compat-layer Python finds `libcuda.so.1` by name on haskell, through EESSI's
     host_injections, but not on fortran (24790).
   - Loaded by path (`/usr/lib/x86_64-linux-gnu/libcuda.so.1`), the driver opens a context, but
     loading the PTX kernel fails with `CUDA_ERROR_JIT_COMPILER_NOT_FOUND` (24792).
   - libcuda opens its JIT compiler by name. Preloading `libnvidia-ptxjitcompiler.so.1` from the
     same directory with `RTLD_GLOBAL` fixes it.
   - `gpu_task.py` now does both when the name lookup fails, so haskell is unchanged.
5. **A short end-to-end run worked** (24797, 90 s): RHAPSODY's placement put each of 31 tasks on
   the host and GPU that node mode chose. Every bus id matched its node's GPU.

Two Dragon facts matter for the results:

- **`Batch.num_workers` is one node's pool** (`workers_per_node_in_pool`). Batch starts one pool
  per node. `common.make_backend` now returns the total over Batch's nodes. One-node runs are
  unchanged.
- **Batch chooses a task's node at random when the task is compiled on its own.** `compile()`
  draws `subnode_offset = random.randrange(num_subnode_managers)` per compiled batch.
  `hefty_partition()` balances load only within that batch. With one task per compile, as with
  (a) or the no-change patch, every task goes to a random node, whatever is running there. A
  process task runs on its manager's node unless it is placed.

### Part B: a plain two-node job over haskell and julia

haskell and julia share partition `cpu`, so Part B runs as an ordinary two-node job
(`-N 2 --ntasks-per-node=1 -c 32 --mem=64G`). Dragon's own `srun` asks for both nodes there.
The wrapper only adds `--overlap`, so Dragon's step shares the CPUs with the samplers.
RHAPSODY's cap sized each pool from the 32 allowed CPUs: 16 workers per node, 32 in all
(`launch_budget_per_node` 16), as on one node in the second campaign (julia, 64 CPUs, 32 workers).

## Software

- **AsyncFlow:**
  - `feature/resource-policy` `6285975` (tree af-resource) for E2;
  - `experiment/policy-priority` `641ee65` (af-prio) for E1 and E3. No policy here sets
    priorities.
- **RHAPSODY:** a new local branch, `experiment/placement-priority` `14faf2f`, in the worktree
  `~/github/radical/rhapsody-placement-priority`.
  - It is `experiment/dragon-priority` (`738288e`) with `feature/dragon-placement` (`762c5b3`)
    merged in.
  - The conflicts were the import block and CHANGELOG; both sides were kept.
  - Unit tests: 273 passed and 14 skipped; ruff is clean.
  - Not pushed. Diff: `results/r3/diffs/`.
- **Dragon:**
  - the stock dragonhpc 0.14.1 wheel; with RHAPSODY's `compile_each_task`, that is the
    no-change patch, labelled "stock + patch";
  - or the branch `experiment/batch-reports-priority` `dbf5405`, built on rust, with (a)+(b):
    `client_linger_sec=0`, `client_max_batch_size=1`, `report_each_task=True`.
- **Python** 3.11.4 (EESSI compat layer), with tcp transport.

## Setup

### E2 (Part A)

- **Jobs:** a heterogeneous job with 28 CPUs, 1 GPU and 64 GiB on each node, so 14 Batch workers
  per node and 28 in all. Each job had a 24-minute limit and held fortran for 13 minutes.
- **Workload:** as in the first campaign, 24 IMPRESS-like pipelines (rfd3 → mpnn → boltz/rfd3)
  for 720 s, each task a synthetic CUDA process (`gpu_task.py`), with these changes:
  - **GPU memory ×1.5**, true and declared:
    - rfd3 declares 8.38 GiB and uses about 7 GiB (CUDA context included);
    - boltz declares 8.38 GiB;
    - mpnn declares 2.79 GiB.
    - Why: a task asks for 1/8 of a GPU, so aggregate admission runs at most 16 tasks, at most
      134 GiB declared against 141.5 GiB in all. Half of them, all rfd3, would need about
      57 GiB on haskell.
    - So the totals don't refuse tasks, but haskell's memory binds. Without the ×1.5, haskell's
      memory would not bind before the 8-task share.
  - **The kernel rate per GPU model:** 79,000 iterations/ms on the Ada, as in the first campaign,
    and 69,000 on the Blackwell, which measured 0.87× the Ada's rate (`diag/fortran_calibrate.log`).
    So a task computes for the same time on either GPU.
  - On the Blackwell a CUDA context takes 0.55 GiB, against 0.42 GiB on the Ada. Tasks allocate
    as if 0.42, so on fortran each uses 0.13 GiB more than modelled.
- **Modes:**
  - none: Dragon chooses the node; each node's one GPU is device 0;
  - aggregate: ResourcePolicy with both NodeShapes summed, and Dragon chooses the node;
  - node: one NodeShape per node, 14 CPUs and 1 GPU each, with haskell=47.0 and
    fortran=94.5 GiB, each under its driver's total. The policy writes `task["placement"]` and
    RHAPSODY runs the task there with `Policy(HOST_NAME, gpu_affinity)`.
- **Matrix:** 3 modes × 3 repetitions on the branch, then on stock, with the modes shuffled per
  repetition. Each task records its host, GPU bus id, start and end. A sampler on each node
  records nvidia-smi once a second, and the load every 5 s.

### E1a, E1b, E3 (Part B)

The second campaign's workloads, unchanged:
- **E1a:** 1,000 bulk tasks of 1–5 s, plus 12 interactive chains of 3 × 0.5 s, one every 6 s.
- **E1b:** 256 diamonds of 1 s tasks.
- **E3:** a chain of 100 trivial tasks, then 2,000 independent ones.

The matrix:
- configurations: stock + patch and branch (a)+(b), each with no policy and with Fair (E1a),
  Lineage (E1b), or both (E3), at capacity 32, the total pool;
- 3 repetitions, shuffled, rep 1 with twice the time limit;
- each task returns its host.

### Courtesy and cleanup

- **One job stream:** everything ran in one singleton stream (`-J d2 -d singleton`), so two Dragon
  jobs of ours never shared a node.
- **Queue checks:** the queue was checked before every wave of GPU jobs.
  - The only pending job of another user, userB's 24387, asked for julia's GPU.
  - userB's 24889 (2 GPUs on haskell) was submitted at 10:24, after my last GPU submission at
    08:37. It waited about 13 minutes for my last E2 job (24852) to release haskell's GPU, and
    started at 10:37.
- **fortran:** used from 06:01 to 10:37 CET, every job under 14 minutes.
- **Cleanup:** each job deleted only its own /dev/shm files on each node, and reported 0 files and
  0 Dragon processes left.
  - Two smoke jobs left files: the cancelled 24794 (36 files) and 24788 (whose cleanup hung).
  - I deleted those by time window, from inside allocations (`diag/shm_cleanup_24794.log`).
  - No dragon-cleanup, no SSH, no configuration change.
- **Co-runners:** other users' load was light.
  - haskell: 3.4 CPUs busy on average (35 at most) during Part B, 0.8 during E2. The jobs there
    were userB's 24889 (16 CPUs, 2 GPUs) during all of Part B, and small interactive and CPU
    jobs of userC and userD.
  - julia: 0.4 CPUs on average, with userB's 24919 (16 CPUs, 1 GPU) from 10:41 to 10:56.
    userA's 24370 ended at 10:20, before Part B.
  - fortran: idle.
  - Per run: `summary.md`.

## Results

### E2: two GPUs of different memory

Each cell: median [min–max] over 3 runs. One Dragon run failed (24815, below); its rerun 24838
is among the three.

| Dragon | Admission | CUDA OOMs (haskell + fortran) | Pipelines lost of 24 | Tasks done in 12 min | GPU busy, haskell · fortran |
|---|---|---|---|---|---|
| branch (a)+(b) | none | 14 [14–15] (all on haskell but 2 of 43) | 14 [14–15] | 326 [317–332] | 85% · 81% |
| branch (a)+(b) | aggregate | 16 [13–16] (all on haskell) | 16 [13–16] | 322 [312–330] | 76% · 82% |
| branch (a)+(b) | node | **0** in 3 runs | 0 | **370** [351–377] | **93% · 97%** |
| stock 0.14.1 | none | 16 [15–17] (all on haskell) | 16 [15–17] | 311 [299–316] | 84% · 75% |
| stock 0.14.1 | aggregate | 15 [14–15] (all on haskell) | 15 [14–15] | 320 [304–340] | 81% · 86% |
| stock 0.14.1 | node | **0** in 3 runs | 0 | **357** [345–361] | **89% · 92%** |

- **When the totals fit, Dragon's node choice overloads the smaller GPU.**
  - Aggregate admission held 16 tasks at its peak in every run: its GPU-share limit, 2 GPUs × 8.
    Their declared memory is at most 134 GiB, under the 141.5 GiB of the two GPUs, so the totals
    never refused a task.
  - Batch sent about half the tasks to each node: 46–56% on haskell, in every mode.
  - Up to 13 tasks ran on haskell at once under aggregate admission, 12 with none. haskell's
    GPU filled to its 47 GiB (nvidia-smi peak 45.9–47.4 GiB in every run) and tasks failed
    there.
  - fortran's GPU never went above 73 GiB under aggregate admission. With none it reached
    94.2 GiB, and 2 of the branch's 43 OOMs were there.
  - The OOMs come throughout the 12 minutes, not only when the pipelines start.
    - With none: 5–6 in the first 10 s, then the rest.
    - With aggregate admission: 4–9 in the first 10 s on the branch, but 1–2 on stock.
    - Stock Dragon groups the 16 opening tasks into one compile, and Batch spreads a compile
      evenly. The branch compiles each task on its own (a), and so on a random node.
  - Each OOM ends its pipeline, so by the end 13–17 of 24 pipelines are gone and the GPUs idle.
- **Node mode avoids it: 0 OOMs in 6 runs, and more work done.**
  - The policy put each task on a node and a GPU that fit its declared memory, and each task ran
    there: all 2,302 node-mode task records name their node's GPU. Over all 18 runs, 6,406 of
    6,406 records with a bus id do.
  - fortran ran close to its 8-task GPU share: 7.9 tasks on average on the branch, 7.4 on stock.
    haskell ran 6.7 (6.3 on stock): its memory, not its share, was the limit.
  - Node mode got 12–15% more tasks done than either other mode on the same Dragon.
    - Branch: 370 tasks, against 326 with none and 322 with aggregate admission.
    - Stock: 357, against 311 and 320.
    - The GPUs were busier: 93% and 97% on the branch, 89% and 92% on stock, against 75–86%.
- **Per-task reports (b) add a little**: 370 against 357 tasks in node mode, and 4–5 points of
  GPU busy. E2's tasks run for tens of seconds, so a launch round's wait matters less than in E1.

Charts:
- `r3_e2_per_node`: OOMs, tasks done and GPU busy per node.
- `r3_e2_gpu_memory_branch` and `r3_e2_gpu_memory_stock`: each GPU's memory over run 1.

### E3, E1a, E1b: 32 workers on two nodes

The one-node values are the second campaign's runs of the same configurations on julia. They
ran a few hours earlier the same day, with the same trees except RHAPSODY's placement code,
which these policies don't use. Median [min–max] over 3 runs.

| Experiment | Dragon | Policy at 32 | One node | Two nodes |
|---|---|---|---|---|
| E3 throughput, 2,000 trivial tasks | branch (a)+(b) | none | 836/s | 311/s [310–317] |
| | | Fair | 853/s | 354/s [340–355] |
| | | Lineage | 837/s | 333/s [329–335] |
| | stock + patch | none | 920/s | 347/s [345–354] |
| | | Fair | 425/s | 293/s [292–300] |
| | | Lineage | 397/s | 289/s [288–291] |
| E3 per-task latency, chain of 100 | all six | | 52.0–52.2 ms | 53.3–61.9 ms |
| E1a chain p50 | branch (a)+(b) | none | 1.86 s | 2.07 s [2.02–2.17] |
| | | Fair | 1.59 s | 2.17 s [2.16–2.40] |
| | stock + patch | none | 13.11 s | 16.83 s [15.95–22.28] |
| | | Fair | 1.65 s | 4.65 s [3.04–4.78] |
| E1a bulk makespan (workers busy) | branch (a)+(b) | none | 98.4 s (97%) | 100.8 s (94%) |
| | | Fair | 99.1 s (96%) | 107.3 s (88%) |
| | stock + patch | none | 149.0 s (63%) | 156.1 s (60%) |
| | | Fair | 107.1 s (89%) | 113.7 s (84%) |
| E1b mean diamond completion | branch (a)+(b) | none | 26.4 s | 26.3 s |
| | | Lineage | 18.4 s | 19.7 s [19.4–20.5] |
| | stock + patch | none | 26.7 s | 26.8 s |
| | | Lineage | 18.4 s | 19.8 s [19.7–19.8] |

- **Per-task reporting holds on two nodes.**
  - With (a)+(b), a chain of three 0.5 s steps beside the bulk takes 2.07 s with no policy and
    2.17 s with Fair. Workers stay 88–94% busy.
  - Batch's scheduler manager runs on julia, the first Batch node, and routes each task to a
    subnode manager on either node. Reports still arrive per task.
  - Stock with the no-change patch reports in rounds, on two nodes as on one, and its chains wait
    4.65 s with Fair and 16.8 s without.
- **Fair at 32 costs more on two nodes than on one:** chain p50 2.17 against 1.59 s on the branch,
  4.65 against 1.65 s on stock. The bulk takes 6–8% longer, and the workers are 5–8 points less
  busy.
  - Each refill now goes through the slower two-node path that E3 shows. With 32 slots and tasks
    of 1–5 s, the refills are many.
  - With no policy, Batch gets 1,000 tasks at once, and the bulk is only 2–5% slower than on
    one node.
- **Capacity 32 still reaches full throughput, relative to no policy.**
  - On the branch, Fair and Lineage at 32 do 333–354 tasks/s against 311/s without a policy.
  - On stock with the patch they do 289–293/s against 347/s, a 16–17% gap. On one node that gap
    was 54–57% (397–425 against 920/s); campaign 2 traced it to round-at-a-time reporting.
- **But two nodes cut E3's throughput by 58–63%** on the branch and with no policy on stock:
  311–354/s against 836–920/s. Stock with a policy, already slow on one node, loses 27–31%.
  - The diagnostic `diag_twonode.py` (jobs 24916, 24917; `diag/diag_twonode_*`) ran raw Batch,
    without AsyncFlow or RHAPSODY, inside the same two-node runtime. It measured 2,000 trivial
    tasks with Batch on one node (julia, 16 workers) and on both (32 workers), 3 times each:
    - stock: 343–375/s on one node, 347–384/s on both;
    - branch (a)+(b): 403–467/s on one node, 279–286/s on both.
  - So most of the loss comes with the Dragon runtime spanning two nodes, even when Batch uses
    one. Spreading the tasks over both costs the branch about another third, and stock nothing.
  - This campaign did not find the cause. The runtime's global services, the client and the
    scheduler manager are on julia. Results go to a DDict with a manager on each node. Messages
    between the nodes use Dragon's TCP agent; there is no HSTA on rust.
  - One limit of the comparison: raw Batch was not measured in a one-node runtime here.
- **Batch spreads tasks evenly at random.** Over a run, 48–52% of the E3 tasks and of E1a's bulk
  ran on haskell. In a chain of 100, 40–55 steps changed node.
- **A dependent task costs the same across nodes.** E3's median step was 52.4–54.1 ms, the same
  when the task ran on its parent's node or on the other (chart `r3_e3_chain_steps`).
  - Raw Batch's lone dependent step: 7.9–14.2 ms on either layout, so most of the 52 ms is
    AsyncFlow and RHAPSODY's path, as on one node.
  - Lineage's depth-first order survives two nodes: 19.7–19.8 s against 26.3–26.8 s with no
    policy, a 25–26% gain against 30–31% on one node.

Chart: `r3_one_vs_two_nodes` (E3 throughput, E1a chain p50 and E1b completion, one node and two).

### A failed run: another job's dragon-cleanup

One of the 19 E2 jobs failed: 24815, the branch with no admission, repetition 3.
- **What happened:** about 60 s in, Dragon's frontend cancelled its own backend steps on both
  nodes (`CANCELLED by` my uid). Its abnormal-exit handler then tried a cleanup over SSH
  (paramiko), which failed with "No authentication methods available". The launcher was killed
  (rc 137).
- **The cause, found afterwards:** another session's job, 24833 (dragon-on-rust, 4 CPUs on
  haskell), ran `dragon-cleanup` on haskell at 07:54:48. It deleted every /dev/shm file of
  this user there, including the live runtime's pools.
  - Slurm accounting puts 24833 on haskell from 07:54:46 to 07:55:00.
  - 24815's backend steps were cancelled at 07:54:53.
  - This is the hazard found in the first campaign (jobs 24662–24664): our scripts don't run
    `dragon-cleanup`, but a job of another stream on the same node did. That session has
    stopped using it.
- **The 9 CUDA OOMs** on haskell at that run's start were normal for that mode.
- **Records:** `raw/24815_noresult.json`, `diag/e2_failed_24815*`. The same configuration was
  rerun as 24838.
- The other 50 jobs of the matrix and the diagnostics, 24803–24917, exited with Dragon rc 0.

## A2. E2 on all four GPUs: not run

Asked by the main session: E2 on haskell's 2 Ada and fortran's 2 Blackwell under
`group2-priority-qos`, only while no other user's job waits for or runs on those GPUs.
- Checked at 11:26 and again at 12:01 CET, in fresh logins.
- `id` did not list the group `group2`, which the partitions `group2-priority`
  and `group2-fortran` require (AllowGroups).
- userB's job 24895 held one of fortran's GPUs.
- Not submitted, as the brief says. `e2_gpu.py` already builds each node's NodeShape from the
  GPUs Dragon reports there. The workload's memory scale would need resizing for 4 GPUs.

## C. Dragon's own dependency edge: a chain of 100 in raw Batch

Asked by the main session: should AsyncFlow submit a dependent task ahead, while its upstream
still runs, passing the upstream's Batch Task as an argument so that Dragon resolves the edge?
Through AsyncFlow + RHAPSODY a dependent step costs 52–57 ms (E3).

- **Setup:**
  - raw Batch on julia (`-c 64`, 32 workers), with the pool capped to the job's CPUs (`experiments/diag_chain.py`);
  - each task takes the previous result and returns its value + 1, timing its body with `time.time()`;
  - three variants, each with a fresh Batch:
    - **sequential:** the next task is submitted once the client has the result;
    - **ahead, one compile:** all 100 submitted in a tight loop, each taking the previous Task as its argument. The loop takes 3–6 ms and its largest gap is 1.5 ms, under Batch's 5 ms linger, so the chain is one compile;
    - **ahead, one compile per task:** the same, with `__defaults__ = (0.0, 1)` on stock and `client_max_batch_size=1` on the branch;
  - stock 0.14.1, and the branch with (b);
  - with no payload, and with a 1 MiB bytes payload along the chain;
  - 5 chains per variant per job, 3 jobs per Dragon, interleaved (24930–24935).
- **Results:** `results/r3/chain/` (raw, `summary.md`); chart `r3_chain_edge`. Every chain ended
  at 99, as expected: the first task returns 0.

Gap from one task's end to the next one's start (median [min–max] of the chains' p50):

| Dragon | Sequential | Ahead, one compile | Ahead, one compile per task | + 1 MiB payload (same three) |
|---|---|---|---|---|
| stock 0.14.1 | 10.15 ms [10.08–10.29] | 10.18 ms [10.09–10.26] | 10.16 ms [10.11–10.19] | 10.15 · 10.15 · 10.16 ms |
| branch (b) | 3.96 ms [3.65–4.51] | 3.05 ms [2.81–3.22] | 3.52 ms [3.08–4.13] | 5.87 · 4.41 · 4.80 ms |

- **Dragon's own edge costs 3–4 ms with per-task reports, and 10 ms on stock,** where every
  variant and payload lands on 10.1–10.2 ms. That is a 10 ms floor on stock; I did not locate
  the timer. Without a payload, a chain of 100 takes 1.02–1.06 s on stock and 0.32–0.41 s on the branch (warm chains 3–5).
- **Submitting ahead, one compile, is the fastest:** 3.05 ms against 3.96 ms sequential on the
  branch.
  - With one compile per task the edge crosses compiles at 3.52 ms.
  - Its first task starts later: 50–57 ms after submission, against 11–14 ms in one compile and
    3–9 ms sequentially.
- **A 1 MiB result adds 1.3–1.9 ms per edge on the branch.** On stock the 10 ms floor hides it.
- **Tails:** the first one or two chains of each Batch have steps of 120–180 ms (p95 up to 135 ms).
  Chains 3–5 have p95 3.7–6.6 ms on the branch and 10.8–11.0 ms on stock.
- **For AsyncFlow:** the 52 ms step through AsyncFlow + RHAPSODY is mostly not Dragon's edge, which
  is 3–4 ms. Submitting ahead with Batch Task arguments would cut a dependent step to a few ms,
  so it is worth building.
  - Part of the 52 ms is how RHAPSODY learns of a completion: D below finds that `Batch.poll()`
    reports a lone task about 47 ms after it ends.

## D. V1-style launch cost: one Dragon process per task against Batch

Asked by the main session: Batch (warm pool workers) against an executor that starts one Dragon
process per task, as RHAPSODY ≤ 0.4.0's DragonExecutionBackendV1 did.

- **Setup:**
  - raw Dragon on julia (`-c 64`; Batch has 32 workers), `experiments/diag_launch.py`;
  - closed loops with 1, 8 and 32 tasks in flight;
  - four task kinds: function `f(x)`; the same after `sleep(1)`; `/bin/true`; `sleep 1`.
- **V1 (stock 0.14.1):**
  - one `dragon.native.process.Process` per task, no placement policy;
  - completions come back on one shared `dragon.native.queue.Queue`;
  - a reaper thread joins every process;
  - executables run inside the Python target with `subprocess.run`, which times the child, so a
    V1 executable is a Python launcher plus the child.
- **Batch (branch dbf5405 with (a)+(b)):**
  - `Batch.function` for functions; `Batch.process` with `launch_exe.py`, the same timing
    launcher, for executables;
  - completions come from `Batch.poll()`;
  - Batch at 1 and 8 in flight added as well.
- **Counts and jobs:** 300 trivial tasks per configuration; 1 s tasks max(30, 10 × in-flight).
  3 jobs per launcher (24936–24941). The first 5 tasks are left out; cold = the first task.
- **Outcome:** 0 failures, and 0 V1 processes left alive.
- **Results:** `results/r3/launch/` (raw, `summary.md` with p95 and max); chart `r3_launch`.

Median of 3 jobs; launch = submit or `start()` to the body's first line; report = body end to the
client seeing the completion. Busy = body time ÷ (in flight × span), for 1 s tasks.

**Function, trivial**

| Launcher | In flight | Launch p50 | Report p50 | `start()` blocks | Tasks/s |
|---|---|---|---|---|---|
| V1 | 1 | 128 ms | 0.1 ms | 2.4 ms | 7.7 |
| V1 | 8 | 134 ms | 0.9 ms | 2.4 ms | 57.8 |
| V1 | 32 | 200 ms | 3.3 ms | 3.9 ms | 162 |
| Batch (a)+(b) | 1 | 3.6 ms | 46.8 ms | – | 19.7 |
| Batch (a)+(b) | 8 | 5.0 ms | 48.0 ms | – | 150 |
| Batch (a)+(b) | 32 | 14.5 ms | 44.1 ms | – | 728 [566–906] |

**Function, sleep 1 s**

| Launcher | In flight | Launch p50 | Report p50 | Tasks/s | Busy |
|---|---|---|---|---|---|
| V1 | 1 | 129 ms | 0.2 ms | 0.9 | 89% |
| V1 | 8 | 132 ms | 3.4 ms | 7.2 | 83% |
| V1 | 32 | 168 ms | 1.9 ms | 29.4 | 84% |
| Batch (a)+(b) | 32 | 5.2 ms | 9.3 ms | 33.8 | 96% |

**Executable, `/bin/true`**

| Launcher | In flight | Launch p50 | Report p50 | Tasks/s |
|---|---|---|---|---|
| V1 | 1 | 128 ms | 0.1 ms | 7.7 |
| V1 | 8 | 135 ms | 0.2 ms | 57.6 |
| V1 | 32 | 198 ms | 2.4 ms | 161 |
| Batch (a)+(b) | 32 | 281 ms | 209 ms | 59.3 |

**Executable, `sleep 1`**

| Launcher | In flight | Launch p50 | Report p50 | Tasks/s | Busy |
|---|---|---|---|---|---|
| V1 | 1 | 129 ms | 0.2 ms | 0.9 | 89% |
| V1 | 8 | 132 ms | 3.6 ms | 7.2 | 83% |
| V1 | 32 | 167 ms | 1.3 ms | 29.4 | 84% |
| Batch (a)+(b) | 32 | 272 ms | 198 ms | 22.8 | 67% |

Batch at 1 and 8 in flight for each kind: `results/r3/launch/summary.md`.

**Reading.**
- **One Dragon process per task costs about 130 ms to start, 170–200 ms at 32 in flight.**
  - It is the process start, not `start()`, which blocks only 2–4 ms. Global services launch
    about 160 processes/s at 32 in flight.
  - For tasks of about 1 s, that already costs 11–17% of busy time, against Batch functions' 4%.
  - It stops mattering only above about 10 s per task, where it is under 2%.
  - For trivial functions, Batch's warm workers are 4.5× faster: 728 against 162 tasks/s.
- **For executables, V1 is the faster launcher on rust:** 161 against 59 tasks/s, with 29.4
  against 22.8/s for 1 s tasks.
  - Batch's `process()` path takes 270–280 ms to launch and about 200 ms to report.
- **Batch functions report late when nothing else is happening.** Report p50 is 44–48 ms with 1,
  8 or 32 trivial tasks in flight, against 0.1–3 ms for V1.
  - The likely cause, in Batch's client worker (`batch.py:6326` and `:6333` on the branch, `:6256` and `:6263` in the 0.14.1 wheel): each loop
    drains the managers' completions once, then blocks up to 50 ms on the client's request queue
    (`_client_request_q.get(timeout=0.05)`).
  - A completion therefore reaches `poll()` up to 50 ms late, unless a new submission wakes the
    worker; with 1 s tasks at 32 in flight that happens often, and report p50 drops to 9.3 ms.
  - `Task.get()` doesn't wait for that loop: about 4 ms per round trip in C.
  - RHAPSODY learns of completions through `poll()`. This likely accounts for most of the
    52–57 ms dependent step in E3; I did not measure that directly.

## Caveats

- **One pair of nodes for each part.** In E2 the GPUs differ in memory and in model. Kernel rates
  are matched, so tasks compute for the same time on either, but the Blackwell's context is
  0.13 GiB larger than modelled.
- **E2's memory scale is chosen to make haskell's memory bind** (×1.5). At the first campaign's
  sizes, the 8-task GPU share binds first, and Dragon's node choice would rarely overload
  haskell. The result is about this regime: totals that fit, nodes that don't.
- **E2's "none" mode is not IMPRESS's pinning.** On one node, pipeline p pinned GPU p mod 2. With
  one GPU per node, nothing can be pinned without placement, so "none" is Dragon's choice.
- **The one-node references come from the second campaign**, a few hours earlier on julia with 64
  CPUs; there was no same-day one-node rerun in this campaign. Part B's two-node jobs used 32 CPUs
  on each of two nodes.
- **The heterogeneous-job method relies on a wrapper** that rewrites Dragon's `srun` calls. It
  assumes one node per component, which holds here.
- **The "Network Config Error Detected" lines in every log are the wrapper's** stderr message,
  which Dragon prints. They are not a network error.
- **userB's GPU job ran on haskell during all of Part B** (16 CPUs of 512; 3.4 busy on average
  besides ours). Part B uses no GPU.

## What this means

- **On nodes of different GPU memory, aggregate admission is not enough.** Dragon's Batch picks a
  task's node without looking at GPU memory: at random with one task per compile, or evenly
  within a compile. So the smaller GPU fills while the totals fit.
  - Node mode, which places each task on a node and GPU that fit it, removes the OOMs and the
    lost pipelines, and gets 12–15% more work done.
  - This is the multi-node case for `task["placement"]` in RHAPSODY.
- **For CPU work, the policies behave on two nodes as on one.** Per-task reports, capacity =
  pool, Lineage's ordering and Fair's latency all carry over, at some cost to Fair's chains.
- **Two nodes cut Dragon's throughput for trivial tasks to 37–42% of one node's.** The
  loss comes from the runtime spanning two nodes, not from the policies. For real tasks of
  seconds (E1a, E1b) it costs 2–8% of makespan; for very short tasks it would dominate.

## Follow-ups

- **Batch's 50 ms report wait.** Wake the client worker when a completion arrives (or drain
  `ret_q` on a shorter timeout), then remeasure RHAPSODY's dependent step. If D's reading is
  right, the 52 ms step should fall to a few ms without submitting ahead.
- **Locate stock 0.14.1's 10 ms edge floor**, which per-task reports (b) remove.

- **Find where two-node Batch loses throughput.**
  - Run raw Batch on one node in a one-node runtime and in a two-node runtime, in the same
    session, with Dragon's debug logs.
  - Time the scheduler manager's routing, the allocation round-trips and the DDict result writes.
- **A memory-aware node choice in Dragon** (or RHAPSODY placing every GPU task) would make even
  aggregate admission safe. Placement already works; ResourcePolicy's node mode is the way to use
  it today.
- **More nodes, and two GPUs per node**, once the partitions allow it: one job over haskell
  (2 Ada) and fortran (2 Blackwell) would test per-GPU placement and node choice together.

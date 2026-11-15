# Multi-node Dragon, #95 and the GPU density limit: fifth campaign, on PSC Bridges-2

Status: experiments, 2026-10-24 to 2026-10-29, on Bridges-2, charged to `dmr170002p`. Everything is
local: the RHAPSODY integration branch is local only, nothing pushed, nothing posted to GitHub.
E2 at IMPRESS scale did **not** run: the GPU tests that came first found a Dragon failure under
oversubscribed workers, and the GPU partitions' queues stalled the follow-up for days (see E2).

The rust campaigns ([rust-experiments.md](rust-experiments.md) to
[rust-experiments-4.md](rust-experiments-4.md)) ran on one or two small nodes. This campaign moves
the same experiments to Bridges-2: raw Batch and E1/E3 on 2 and 4 RM nodes (128 cores each), #95 on
RM-shared, and E2 on V100 nodes.

Files:
- Scripts: `design/presentations/scheduling-policies/experiments/`. New: `b2run.sbatch` (the
  runner), `b2_submit.sh`, `b2smoke1.sbatch`, `b2smoke2.sbatch`, `b2density.sbatch`,
  `b2density4.sbatch`, `b2stall.sbatch`, `b2_issue95.py`, `analyze_b2.py`. Changed: `e2_gpu.py`
  (per-tool task kinds, Dragon-derived node shapes, learned memory, worker override, SIGUSR1 stack
  dump, per-failure tracebacks), `diag_twonode.py` (`--layouts`, `--tag`), `common.py`,
  `node_sampler.sh` (`ps` snapshots), `srun-hetjob` (short host names, log file).
- Raw results: `results/b2/raw/{raw,af,i95,smoke,density4,stall}/`.
- Tables: `results/b2/summary.md` and `summary.json`.
- Charts: `results/b2/charts/b2_raw_batch`, `b2_af`, `b2_i95`, PDF and PNG.
- Diagnostics: `results/b2/diag/` — job logs (`logs/`, with `projects` before and after), run
  directories (`runs/`: node load, `ps`, GPU samples, per-task records), the Dragon DEBUG logs of
  section 3a (`dragon-debug-50006591-runs7-9.tar.xz`, 11 MB from 700 MB), tree commits.
- Diffs: `results/b2/diffs/` — the RHAPSODY integration branch against its parents.

## Questions and answers

**Where does the two-node loss seen on rust come from?** From the runtime's size, not from
spreading tasks.
- One node alone does 635–668 tasks/s (stock) on 2,000 trivial tasks. The *same single node*
  inside a 2-node runtime does 283–295 tasks/s, and inside a 4-node runtime 237–239.
- Once the runtime is multi-node, using more of its nodes changes little: 245–265 tasks/s on 2 of
  2 nodes, 251–266 on 4 of 4.
- The dependent step follows: 10.2 ms on one node alone, 13 ms for one node inside a larger
  runtime, 16–18 ms across 2 or 4 nodes.
- So the cost is paid as soon as Dragon's infrastructure spans nodes (global services, the TCP
  transport over IPoIB), whatever the tasks touch. This matches rust's two-node loss and points
  at Dragon, not at AsyncFlow or RHAPSODY.

**Do the policies still help at 2 and 4 nodes?** Yes, more than on one node.
- E1a, an interactive chain beside bulk work: chain p50 1.9 s with Fair against 45.5 s with no
  policy on 2 nodes; 2.1 s against 56.4 s on 4 nodes. Bulk makespan costs 3–4%.
- E1b, diamonds: the first diamond finishes after 3.2 s with Lineage against 7.3 s with none on
  2 nodes, and 4.9 against 25.2 s on 4 nodes. Lineage keeps 128 or 256 diamonds open against 280
  or about 1,590; the makespan costs 4% on 2 nodes and 15% on 4.
- The no-policy baseline gets worse with the worker count: on one node no policy's chain p50 is
  1.80 s with 32 workers (follow-up 2, close to Fair's 1.62 s, as on rust), 20.6 s with 64
  (follow-up 3) and 44.1 s with 128 (follow-up 4), against 45.6 s for 128 workers over 2 nodes;
  Fair stays at 1.6 s. The worker count alone produces the gap; spanning nodes adds little.
- E3: the policies cost nothing at the median (20–21 ms on 2 nodes, 25–27 ms on 4). On 4 nodes
  they cut the p95 from 177 ms to 42–48 ms.

**Does the pool cap fix #95 on Bridges-2?** Yes, as on rust.
- On 4 RM-shared cores, stock RHAPSODY 0.6.0 starts 64 workers: one trivial task takes 12.8–17.6 s,
  #95's workflow 17–25 s, shutdown 13–51 s. With the pool cap (2 workers): 0.6–0.9 s, 0.4–0.7 s and
  1.6–1.7 s.
- On 16 cores the gap is smaller: 2.0–3.3 s, 7.4–8.5 s and 2.2–3.5 s against 0.2–0.5, 0.5 and
  1.2–1.5 s.
- rust's reference was 111 s against 0.5 s; Bridges-2's slower case is less extreme but the same
  shape: oversubscribed workers, not the workflow, cost the time.

**What density can E2 run at on a V100 node?** Up to 1.6 Batch workers per core ran clean with
Dragon 0.14.1, which allows 8 tasks per GPU on 4 GPUs; 2.4 per core (48 on 20 cores, the ratio
of the planned 96 workers on a 40-core node) did not.
- With 48 workers on 20 cores and 32 GPU pipelines, node-mode E2 either stalled (no completion
  after the first 32 tasks, twice) or Dragon failed to launch tasks (`TypeError: 'NoneType' object
  is not subscriptable`, `ProcessGroup manager is not in state DEAD`; 12 and 17 pipelines lost).
- With 20 workers on 20 cores the same run was clean: 82 tasks, no OOM, no loss.
- The threshold test (job 49897532, run on 2026-10-29 after about 3 days in the queue) was clean
  at 24, 28 and 32 workers on 20 cores: 86, 107 and 126 tasks, no OOM, no loss; at 32 workers it
  reached 8 tasks per GPU. So the break is between 32 and 48 workers on 20 cores, not at the core
  count; E2's density is still to be decided.

## Setup

- **Nodes:** RM: 128 cores (2 × AMD EPYC 7742), 256 GB; Batch's default 64 workers per node, so
  never more workers than cores. GPU: v100-32, 40 cores, 8 × V100-SXM2-32GB, 515 GB.
- **Dragon:** stock `dragonhpc 0.14.1` wheel; "branch" is the same wheel with `batch.py` from
  `0a7d87d` (switches (a) linger 0 / batch size 1, (b) a report per task, (e) wake on completion).
  Two uv venvs, Python 3.11.17, built with `UV_LINK_MODE=copy` (uv's hard links had made the two
  envs share `batch.py`; rebuilt before any result). Transport: TCP over IPoIB (`ib0`,
  `--network-prefix '^(ib)\w*$'`); HSTA is not configured.
- **AsyncFlow:** `feature/resource-policy` `22d4882` (af-resource) for everything.
- **RHAPSODY:** local `experiment/bridges2` `d572b35` (rh-b2): `14faf2f` + placement `bb7e76b` +
  the pool cap, `compile_each_task` and observed peaks (`8a74e99`). #95 compares `rh-fix`
  `d2cbc58` (the pool cap) with 0.6.0 `d5ae5fa`.
- **Runner:** `b2run.sbatch` runs one Dragon runtime per plan line, samples each node (load,
  `ps`, `nvidia-smi`), merges the node state into each result, and between runs removes only our
  own `/dev/shm` files newer than the run and counts leftover Dragon processes.
- **Site rules kept:** one job stream per name with `-d singleton` (d2 GPU, d2s GPU-shared tests,
  d2r RM), so two of our Dragon jobs never share a node; nothing run on login nodes; no
  `dragon-cleanup`; transfers started from the Mac to `data.bridges2.psc.edu`.

## 3a. Raw Batch across 1, 2 and 4 nodes (job 50006591)

Three runtimes on the same 4 nodes, each running 2,000 trivial tasks and a chain of 100 on 1, 2
or 4 of its nodes, three reps, layouts reversed in even reps. Then one DEBUG run per runtime size.

| Runtime | Nodes used | Stock tasks/s | Stock step p50 | Branch tasks/s | Branch step p50 |
|---|---|---|---|---|---|
| 1 | 1 | 668 [635–668] | 10.2 ms | 664 [640–664] | 5.5 ms |
| 2 | 1 | 295 [283–295] | 13.2 ms | 288 [272–291] | 10.8 ms |
| 2 | 2 | 256 [245–265] | 15.7 ms | 194 [186–214] | 13.0 ms |
| 4 | 1 | 237 [237–239] | 13.3 ms | 223 (1 run) | 12.4 ms |
| 4 | 2 | 240 [211–246] | 17.3 ms | 161 (1 run) | 16.9 ms |
| 4 | 4 | 253 [251–266] | 18.3 ms | hung | |

- **The branch's switches halve the one-node step** (5.5 against 10.2 ms), as on rust, but across
  nodes they cost throughput: 194 against 256 tasks/s on 2 of 2 nodes. A report per task means
  more cross-node messages over the slower transport.
- **The branch hung in the 4-node runtime**: in rep 1 it finished the 1-node layout (223 tasks/s)
  and the 2-node layout (161 tasks/s), then hung in the 4-node layout until the 611 s timeout.
- **DEBUG logging changes the result:** 108 tasks/s on one node, 139 on two; the 4-node DEBUG run
  died after 660 s with about 10,000 `ChannelRecvTimeout` errors in the TCP transport agents. The
  DEBUG logs are kept for 1 and 2 nodes; at 4 nodes they show the transport timing out under its
  own logging, so they cannot locate the non-DEBUG cost.

## 3b. E1a, E1b and E3 on 2 and 4 nodes (jobs 50006592–50006597)

Branch (a)+(b)+(e); 128 workers on 2 nodes, 256 on 4; capacity = pool. The workloads scale with
the workers to keep rust's contention: E1a 1,000 bulk tasks per 32 workers (4,000 and 8,000) plus
12 chains of 3 × 0.5 s; E1b 256 diamonds per 32 workers (1,024 and 2,048), 1 s tasks. Runs
shuffled per rep. Chart: `b2_af`.

| | 2 nodes, none | 2 nodes, policy | 4 nodes, none | 4 nodes, policy |
|---|---|---|---|---|
| E1a chain p50 (Fair) | 45.5 s | 1.94 s | 56.4 s | 2.09 s |
| E1a bulk makespan | 98.5 s | 102.2 s | 100.0 s | 103.4 s |
| E1b first done (Lineage) | 7.3 s | 3.2 s | 25.2 s | 4.9 s |
| E1b makespan | 35.3 s | 36.6 s | 43.8 s | 50.2 s |
| E3 step p50 (Fair / Lineage) | 20.6 ms | 21.3 / 20.2 ms | 25.5 ms | 26.5 / 26.1 ms |
| E3 step p95 | 59 ms | 48 / 48 ms | 177 ms | 42 / 48 ms |
| E3 tasks/s | 163 | 185 / 186 | 162 | 160 / 156 |

- **E3 on 4 nodes, rep 1, hung** (job 50006595): Fair's pool of 256 workers started and nothing
  came back for 900 s. Reps 2 and 3 were clean. With 3a's branch hang, both hangs are the branch
  `batch.py` on 4 nodes; no stock run hung outside DEBUG.
- **No policy's 45–56 s chains come from scale, not from the site.** rust's no-policy chain p50
  was 1.75–1.86 s on one node and 2.07 s on two (32 workers, rust-experiments-3/-4), close to
  Fair. Follow-up 2 (job 50008020) ran rust's E1a on one Bridges-2 RM node with 32 Batch
  workers, capacity 32, the same trees, 3 reps each, order alternated:

  | 1 node, 32 workers | Chain p50 | Chain p95 | Bulk makespan |
  |---|---|---|---|
  | No policy | 1.80 s [1.72–1.83] | 2.07 s [1.94–2.13] | 98.6 s [96.5–98.6] |
  | Fair | 1.62 s [1.58–1.65] | 1.81 s [1.69–1.95] | 98.5 s [97.5–98.8] |

  That is rust's result (1.75–1.86 and 1.59–1.62 s). So Bridges-2 is not the cause: with 32
  workers no policy keeps chains short, and at 128 workers over 2 nodes or 256 over 4 it does
  not.
- **It is the worker count.** Follow-up 3 (job 50018097) ran the same E1a on one RM node
  with 64 workers (the per-node count of the 2- and 4-node runs), capacity 64, bulk 2,000, the
  same trees, 3 reps each, order alternated; all six runs clean:

  | 1 node, 64 workers | Chain p50 | Chain p95 | Bulk makespan |
  |---|---|---|---|
  | No policy | 20.56 s [19.80–21.81] | 50.45 s [49.53–51.59] | 97.8 s [96.6–98.4] |
  | Fair | 1.57 s [1.57–1.60] | 1.70 s [1.64–1.71] | 97.4 s [96.8–98.6] |

  Per rep, no policy: 20.56, 21.81, 19.80 s (makespan 97.8, 98.4, 96.6 s); Fair: 1.60, 1.57,
  1.57 s (97.4, 98.6, 96.8 s). No policy's chain p50 goes 1.8 → 20.6 → 45.6 → 56.4 s at 32, 64,
  128 and 256 workers (1, 1, 2 and 4 nodes) with the bulk scaled alike; Fair stays at 1.6–2.1 s
  and the makespan near 98 s. Doubling workers on one node already takes the chain from 2 s to
  tens of seconds, so the worker count (and the bulk backlog that comes with it) drives the gap.
- **Spanning nodes adds little.** Follow-up 4 (job 50076655) ran the same E1a on one RM node with
  128 workers, capacity 128, bulk 4,000; all six runs clean:

  | 1 node, 128 workers | Chain p50 | Chain p95 | Bulk makespan |
  |---|---|---|---|
  | No policy | 44.11 s [43.61–45.33] | 74.04 s [73.54–75.23] | 97.5 s [96.9–98.3] |
  | Fair | 1.55 s [1.55–1.57] | 1.61 s [1.57–1.62] | 97.4 s [97.0–97.7] |

  Per rep, no policy: 43.61, 45.33, 44.11 s (makespan 96.9, 98.3, 97.5 s); Fair: 1.55, 1.57,
  1.55 s (97.0, 97.7, 97.4 s). With 128 workers no policy's chain p50 is 44.1 s on one node and
  45.6 s over 2 (p95 74.0 against 75.5 s): the same within 2 s. The series by workers is
  1.8 / 20.6 / 44.1 s on one node, 45.6 s on 2 and 56.4 s on 4; whether the 4-node step comes from
  256 workers or from 4 nodes is not separable on one 128-core node. Each run warned that Dragon
  may use only 128 of the node's 256 CPUs (SMT): the pool was set to 128 explicitly, so the
  workers did not exceed the allowed CPUs.
- The result files say `stock` in the experiment name (`e1-mixed-stock_…`): that is E1's
  submission mode, not the Dragon build; every 3b run used the branch (the `dragon` field).

## 4. rhapsody#95 on RM-shared (jobs 50006598–50006605)

One run per job, stock Dragon, 4 and 16 cores, two reps each. Chart: `b2_i95`.

| Cores | RHAPSODY | Workers | One trivial task | #95's workflow | Shutdown |
|---|---|---|---|---|---|
| 4 | 0.6.0 | 64 | 15.2 s [12.8–17.6] | 21.0 s [17.0–25.1] | 31.8 s [12.8–50.9] |
| 4 | pool cap | 2 | 0.78 s [0.62–0.94] | 0.56 s [0.39–0.73] | 1.6 s |
| 16 | 0.6.0 | 64 | 2.6 s [2.0–3.3] | 8.0 s [7.4–8.5] | 2.8 s [2.2–3.5] |
| 16 | pool cap | 8 | 0.37 s [0.21–0.53] | 0.50 s [0.48–0.53] | 1.3 s |

All 8 runs finished with the right answer. The 4-core 0.6.0 runs shared their node with other
users' jobs (load 146 at the start of one), which widens their spread.

## E2: the GPU tests, and why E2 did not run

The plan was E2 on 4 v100-32 nodes (32 GPUs, 256 pipelines, 8 per GPU, 96 Batch workers per node),
after a one-node density test. What ran instead (all GPU-shared, V100-32GB, 5 cores per GPU):

| Job | Run | GPUs | Pipelines | Workers | Peak tasks at once (per GPU) | Tasks | Outcome |
|---|---|---|---|---|---|---|---|
| 49821147 | res-node | 1 | 4 | 8 | 4 (4) | 4 | clean |
| 49821147 | res-learn | 1 | 4 | 8 | 2 | 3 | 2 lost: my pinning bug, fixed |
| 49881976 | res-node | 4 | 32 | 48 | 32 (8) | 32 | stalled after the first 32 |
| 49881976 | res-learn | 4 | 32 | 48 | 20 (7) | 84 | 12 lost, `TypeError` |
| 49885043 | res-node | 1 | 8 | 16 | 8 (8) | 25 | clean |
| 49885184 | res-node | 4 | 32 | 48 | 15 (6) | 99 | 17 lost at 91.9 s, `TypeError` ×14, `ProcessGroup` ×3 |
| 49886638 | res-node | 4 | 32 | 48 | 32 (8) | 64 | stalled after 64 |
| 49886638 | res-node | 4 | 32 | 20 | 20 (5) | 82 | clean |
| 49897532 | res-node | 4 | 32 | 32 | 32 (8) | 126 | clean |
| 49897532 | res-node | 4 | 32 | 24 | 24 (6) | 86 | clean |
| 49897532 | res-node | 4 | 32 | 28 | 28 (7) | 107 | clean |

- **The failure needs both many workers per core and many launches at once.** 8 pipelines on one
  GPU with 16 workers on 5 cores ran clean; 32 pipelines with 48 workers on 20 cores never did;
  20 workers on 20 cores did, and so did 24, 28 and 32 (job 49897532).
- **The lost tasks never started:** no task record, all failing at the same instant, raised by
  Dragon when launching the process. The stalls show the client idle in `poll()` (SIGUSR1 stack
  dump), waiting for completions that do not come.
- **Idle workers cost CPU:** 92 Dragon native processes for 48 workers took about 20% CPU each
  while idle, about 1,900% together — all 20 cores. GPU tasks use little CPU
  (`CU_CTX_SCHED_BLOCKING_SYNC`; under 75% in total for 32).
- **Memory was fine wherever tasks ran:** 24.4–26.4 GiB peak per GPU of 31.73 at 8 per GPU, no
  CUDA OOM in any run. At 8 per GPU an rfd3 burst took 1.35× its nominal time.
- **Consequence:** with Dragon 0.14.1, E2's 96 workers on a 40-core node would oversubscribe 2.4×.
  At one worker per core a node runs 40 tasks, 5 per GPU: about 19 GiB per GPU, so aggregate
  admission would no longer be stressed and E2 would lose its point. The threshold test then
  ran clean at up to 32 workers on 20 cores (8 per GPU), so the limit sits between 1.6× and
  2.4× the cores; the whole-node density test (49821597) is still held.
- **Smoke test 2 found the hostname bug:** Dragon passes `--nodelist` with FQDNs
  (`r016.ib.bridges2.psc.edu`), which Slurm rejects; `srun-hetjob` now strips the domain.
  "Network Config Error Detected" was our wrapper's own stderr note, not a network fault.

## SU

From the job logs (sacct timed out, slurmdbd unreachable): first run to `job done`, plus a minute.

| | RM SU | GPU SU |
|---|---|---|
| Setup and smoke tests (Oct 25) | 74 | 0.3 |
| GPU density and stall tests (Oct 26) | — | 2.7 |
| 3a raw Batch | 268 | — |
| 3b, 2 nodes, 3 reps | 100 | — |
| 3b, 4 nodes, 3 reps | 352 | — |
| 4, #95, 8 jobs | 2 | — |
| Follow-up 2, E1a on 1 node, 32 workers (job 50008020) | 26 | — |
| Follow-up 3, E1a on 1 node, 64 workers (job 50018097) | 27 | — |
| Follow-up 4, E1a on 1 node, 128 workers (job 50076655) | 27 | — |
| GPU threshold test (job 49897532) | — | 0.9 |
| **Total** | **876** | **3.9** |

`projects` on 2026-10-29 still showed 65 RM and 3 GPU SU used (it lags). Budget: 1,600 RM and
300 GPU estimated; caps 3,000 and 500.

## Follow-ups

1. **Dragon, for HPE:** launch failures and stalls when Batch workers outnumber cores and ~32 GPU
   processes launch at once (tracebacks in `results/b2/raw/stall/`); the branch `batch.py` hanging
   on 4 nodes (twice); the per-runtime throughput loss (one node: 668 → 295 tasks/s once the
   runtime spans 2 nodes). These belong in the Dragon report draft.
2. **E1a no-policy gap: done, the worker count.** One node, 32 workers (job 50008020): no policy
   1.80 s, Fair 1.62 s, as on rust. One node, 64 workers (job 50018097): no policy 20.6 s, Fair
   1.57 s. One node, 128 workers (job 50076655): no policy 44.1 s, Fair 1.55 s, against 45.6 s for
   128 workers on 2 nodes, so spanning nodes adds little.
3. **E2:** the threshold job ran clean at up to 32 workers on 20 cores; E2 at a density the user
   chooses from that (32 workers per 4 GPUs, i.e. 64 per 8-GPU node at 8 per GPU, untested), or with
   Dragon 0.15's `workers_per_node`, which may decouple workers from launches.
4. **The 4-node E3 hang:** rerun E3 alone on 4 nodes with the branch, several reps, to measure how
   often it hangs.

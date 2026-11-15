# Dragon on rust: rhapsody#95 is CPU oversubscription

Status: findings, 2026-10-24. Experiments on the rust cluster (haskell and julia), with
dragonhpc 0.14.1, rhapsody-py 0.6.0 and radical.asyncflow 0.6.0. There are two local fixes,
kept apart and neither pushed:
- Dragon: `ae8af5e` on `fix/batch-affinity-sizing`, from `v0.14.1`.
- RHAPSODY mitigation: `d2cbc58` on `fix/dragon-affinity-sizing`.

Nothing has been posted to GitHub.

## Question and answer

**Question.** Is [radical-cybertools/rhapsody#95](https://github.com/radical-cybertools/rhapsody/issues/95)
caused by CPU oversubscription on rust? And how do we run current RHAPSODY + Dragon there?

**Answer.**
- **Yes, it is oversubscription.** Dragon's `Batch` sizes its worker pool from the node's
  CPU count (512), not from the CPUs the job holds. A `-N1 -n4` job gets 4 logical CPUs, so
  about 280 Dragon processes share them: 256 workers, 2 managers and the runtime.
  - On that slice, one trivial `Batch` task takes 111 s. The issue's 3-task workflow takes
    271 s, and its shutdown does not finish within 5 minutes.
  - With the pool sized to the slice (2 workers), the same task takes 0.5 s, the workflow
    2.5 s, and the shutdown 1.2 s.
- **Whole nodes are not an option on rust.** The `normal` QOS caps each user at 128 CPUs, so
  an `--exclusive` job waits forever (`QOSMaxCpuPerUserLimit`). Every Dragon run on rust is a
  partial-node run, so the pool must be capped.
- **The root cause is in Dragon.** A small change in `Batch.__init__` sizes the pool from
  `min(os.cpu_count(), len(Node.cpus))`. It is built from source on rust.
  - With stock RHAPSODY 0.6.0 and no shim, the 4-CPU slice gets 2 workers, the first task
    takes 0.3 s, and the workflow 2.2 s.
  - Dragon's own Batch tests pass on the slice (20, including 6 new ones).
  - Dragon `main` (0.15.0) still has the old sizing.
- **A working recipe exists for all three setups.** The sbatch script is in [Recipe](#recipe).
  - Stock dragonhpc 0.14.1, the setup the policy experiments will use: the pool is capped by
    the RHAPSODY fix branch or, with the 0.6.0 release, by a 20-line import shim.
  - The patched Dragon build: `DRAGON_TREE=...`.
  - Verified on a 64-CPU slice: 32 workers, the workflow in 2.2 s, a clean shutdown, and no
    `/dev/shm` leftovers.
- **DragonHPC/dragon#89 does not bear on this.** It fixes a leak of cleanup notifications in
  the tcp transport agent, for channels that received sends from another node. #95's stall,
  `DDict.__del__` → `destroy_process_local()`, is a request to Local Services on the same
  node, in a single-node job. See [Upstream](#upstream-dragonhpcdragon89).
- **Stretch: both checks work.**
  - GPU pinning: with `feature/dragon-placement` merged with the fix, a task placed on GPU 1
    sees only GPU 1, checked through libcuda and not only through `CUDA_VISIBLE_DEVICES`.
  - 2 nodes: Dragon runs on haskell + julia over tcp, and placed tasks land on the node they
    name.

## Mechanism

- `Batch.__init__` (`dragon/workflows/batch/batch.py:5167`, dragonhpc 0.14.1):
  `cpus_per_node = max(1, node_objs[0].num_cpus // 2)`, and one pool of that many workers
  per node. `Node.num_cpus` is the node's `os.cpu_count()` (`infrastructure/node_desc.py:177`),
  which is 512 on haskell and julia. `Batch()` has no parameter for the number of workers.
  Dragon `main` (0.15.0, `fae6165`) has the same line, at `batch.py:5245`.
- Dragon's Slurm launcher starts its backend with
  `srun --nodes=N --ntasks=N --cpu_bind=none` (`launcher/wlm/slurm.py`). Without `--exact`,
  that step gets all the job's CPUs on each node, and no more. On a `-n4` job that is
  `Cpus_allowed_list: 0,128,256,384`, i.e. 2 cores with 2 threads each. Every Dragon process
  inherits it: the samples show 279 processes, all with affinity `0,128,256,384`.
- Dragon already knows the right number. `Node.cpus` is the Local Services affinity and
  reports 4, but Batch never reads it.
- How this produces each symptom in #95:
  - The node load average is 160–265 on 4 allowed CPUs.
  - Every `poll()`, DDict access or channel teardown is a round trip between processes that
    each get about 1/70 of a CPU. That is why `poll()` and DDict teardown take minutes.
  - V1 starts one process per task and has no pool, which is why it works in 3 s.
  - Bridges-2 uses whole nodes, where the affinity equals `cpu_count`, so it does not
    reproduce there.
- With rhapsody 0.6.0, the uncapped workflow finishes, slowly, where the issue's 0.5.0 run
  hung for 25 minutes. Shutdown still times out: rhapsody logs
  `Error closing batch gracefully: timed out waiting for batch runtime shutdown`.
- At 128 CPUs the uncapped pool is 2x oversubscribed and costs about 2 s on the first task.
  On the 4-CPU slice it is 64x oversubscribed. The cost grows with the ratio.

## Evidence

All runs were on haskell (julia was added for the 2-node runs), Slurm partition `cpu` (`gpu`
for the GPU check), launched with `dragon -w slurm -t tcp --network-prefix
'^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'`. Times come from the scripts' own timestamps.

"Capped" means the pool was sized from `Node.cpus`. For runs on the 0.6.0 release, that was
done by overriding `Node.num_cpus` in the client before `Batch()` (env `CAP_TO_AFFINITY=1` in
`job.sh`). "Fix" means `fix/dragon-affinity-sizing`, with no shim.

Dragon: every run in the next two tables used the stock dragonhpc 0.14.1 wheel from PyPI
(`v0.14.1`, `0701b62`). The patched Dragon build has [its own table](#patched-dragon-results).

Raw `Batch`, one trivial task, then a second one, then `join()` (`raw_batch.py`):

| Job | Allocation | Pool | 1st task | 2nd task | `join()` | Dragon wall |
|---|---|---|---|---|---|---|
| 24429 | `-n4` (4 CPUs), uncapped | 256 workers | 111.1 s | 59.5 s | > 3.5 min, killed | 412 s (timeout) |
| 24431 | `-n4`, capped | 2 workers | 0.5 s | 0.1 s | 1.6 s | 9 s |
| 24433 | `-c128` (QOS maximum), uncapped | 256 workers | 2.4 s | 0.4 s | 1.5 s | 10 s |
| 24434 | `-c128`, capped | 64 workers | 0.5 s | 0.1 s | 1.1 s | 7 s |

The issue's 3-task asyncflow workflow on `DragonExecutionBackend` (`af3.py`):

| Job | Allocation | Pool | task1 / task2 / task3 | Workflow | Shutdown | Dragon wall |
|---|---|---|---|---|---|---|
| 24437 | `-n4`, uncapped (as in the issue) | 256 | 114.3 / 71.4 / 85.3 s | 271.0 s | > 5 min, killed | 618 s (timeout) |
| 24432 | `-n4`, capped | 2 | 0.5 / 0.7 / 1.3 s | 2.5 s | 1.2 s | 11 s |
| 24438 | `-n4`, fix branch | 2 | 0.7 / 0.5 / 1.0 s | 2.2 s | 1.5 s | 11 s |
| 24435 | `-c128`, uncapped | 256 | 1.9 / 0.6 / 1.0 s | 3.5 s | 1.5 s | 11 s |
| 24436 | `-c128`, capped | 64 | 0.5 / 0.7 / 1.0 s | 2.2 s | 1.2 s | 9 s |
| 24443 | recipe sbatch, `-c64`, fix branch | 32 | — | 2.2 s | 1.1 s | 8 s |
| 24444 | recipe sbatch, `-c64`, 0.6.0 + shim | 32 | — | 2.2 s | 1.1 s | 8 s |
| 24459 | final recipe sbatch, `-c64`, 0.6.0 + shim | 32 | — | 2.2 s | 1.2 s | 9 s |
| 24442 | 2 nodes (haskell + julia, 2 CPUs each), fix | 1 per node, 3 managers | — | 4.0 s | 3.5 s | 16 s |

Process samples for the uncapped `-n4` runs (`runs/24429/samples.txt`, `runs/24437/samples.txt`,
taken every 20 s):
- 279–283 processes of mine on the node, all with affinity `0,128,256,384`.
- The Local Services cgroup is `job_<id>/step_2/user/task_0`, with
  `cpuset.cpus.effective=0,128,256,384`.
- The node load average rose from 18 to 164 (raw Batch) and to 265 (asyncflow). The node was
  otherwise idle at about 1.

Not tested: a whole 512-CPU node (`--exclusive --mem=0`). Job 24430 waited on
`QOSMaxCpuPerUserLimit` and was cancelled. The QOS cannot be changed without editing the
cluster configuration, which was out of scope.

### Patched Dragon results

All rows used Dragon `ae8af5e` (`fix/batch-affinity-sizing` on `v0.14.1`, built from source on
rust) with stock rhapsody-py 0.6.0 and radical.asyncflow 0.6.0, without the shim or the
RHAPSODY branch. Dragon logs `node haskell allows 4 of its 512 CPUs; sizing each worker pool
to 2 worker(s)` at INFO.

| Job | Allocation | Script | Pool | Result | Dragon wall |
|---|---|---|---|---|---|
| 24454 | `-n4` | raw Batch | 2 workers | 1st task 0.3 s, 2nd 0.6 s, `join()` 1.8 s | 10 s |
| 24455 | `-n4` | 3-task asyncflow | 2 workers | workflow 2.2 s (0.6 / 1.0 / 0.6 s), shutdown 1.2 s | 11 s |
| 24457 | 2 nodes, 2 CPUs each | 3-task asyncflow | 1 per node, 3 managers | workflow 3.3 s, shutdown 2.1 s | 15 s |
| 24456 | `-n4` | Dragon `test_batch.py`, 4 classes | — | 20 tests OK in 20.6 s | 25 s |
| 24460 | `-c64`, final recipe sbatch (`DRAGON_TREE`) | 3-task asyncflow | 32 workers | workflow 2.2 s, shutdown 1.3 s | 9 s |

Jobs 24450–24453 ran the same four checks on `6c0ad84`, which differed only in the wording
of the log message, with the same outcome.

## Dragon fix

Branch `fix/batch-affinity-sizing`, commit `ae8af5e`, on `v0.14.1` (`0701b62`, the version
rhapsody-py 0.6.0 pins). Local only, not pushed, no fork. It is in the worktree
`~/github/dragonhpc/dragon-batch-affinity`, so the shared clone `~/github/dragonhpc/dragon`,
which other sessions use, was left alone. Dragon development runs inside HPE (`CONTRIBUTING.md`), so any upstream step goes
through a Dragon developer first.

- `src/dragon/workflows/batch/batch.py`:
  - New pure helper `_resolve_cpus_per_node(num_cpus, allowed_cpus, hostname)`, modelled on
    the existing `_resolve_results_ddict_managers_per_pool`. It returns
    `max(1, min(num_cpus, allowed_cpus) // 2)` and a message when that is less than the full
    node. An empty affinity (0) keeps the old sizing.
  - `Batch.__init__` calls it with `len(node_objs[0].cpus)`, i.e. Local Services' affinity,
    and logs the message at INFO once `self.log` exists. Whole nodes are unchanged.
- `test/multi-node/test_batch.py`:
  - New class `TestCpusPerNodeLogic` with 5 pure tests and one test that runs Batch on the
    current allocation.
  - `TestResultsDDictManagersPerPoolE2E._workers_per_node()` now uses the helper; without
    that change it would expect the old pool size on a partial node.
- Lint: `black>=25.1.0 --check` passes on both files (Dragon's Black Linter workflow).
- Build on rust (`build_dragon.sh`, job 24449, 551 s), following `BUILDING.md`
  (`devtools/clean_build base`, so no uv; HSTA is not built):
  - Python: the same EESSI compat-layer 3.11.4 as the stock venv, put first on `PATH` as
    `python3`. The system `python3` is 3.13.
  - Compiler: EESSI `GCCcore/13.2.0`. The compat layer's glibc is 2.37, while the system gcc
    14 builds against glibc 2.41, and its objects can need 2.38 symbols.
  - Run time needs `LD_LIBRARY_PATH=$EBROOTGCCCORE/lib64` (after `module load
    GCCcore/13.2.0`). Otherwise the compat layer's GCC 10 libstdc++ fails with
    `GLIBCXX_3.4.32 not found`.
  - `rhapsody-py==0.6.0`, `radical.asyncflow==0.6.0` and pytest were installed into the
    build's `_env` without the `dragon` extra, so `dragonhpc` stays the editable source tree.
- Rebuilding after a Python-only change needs no compile: rsync the changed files into
  `~/tmp/dragon-rust/dragon-src` and update its `COMMIT` file. `job.sh` and
  `rust-dragon.sbatch` print it as the Dragon version of each run.
- The RHAPSODY mitigation below is what makes stock 0.14.1 work. Combined with the patched
  Dragon, both compute the same pool size, and RHAPSODY still logs its warning.

## Upstream: DragonHPC/dragon#89

[DragonHPC/dragon#89](https://github.com/DragonHPC/dragon/pull/89) (open, by tpn, "Complete
TCP channel cleanup without waiting for a response") does not explain #95 or anything seen on
rust.

- **What it fixes.**
  - The tcp transport server calls `ch.notify_on_destroy()` on a channel the first time it
    opens a send handle for a *remote* `SendRequest` (`transport/tcp/server.py:146`).
  - When that channel is destroyed, `dragon_channel_destroy()` sends a `CHANNEL_CLEANUP`
    gateway event to the local transport agent (`channels.c` `_notify_of_channel_cleanup`)
    and returns.
  - The agent's client forwards it, but then awaits a response the server never sends. So
    the task, its response future and the gateway message's pool allocation leak.
  - The PR completes cleanup requests without waiting. It changes Python and Cython only,
    with no C changes.
- **Why it does not apply to #95.**
  - #95 and the `-n4` runs here are single-node jobs. No remote `SendRequest` reaches the
    tcp server, so no channel is ever flagged for cleanup notification.
  - The stall the issue caught, `DDict.__del__` → `_free_process_local_channels()` →
    `destroy_process_local()`, is `dragon_destroy_process_local_channel()`
    (`messages_api.cpp`). That is a request/response with Local Services on the same node
    (`dragon_ls_send_receive`, `SHDestroyProcessLocalChannel`). It is not a
    `dragon_channel_destroy()` of a flagged channel, and the transport is not involved.
  - It blocks for minutes because Local Services gets a sliver of 4 CPUs among about 280
    processes. With the pool capped, the same teardown is part of a 1.2 s shutdown.
- **Where it does show on rust: multi-node runs.** Every 2-node run (jobs 24441, 24442, 24453,
  24457, all successful) logs 48–82 ERROR lines from `dragon.transport.tcp.client`.
  - Some are timed-out remote receives, logged with tracebacks (`client.py:213-214`).
  - The rest are "Error awaiting response to gateway message" for every request still
    pending at halt (`client.py:173-178`).
  - The leaked `CHANNEL_CLEANUP` awaits that #89 fixes would be among those at halt. The log
    does not show the message kind, so this is not verified.
  - Test: apply #89 to `v0.14.1`, rebuild, rerun 24442 and count. Details are in the
    dragon-report notes (`dragon-report/dragon-on-rust.md`, finding 8).

## RHAPSODY fix

Branch `fix/dragon-affinity-sizing`, commit `d2cbc58`, from `origin/main` (0.6.0, `d5ae5fa`).
Local only. It is in the worktree `~/github/radical/rhapsody-dragon-affinity`, because the
main clone `~/github/radical/rhapsody` is on another session's `fix/dragon-cancel-on-loop`
and was left there.

- `src/rhapsody/backends/execution/dragon.py`:
  - `_nodes_with_restricted_cpus()` lists the Dragon nodes where `len(Node.cpus) <
    Node.num_cpus`.
  - `_pool_sized_to_affinity()` wraps `Batch()`. When some node is restricted, it overrides
    `Node.num_cpus` with `min(num_cpus, len(cpus))` for the duration of the call, restores it
    afterwards (also when `Batch()` raises), and logs one warning, for example:
    `Dragon may use only part of the CPUs on some nodes (allowed/total: haskell 4/512);
    sizing the Batch worker pools from the allowed CPUs. Request whole nodes to use all
    CPUs.`
  - New constructor argument `size_pool_to_affinity=True`. When False, Batch's sizing is kept
    and the warning says the workers will oversubscribe.
  - If the probe fails, `Batch()` still runs, as before. On whole nodes nothing changes and
    nothing is logged.
- Overriding a property is the only lever, because `Batch()` 0.14.1 takes no worker count.
  `num_cpus` is read only at `batch.py:5167`. The real fix belongs in Dragon: Batch should
  size from `Node.cpus`, or accept a workers-per-node argument.
- `tests/unit/test_backend_execution_dragon_affinity.py`: 8 tests with a fake Batch that
  sizes like 0.14.1. They run without Dragon and cover the partial node, a restored property
  (also after a failure), a whole node, opt-out, mixed nodes, a failed probe and an empty
  affinity.
- `CHANGELOG.md` (Unreleased/Fixed) and a "Partial-node allocations" warning in
  `docs/hpc-machines/index.md`.
- Verified:
  - Locally (macOS, py3.13): ruff, ruff format and all pre-commit hooks pass, including the
    strict docs build. The new file passes 8/8. The suites without Dragon: 268 passed,
    12 skipped.
  - On rust, on a `-n4` slice under Dragon (job 24439):
    `test_backend_execution_dragon.py` plus the new file, 58 passed in 24 s. Without the fix,
    the real-Batch tests in that module would hit the 111 s-per-task regime.
  - End to end on rust: jobs 24438, 24442 and 24443 above.
- Merging with the sibling branches:
  - With `feature/dragon-placement` (`762c5b3`), the only conflicts are the import block
    (both add `Node`/`System`; keep `Policy`, `Node` and `System`) and `CHANGELOG.md`.
  - With `fix/dragon-cancel-on-loop` (`f406fbd`), only `CHANGELOG.md` conflicts.
  - The merged tree (`git merge-tree`, with the import conflict resolved) passes both
    Dragon-free test files (28) and the GPU and 2-node checks below.

## Stretch: placement and two nodes

The merged tree (placement + fix) was used, synced to
`~/tmp/dragon-rust/rhapsody-merged/src` on rust.

- **GPU pinning, job 24440.** Partition `gpu`, `--gres=gpu:2` on haskell, 4 CPUs.
  `nvidia-smi`: index 0 = `11:00.0`, index 1 = `61:00.0`. Each task reports
  `CUDA_VISIBLE_DEVICES` and the PCI bus IDs that `cuDeviceGetCount`/`cuDeviceGetPCIBusId`
  enumerate:
  - `gpus=[0]`: `CUDA_VISIBLE_DEVICES=0`, CUDA sees only `0000:11:00.0`.
  - `gpus=[1]`: `CUDA_VISIBLE_DEVICES=1`, CUDA sees only `0000:61:00.0`.
  - `gpus=[0,1]`: both.
  - An executable with `gpus=[1]`: `CUDA_VISIBLE_DEVICES=1`.
  - `gpus=[7]`: FAILED with `ValueError("placement GPUs [7] are not on Dragon node
    'haskell'; its GPUs: [0, 1]")`, and the runtime stayed up.
  - An unplaced native function ran in a Batch worker that Dragon had pinned to GPU 1.
    Unplaced tasks therefore are not GPU-free.
- **Two nodes over tcp, jobs 24441 and 24442.** `-N2 -w haskell,julia --ntasks-per-node=2`.
  - Functions and executables placed on julia ran on julia, and those placed on haskell ran
    on haskell.
  - Dragon numbered julia as node 0, and the client ran there. So asyncflow's node index must
    come from Dragon's node order, not from Slurm's.
  - The 3-task workflow took 4.0 s.
- **Gotcha:** without `--gres`, Dragon reports `gpus=[0]` on haskell and julia, while
  `nvidia-smi` in the job says "No devices were found". `Node.gpus` does not follow Slurm's
  device cgroup. A GPU placement in such a job passes the placement check but cannot reach a
  GPU, so always request `--gres` when tasks use GPUs.

## Recipe

Environment, already built on rust (Python 3.11.4 from EESSI on `/cvmfs`, visible on the
compute nodes; the system Python is 3.13):

```bash
PY=/cvmfs/software.eessi.io/versions/2023.06/compat/linux/x86_64/usr/bin/python3.11
$PY -m venv ~/tmp/dragon-rust/venv
~/tmp/dragon-rust/venv/bin/pip install "dragonhpc==0.14.1" "rhapsody-py[dragon]==0.6.0" "radical.asyncflow==0.6.0"
```

Files on rust in `/home/mturilli/tmp/dragon-rust/`:
- `rust-dragon.sbatch`: the job script below.
- `dragon_affinity_cap.py`: the shim for released RHAPSODY.
- `shm_cleanup.sh`: the scoped `/dev/shm` cleanup that the job scripts run on each node.
- `rhapsody-branch/src`: the RHAPSODY fix branch.
- `rhapsody-merged/src`: placement + RHAPSODY fix.
- `dragon-src`: Dragon `ae8af5e`, built, with its own `_env`. It is rebuilt by
  `build_dragon.sh`, and its `COMMIT` file names the commit.
- Test scripts:
  - `af3.py`, `raw_batch.py`, `check_placement.py` and `common.py`.
  - `job.sh` adds a `ps`/affinity sampler and cleanup, and takes `DRAGON_TREE`,
    `RHAPSODY_SRC` and `CAP_TO_AFFINITY`.
  - `pytest_job.sh` runs RHAPSODY's tests, and `dragon_test_job.sh` runs Dragon's Batch
    tests.
- Logs in `logs/`, and per-run directories with the samples in `runs/<jobid>/`.

Usage. Without `DRAGON_TREE`, every line runs the stock dragonhpc 0.14.1 wheel:

```bash
sbatch rust-dragon.sbatch workflow.py                       # 64 CPUs on haskell, RHAPSODY fix
RHAPSODY_SRC=/home/mturilli/tmp/dragon-rust/rhapsody-merged/src \
ASYNCFLOW_SRC=/path/to/radical.asyncflow/src \
  sbatch -p gpu --gres=gpu:2 -c 120 rust-dragon.sbatch workflow.py   # placement + GPUs
sbatch -N2 -w haskell,julia --ntasks-per-node=1 -c 60 rust-dragon.sbatch workflow.py
RHAPSODY_SRC= sbatch rust-dragon.sbatch workflow.py         # 0.6.0: workflow imports the shim
DRAGON_TREE=/home/mturilli/tmp/dragon-rust/dragon-src RHAPSODY_SRC= \
  sbatch rust-dragon.sbatch workflow.py                     # patched Dragon, stock RHAPSODY
```

Building a Dragon branch on rust: rsync the worktree, without `.git`, to
`/home/mturilli/tmp/dragon-rust/dragon-src/`, write its commit into `dragon-src/COMMIT`, then
`sbatch /home/mturilli/tmp/dragon-rust/build_dragon.sh` (about 9 minutes on 8 CPUs). Then
install the pure-Python packages into its `_env`:
`dragon-src/_env/bin/pip install rhapsody-py==0.6.0 radical.asyncflow==0.6.0`, without the
`dragon` extra.

`rust-dragon.sbatch`:

```bash
#!/bin/bash
# RHAPSODY + Dragon on the rust cluster. Usage: sbatch rust-dragon.sbatch <workflow.py> [args]
#
# - QOS "normal" caps each user at 128 CPUs in total, so whole nodes (--exclusive, 512
#   CPUs) never start. Ask for a slice; Dragon's Batch must then be sized to that slice:
#   use RHAPSODY with fix/dragon-affinity-sizing (on PYTHONPATH below), import
#   dragon_affinity_cap in the workflow, or run a Dragon built from
#   fix/batch-affinity-sizing (DRAGON_TREE=...). Without one of them, Batch puts 256
#   workers on the slice and tasks take minutes (radical-cybertools/rhapsody#95).
# - Batch starts len(allowed CPUs) // 2 workers per node: 128 CPUs -> 64 workers.
# - GPUs: --partition=gpu --gres=gpu:2 on haskell (julia has 1). The gpu partition
#   can preempt (requeue) jobs.
# - Multi-node: --nodes=2 --nodelist=haskell,julia; CPUs count against the same 128.
#SBATCH --job-name=rust-dragon
#SBATCH --partition=cpu
#SBATCH --nodelist=haskell
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=%x-%j.out

WORKFLOW=${1:?usage: sbatch rust-dragon.sbatch <workflow.py> [args]}
shift
BASE=/home/mturilli/tmp/dragon-rust
if [ -n "$DRAGON_TREE" ]; then
  # Dragon built from source by build_dragon.sh (e.g. $BASE/dragon-src): its venv and paths.
  # libdragon.so is built with EESSI GCC 13; the compat-layer Python ships GCC 10's libstdc++.
  source "$DRAGON_TREE/_env/bin/activate"
  source "$DRAGON_TREE/devtools/VARIABLES"
  source /cvmfs/software.eessi.io/versions/2023.06/init/bash >/dev/null
  module load GCCcore/13.2.0
  export LD_LIBRARY_PATH="$EBROOTGCCCORE/lib64:$LD_LIBRARY_PATH"
  echo "Dragon: source tree $(cat "$DRAGON_TREE/COMMIT")"
else
  # Python 3.11.4 (EESSI, /cvmfs) venv: dragonhpc 0.14.1, rhapsody-py 0.6.0, radical.asyncflow 0.6.0.
  source "$BASE/venv/bin/activate"
  echo "Dragon: dragonhpc $(pip show dragonhpc 2>/dev/null | awk '/^Version/{print $2}') wheel"
fi
export PYTHONUNBUFFERED=1
# Branch checkouts go first; `radical` is a namespace package, so an asyncflow src works too.
# RHAPSODY_SRC defaults to the fix branch; set it empty to use the installed 0.6.0 release
# (then the workflow must import dragon_affinity_cap).
RHAPSODY_SRC=${RHAPSODY_SRC-$BASE/rhapsody-branch/src}
for src in "$ASYNCFLOW_SRC" "$RHAPSODY_SRC"; do
  [ -n "$src" ] && PYTHONPATH="$src${PYTHONPATH:+:$PYTHONPATH}"
done
export PYTHONPATH

# No HSTA fabric on rust (Proxmox VMs): tcp transport, and the bridge interfaces are vmbr*.
NETWORK_PREFIX='^(hsn|ipogif|ib|eth|en|nic|bond|lo|vmbr)\w*$'

# Do not share a node with another job's Dragon runtime (same user: ports, /dev/shm).
busy=$(srun --nodes="$SLURM_JOB_NUM_NODES" --ntasks="$SLURM_JOB_NUM_NODES" --cpu_bind=none bash -c \
  'for p in $(pgrep -u $USER -f "dragon-(backend|localservices|globalservices|tcp)|dragon\.cli"); do grep -q "/job_${SLURM_JOB_ID}/" /proc/$p/cgroup 2>/dev/null || echo "$(hostname):$p"; done')
if [ -n "$busy" ]; then
  echo "abort: another Dragon runtime of $USER is running here: $busy"
  exit 3
fi

# Stop Dragon 2 minutes before the time limit, so the cleanup below always runs.
LEFT=$(squeue -h -j "$SLURM_JOB_ID" -o %L | awk '{d=0; t=$1; if (index(t, "-")) {split(t, a, "-"); d=a[1]; t=a[2]}
  n=split(t, p, ":"); s=0; for (i=1; i<=n; i++) s=s*60+p[i]; print d*86400+s}')
[ "${LEFT:-0}" -gt 300 ] 2>/dev/null || LEFT=1800
SECONDS=0
T0=$(date +%s)
timeout --signal=INT --kill-after=60 $(( LEFT - 120 )) \
  dragon -w slurm -t tcp --network-prefix "$NETWORK_PREFIX" "$WORKFLOW" "$@"
echo "dragon rc=$? wall=${SECONDS}s"

# Remove only this run's /dev/shm files (see shm_cleanup.sh); never dragon-cleanup,
# which also kills other jobs' Dragon runtimes of the same user on the node.
T1=$(date +%s)
srun --nodes="$SLURM_JOB_NUM_NODES" --ntasks="$SLURM_JOB_NUM_NODES" --cpu_bind=none \
  /home/mturilli/tmp/dragon-rust/shm_cleanup.sh "$T0" "$T1"
```

`dragon_affinity_cap.py`, the shim for released RHAPSODY. Import it before creating the
backend:

```python
from dragon.native.machine import Node

_num_cpus = Node.num_cpus


def _allowed_cpus(node):
    return min(_num_cpus.fget(node), len(node.cpus) or _num_cpus.fget(node))


Node.num_cpus = property(_allowed_cpus)
```

Notes for the policy experiments:
- **CPU budget.** The 128-CPU QOS limit covers all your running jobs together. With a
  128-CPU job running, the next one queues.
- **Pool size.** Batch gives `allowed // 2` workers per node, so a function-task workload
  sees 64 concurrent workers at most. Placed tasks (`feature/dragon-placement`) run as their
  own Dragon processes, outside the pool.
- **Batch warning.** On small slices Batch logs `results_ddict_managers_per_pool=4 is out
  of range [1, 2]; clamping to 2`. It is harmless.
- **GPUs.** julia's single GPU is often requested by another user's job
  (`--nodelist=julia --gres=gpu:1`). Use haskell's 2 GPUs. The `gpu` partition can requeue
  jobs.
- **Cleanup: never `dragon-cleanup`.** It kills every Dragon process and deletes every
  `/dev/shm` file the user owns on the node, including another job's live runtime. On
  2026-10-25 my job 24833's `dragon-cleanup` killed rust-experiments' job 24815 this way.
  - The scripts now call `shm_cleanup.sh <start> <end>`. It removes only the `/dev/shm`
    files created during the job's own Dragon run, and skips the node if Dragon processes of
    another job are running there. Slurm's cgroup cleanup kills the job's own processes.
  - `job.sh` and `rust-dragon.sbatch` also refuse to start Dragon on a node where another job
    of the user runs one, because ports and `/dev/shm` are shared (job 24837 checks the
    final script).
- **Interface names.** The `--network-prefix` regex must include `vmbr`.

## Draft comment for rhapsody#95

Not posted.

> We traced this to CPU oversubscription. It is not in RHAPSODY's polling code.
>
> On our cluster, a `-N1 -n4` Slurm job (cons_tres + task/cgroup) holds 4 logical CPUs of a
> 512-CPU node. Dragon starts its backend with `srun --ntasks=N --cpu_bind=none`, so every
> Dragon process inherits those 4 CPUs. But `Batch()` sizes each worker pool as
> `Node.num_cpus // 2`, and `Node.num_cpus` is `os.cpu_count()`, i.e. 512
> (`dragon/workflows/batch/batch.py`, dragonhpc 0.14.1). That puts 256 workers, 2 managers
> and the Dragon runtime, about 280 processes, on 4 CPUs, and the node load average reaches
> 160–265. Dragon already has the right number: `Node.cpus`, the Local Services affinity,
> is 4.
>
> Measured with dragonhpc 0.14.1, rhapsody-py 0.6.0 and radical.asyncflow 0.6.0, on the
> issue's 3-task workflow:
>
> | Allocation | Batch pool | Raw `Batch`, 1st task | asyncflow, 3 tasks | Shutdown |
> |---|---|---|---|---|
> | `-n4` (as in the issue) | 256 workers on 4 CPUs | 111 s | 271 s | not done after 5 min |
> | `-n4`, pool sized from `Node.cpus` | 2 workers | 0.5 s | 2.5 s | 1.2 s |
> | `-c128` | 256 workers on 128 CPUs | 2.4 s | 3.5 s | 1.5 s |
> | `-c128`, pool sized from `Node.cpus` | 64 workers | 0.5 s | 2.2 s | 1.2 s |
>
> That accounts for every symptom above:
> - globalservices at 80% CPU.
> - `poll()` and DDict channel teardown taking minutes: each is a round trip among about 280
>   processes on 4 CPUs.
> - Raw Batch needing about 110 s for one task.
> - V1 working: it uses one process per task and has no pool.
> - No reproduction on Bridges-2, where jobs get whole nodes.
>
> With 0.6.0 the workflow does finish, slowly, but the shutdown still times out. Whole nodes
> are not a way out on this cluster, because the QOS caps users at 128 CPUs.
>
> The root cause is in Dragon's Batch sizing, and it is unchanged on Dragon `main`. With
> Batch sizing its pools from `min(num_cpus, len(Node.cpus))`, built from source on top of
> 0.14.1, the same `-n4` job runs the workflow in 2.2 s with stock RHAPSODY 0.6.0. We are
> raising it with the Dragon team.
>
> Until a Dragon release has that, `DragonExecutionBackend` can compare `Node.cpus` with
> `Node.num_cpus` before building `Batch`. When the affinity is smaller, it sizes the pools
> from it and logs a warning, with `size_pool_to_affinity=False` to opt out. Batch 0.14.1
> takes no worker count, so this overrides `Node.num_cpus` while `Batch()` runs. The change
> is ready on `fix/dragon-affinity-sizing`, and we can open a PR.
>
> DragonHPC/dragon#89 (tcp cleanup notifications) does not apply here. It covers channels
> that received sends from another node. The `destroy_process_local()` stall above was a
> Local Services round trip in a single-node job, slowed by the same oversubscription.
>
> Workaround for 0.6.0. Import this before creating the backend:
>
> ```python
> from dragon.native.machine import Node
>
> _num_cpus = Node.num_cpus
> Node.num_cpus = property(lambda n: min(_num_cpus.fget(n), len(n.cpus) or _num_cpus.fget(n)))
> ```

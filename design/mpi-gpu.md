# MPI with GPUs through Dragon: rust and Bridges-2

Status: 2026-11-02. Done. rust and Bridges-2 are correct on every path, in host and device
mode, with one GPU per rank.
Nothing committed or pushed. Scripts: `design/mpi-gpu/`. Kit: the mpi-launch kit at
experiments `62994e1` (`mpi_gpu.c`, `harness/mpi_case.py`, `summarize.py`), used unchanged.

**The test.** One GPU per rank. Each rank puts rank+1 in GPU memory and reduces it with
`MPI_Allreduce`, through host memory (`host`) or by handing MPI the device pointer (`device`,
which needs a CUDA-aware MPI). Four launch paths, each in its own Dragon runtime:
- `pg`: Dragon `ProcessGroup`;
- `batch`: `Batch.job`;
- `rhapsody-batch`: an AsyncFlow executable task on RHAPSODY's `DragonExecutionBackend`;
- `rhapsody-native`: the same on `DragonNativeExecutionBackend`.

Each rank is its own template with `gpu_affinity=[i]`. `pg-dev-all` drops the affinity, so each
rank sees all of its node's granted GPUs and takes its local rank's. The PMI is `pmix` on every
path, in the task's description for the RHAPSODY paths. The baseline runs without Dragon:
`srun --mpi=pmix` on rust, HPC-X's `mpirun` on Bridges-2. Times are per MPI job, from submission
to result, as the harness measures them.

## rust

### Why job 26141 had no GPU on julia

Every case in 26141 was `no-gpu`, the `srun` baseline included.
- On julia, `mpi_gpu`'s `dlopen("libcuda.so.1")` failed (`error no-libcuda`). haskell's rank
  found its GPU.
- Dragon listed no GPUs on julia (`GPUs: {'julia': []}`), so the native backend refused
  `gpu_affinity=[0]`.

Cause: the binaries and the venv's Python run under EESSI 2023.06's compat-layer loader. That
loader searches neither the host's `/usr/lib/x86_64-linux-gnu` nor its `ldconfig` cache (as in
`rust-experiments-3.md`, finding 4, for fortran).
- haskell has EESSI's `host_injections` (`/opt/eessi/nvidia`), which links the host driver into
  EESSI's search path. julia has no `/opt/eessi`. Both nodes have driver 580.126.09 at the same
  path (job 26142).
- Dragon 0.15 lists a node's GPUs by dlopening `libcudart.so` (`dragon_gpu_get_device_list` in
  `libdragon.so`). That is the CUDA *runtime*: EESSI's loader finds it on neither node, so
  Dragon would list no GPUs on haskell either. 26141's native cases failed on julia first.

Fix, with no change to the nodes (`rust-gpu-setup.sh`, job 26143):
`~/tmp/rhapsody-mpi2-gpu/nvlib/` holds links to the host's `libcuda.so.1`,
`libnvidia-ml.so.1` and `libnvidia-ptxjitcompiler.so.1`, and `libcudart.so` from PyPI's
`nvidia-cuda-runtime-cu12` 12.9.79. `mpi2g.sbatch` puts it first on `LD_LIBRARY_PATH`. With it,
Dragon lists `[0]` on each node, and `[0, 1]` on haskell with two GPUs.

### Results

EESSI 2023.06's Open MPI 4.1.6 (CUDA support built in), PMIx 4.2.6, dragonhpc 0.15.0, the kit's
RHAPSODY archive, AsyncFlow 437a10d. The venv is mpi2.sbatch's (`~/tmp/rhapsody-mpi2/venv`).
Every job was `correct`: right size and right sum on every rank. Every rank used its own GPU:
distinct PCI bus ids per job, and no `shared-gpu` verdict.

Job 26144: haskell + julia, 1 + 1 ranks, one GPU each. GPUs: haskell `0000:61:00.0` (RTX 6000
Ada), julia `0001:C5:00.0` (RTX PRO 6000 Blackwell).

| Path | host: jobs, s per job | device: jobs, s per job |
|---|---|---|
| `srun --mpi=pmix` | 3/3 correct, 2.57 | 3/3 correct, 2.33 |
| `pg` | 3/3, 4.35 | 3/3, 4.43 |
| `pg`, no affinity (`-all`) | – | 2/2, 4.59 |
| `batch` | 2/2, 4.61 | 2/2, 4.85 |
| `rhapsody-batch` | 2/2, 5.18 | 3/3, 5.13 |
| `rhapsody-native` | 2/2, 4.67 | 3/3, 4.53 |

Job 26145: haskell alone, 2 ranks on its two GPUs (`0000:11:00.0`, `0000:61:00.0`).

| Path | host | device |
|---|---|---|
| `srun --mpi=pmix` | 3/3 correct, 2.91 | 3/3 correct, 2.94 |
| `pg` | 3/3, 4.37 | 3/3, 4.45 |
| `pg`, no affinity (`-all`) | – | 2/2, 4.60 |
| `batch` | 2/2, 4.99 | 2/2, 4.86 |
| `rhapsody-batch` | 2/2, 5.03 | 3/3, 4.73 |
| `rhapsody-native` | 2/2, 4.16 | 3/3, 4.16 |

Notes:
- With `gpu_affinity` each rank saw one GPU: Dragon set `CUDA_VISIBLE_DEVICES` to that one.
  Without it, and under `srun`, each rank saw both of haskell's GPUs and took its local rank's.
- Through Dragon a job takes 4.2–5.2 s against `srun`'s 2.3–2.9 s: about 2 s more, as on CPUs
  (`mpi-check.md`). Device mode costs no more than host mode at this size.
- A Dragon runtime per case took 15–25 s to start and stop. RHAPSODY's Batch backend started in
  1.8 s, the native backend in 0.45 s.
- RHAPSODY's task result is the ranks' stderr: UCX's "failed to create UD QP" on haskell, as in
  the CPU runs. UCX then falls back to TCP.
- Cleanup: no processes and no `/dev/shm` files left, on either node, after any case.

Run directories: `rust:~/tmp/rhapsody-mpi2-gpu/runs/{26144,26145}` (`summary.md`,
`summary.json`, `gpus.txt`, `rep-1/*/out/rep-*/rank-*.txt`); logs in
`rust:~/tmp/rhapsody-mpi2-gpu/logs/d2-<job>.out`.

## Bridges-2

### How to run Dragon with MPI there

- **Dragon:** the Slurm launcher works (the fifth campaign's runs), with `srun-hetjob` first on
  `PATH` to strip the domain from Dragon's `--nodelist`. Transport: TCP over InfiniBand, one
  port per node: `--network-prefix '^(b2-)?ib0$'` (RM nodes have `ib0`, H100 nodes `b2-ib0`).
  - RHAPSODY's `docs/hpc-machines/bridges2.md` says to use `-w ssh` because Bridges-2 lacks the
    PMI hooks the Slurm launcher needs. That isn't what we found.
  - Dragon's PMIx doesn't use Slurm's PMI at all: Dragon runs its own PMIx server, which
    dlopens `libpmix.so`.
- **Slurm:** 22.05.11. `srun --mpi=list` shows `pmix` (`pmix_v2`), `pmi2` and `cray_shasta`.
- **MPI:** the only CUDA-aware MPI found is HPC-X 2.12 in NVHPC 22.9 (Open MPI 4.1.5a1, built
  with CUDA 11.7: `smcuda`, `coll/cuda`, UCX). Its ranks are PMIx 3.2.3 clients (`pmix3x`); it
  has no `s1`/`s2` components, so no PMI-2.
  - PSC's `openmpi/5.0.8-gcc13.3.1` isn't CUDA-aware (accelerator `null` only), and its module
    file says it is for the RM nodes. But it installs a standalone PMIx 5.0.8 `libpmix.so` with
    an absolute RPATH.
- **The recipe** (`b2-setup.sbatch`, `b2gpu.sbatch`):
  - ranks: HPC-X (`hpcx_load`), built with `OMPI_CC=gcc`, because NVHPC's wrapper defaults to
    `nvc`;
  - Dragon's PMIx server: a folder holding only `libpmix.so`, linked to 5.0.8's, first on
    `LD_LIBRARY_PATH`. Dragon takes 5.0.8, and the ranks still find HPC-X's `libpmix.so.2`;
  - `PMIX_MCA_gds=hash`: PMIx 5's server offers its `shmem2` data store, which 3.2.3 clients
    don't have, and every rank's `pmix_init` failed on it (dry run 50120151);
  - Dragon's GPU listing: NVHPC's CUDA 11.7 `libcudart.so`, on `LD_LIBRARY_PATH`. The V100
    driver is 580.178.04;
  - venv: dragonhpc 0.15.0, the kit's RHAPSODY archive and AsyncFlow 437a10d, on the fifth
    campaign's uv CPython 3.11. Compute nodes reach PyPI and GitHub, so setup ran in a job
    (50120023).
- **Dry run without GPUs** (RM-shared, 2 ranks, 50120580): on all four paths the HPC-X ranks
  wired up through Dragon's PMIx (`rank r of 2`) and reached the GPU check. There they exited 5,
  as designed for a node without a GPU. `mpirun` did the same.

### What didn't work

- **`srun --mpi=pmix` hangs** with HPC-X's ranks: no rank file in 90 s, also with
  `PMIX_MCA_gds=hash`. Its steps then outlived `scancel` and the job's end. Nodes r036 and r283
  stayed in `completing` after jobs 50120151 and 50120580 ended. No process of mine was left on
  r283: it is Slurm's own step cleanup. The GPU job therefore skips `srun` (`SRUN_BASELINE=1`
  turns it back on), and its baseline is HPC-X's `mpirun --oversubscribe`.
- `mpirun` without `--oversubscribe` refused 2 ranks: the job has one task per node.
- **The first GPU job (50120751) failed on every Dragon path.** Untyped `--gpus-per-node=8` got
  two H100 nodes (w007, w008). Their InfiniBand ports are `b2-ib0` to `b2-ib7`, so the RM nodes'
  pattern `^(ib)\w*$` matched none: Dragon stopped at its network configuration ("No NICs found
  ... with regex pattern"), 1 to 8 s per case. The `mpirun` baselines ran and were correct.
  The pattern is now `^(b2-)?ib0$`; rerun as 50125481. It cost about 1.9 GPU SU (3.6 min, 16
  H100s at 2 SU per GPU-hour).
- **GPU-shared is backlogged:** about 1,500 jobs pending, start estimates near Nov 7 for every
  GPU type. So the test runs on the GPU partition: 2 whole nodes, 8 GPUs each, 2 + 2 ranks, so
  that ranks share a node and span nodes. Untyped `--gpus-per-node=8` was estimated to start
  around 23:00 EST on Nov 2; `v100-32` alone, Nov 3 07:00. It started at 07:47 on Nov 2, on
  H100 nodes, which cost twice the V100 rate.
- The fifth campaign's held job 49821597 is named `d2` and blocks `-J d2 -d singleton`. It was
  left alone; these jobs use `-J d2g -d singleton`.
- Bridges-2 gives each job its own `/dev/shm`, owned by the user. The cleanup's `find` matched
  the folder itself (harmless: removing it failed); `-mindepth 1` now.

### Results

Job 50125481 (`-p GPU --gpus-per-node=8 -N 2 -c 40 -t 20`, RPN=2): two H100 nodes, w007 and
w008, 2 + 2 ranks, 5 min 58 s in all. Every job on every path is correct (sum 10, want 10, in
each rank's file), and each rank ran on its own GPU: on both nodes the two ranks used bus ids
0000:0A:00.0 and 0000:18:00.0. With Dragon's GPU affinity each rank sees 1 GPU; with `-all` and
with `mpirun` each sees 8 and picks by local rank.

Seconds per job (median of 2 or 3):

| Path | host | device |
|---|---|---|
| `mpirun` (HPC-X) | 8.80 | 8.55 |
| `pg` | 5.97 | 7.46 |
| `pg`, all GPUs visible | – | 7.25 |
| `batch` | 7.03 | 7.32 |
| `rhapsody-batch` | 7.11 | 8.69 |
| `rhapsody-native` | 6.56 | 7.64 |

- Unlike on rust, a job through Dragon is faster than the baseline. `mpirun` starts its daemons
  through `srun` on both nodes for every job; Dragon's runtime is already up. The runtime itself
  takes 22 to 35 s per case (wall time with 2 or 3 jobs).
- Device mode costs about 1 s more than host mode on the Dragon paths, not on `mpirun`. Two or
  three jobs per case, and single jobs vary by up to 4 s (`rhapsody-batch` device: 5.5 to
  9.5 s), so this is a hint at most.
- Backend start: Batch 1.5 to 2.2 s, native 0.35 s.
- Cleanup: no Dragon process and no `/dev/shm` file left on either node. Three cases left one
  Slurm step each, which the script cancelled.

Run directories: `/ocean/projects/dmr170002p/mturilli/mpi-gpu/runs/50125481` (`mpirun/` holds
the baseline), log `logs/d2g-50125481.out`; the failed 50120751 beside it.

Remote layout: `/ocean/projects/dmr170002p/mturilli/mpi-gpu/`: `venv/`, `pmixlib/`, `kit/`
(the kit's three files and the RHAPSODY archive), `runs/<job>/`, `logs/`.

SU used (from `sacct`; PSC's `projects` didn't answer): RM-shared 5.2 (setup and three dry
runs, 16 cores); GPU 5.1 (9.6 min of 16 H100s at 2 SU per GPU-hour, both GPU jobs).

## Proposed kit changes (not committed)

In the worktree `~/github/radical/experiments-mpi-gpu`, branch `topic/mpi-gpu` from `62994e1`.
Untested through `./kit`; only `./kit run bridges2 mpi-launch/gpu-b2 --dry-run` was checked.
- `machines/bridges2.sh` (new): the recipe above, with `--network-prefix '^(b2-)?ib0$'`. RM,
  GPU (whole nodes) or GPU-shared from
  `KIT_CONSTRAINT`/`KIT_GPUS_PER_NODE`; `GPU_TYPE` optional; `JOB_NAME=radical-kits`,
  singleton.
- `machines/bridges2-bin/srun` (new): the Dragon `srun` shim (short host names, `--overlap`).
- `kits/mpi-launch/run.sh`: `bridges2` defaults. The baseline is `mpirun` there, not `srun`,
  with `--oversubscribe --bind-to none -x …`. Over several nodes `mpirun` gets
  `--map-by ppr:<n/nodes>:node`; three, with one node, is unchanged.
- `kits/mpi-launch/presets/gpu-b2.env` (new): 2 + 2 ranks on two GPU nodes, the nine cases.
- `kits/mpi-launch/kit.env`: `bridges2` in `KIT_MACHINES`. `README.md`: the Bridges-2 column
  and a note on rust's GPU libraries.
- `tools/dragonlib.sh`, `kits/mpi-launch/run.sh`: `find /dev/shm -mindepth 1 …`. On Bridges-2
  `/dev/shm` itself belongs to the job's user and matched.

As text only (`summarize.py` is read by `analysis/`):
- `summarize.py` keys a baseline by ranks and mode, so two launchers at the same size merge. A
  key that includes the launcher would let `srun` and `mpirun` sit side by side.
- rust: the GPU recipe (`nvlib/` and `cudart/`) belongs in whatever runs GPU kits there:
  `mpi2.sbatch`, or a rust machine profile's `activate_env` for GPU jobs.

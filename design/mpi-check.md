# MPI through Dragon: one node on three, two nodes on rust

2026-11-01. Can Dragon launch an Open MPI program, directly and through Batch, before an MPI kit
is built for Perlmutter? No campaign had run an MPI task on any site or backend (the MPI row of
the experiments repo's `d2_coverage` table).

**Test program.** `mpi_hello.c`: each rank writes its rank, the size it sees, its host and an
`MPI_Allreduce` of rank+1, and exits 4 unless the size is the one expected. Without a working
PMI every rank starts alone as rank 0 of 1, so a wrong launch can't pass. With `fail`, rank 1
exits 7 right after `MPI_Init` and the others wait for it in the Allreduce. Scripts are in
`design/mpi-check/`.

## Results

### three, one node

Ubuntu 26.04.1, 64 CPUs, dragonhpc 0.15.0 (pip, system Python 3.12), Open MPI 5.0.10 with the
system PMIx 5.0.9. Compiled against the 5.0.10 headers from the Ubuntu archive
(`libopenmpi-dev`, SHA-512 checked, unpacked in the test folder; nothing installed). One run per
case. `run_case.sh` and the `*_check.py` drivers.

| Launch | PMI | Result | Time |
|---|---|---|---|
| `mpirun -np 4`, no Dragon | | 4 ranks, size 4, correct | 0.38 s |
| `ProcessGroup` | `pmix`, as installed | runtime aborts: "Unable to initialize PMIx server"; 5 processes and 6 `/dev/shm` files left | |
| `ProcessGroup` | `pmix`, `libpmix.so` on `LD_LIBRARY_PATH` | 4 and 16 ranks, correct | 2.63 s |
| `ProcessGroup` | `cray` | no error; 4 separate jobs of 1 rank each | 1.10 s |
| `ProcessGroup` | none | the same | 1.09 s |
| `Batch.job` | default (`cray`), RHAPSODY's path | no error; 4 jobs of 1 rank; the task returns `[4, 4, 4, 4]` | 1.50 s |
| `Batch.job` | none | the same | 1.47 s |
| `Batch.job` | `pmix`, as installed | runtime aborts as above; 23 processes and 14 `/dev/shm` files left | |
| `Batch.job` | `pmix`, `libpmix.so` on `LD_LIBRARY_PATH` | 4 and 16 ranks, correct | 3.22 / 3.36 s |
| `Batch` process task running `mpirun -np N` | (Open MPI's own) | 4 and 16 ranks, correct; Batch sees one process | 1.56 / 1.69 s |

### rust, two nodes

haskell and julia (partition `cpu`), 16 CPUs on each, dragonhpc 0.15.0 in
`~/tmp/dragon-native-pool/venv` (EESSI 2023.06 Python 3.11.4), Dragon over TCP as in
`run2n.sbatch`. Open MPI 4.1.6 with PMIx 4.2.6 from EESSI 2023.06 (generic x86_64) on both
nodes: julia has no system Open MPI, haskell has the system's 5.0.7. `mpi2n.sbatch` runs one
Dragon runtime per case; jobs 26081, 26082 and 26084, plus 26085 for the dead-rank baseline.
One run per case unless a range is given. All runs in the table are correct and clean unless
stated.

| Launch | Placement | Result | Time per job |
|---|---|---|---|
| `srun --mpi=pmix`, no Dragon | 4 + 4 | correct | 3.45–3.69 s (3 runs) |
| `ProcessGroup`, `pmix` | 4 + 4, by host name | correct | 5.38–6.26 s (2 runs) |
| `ProcessGroup`, `pmix` | Dragon's own | 4 + 4 | 5.16 s |
| `ProcessGroup`, `pmix`, 16 ranks | 8 + 8 | correct | 7.55 s |
| `Batch.job`, `pmix` | 4 + 4, by host name | correct | 6.58–6.76 s (2 runs) |
| `Batch.job`, `pmix` | Batch's own | all 8 on julia | 4.91 s |
| `Batch.job`, `pmix`, 8 jobs of 4 at once | Batch's own | all correct; the jobs alternate nodes | 12.39 s for all 8 |
| Dragon's own steps left on Slurm's default `--mpi=pmix` | 4 + 4 | correct, both ways | 5.45 / 6.07 s |
| `srun --mpi=pmix`, rank 1 dies | 4 + 4 | Slurm aborts the step: the other 7 killed | 4.1 s |
| `ProcessGroup` / `Batch.job`, `pmix`, rank 1 dies | 4 + 4 | the other 7 wait until the 30 s limit; exit codes 7 and -2 | 32.7 / 33.3 s |
| `Batch.job`, default (`cray`) | 4 + 4 | every rank exits 1 at once, no output | 1.62 s |
| `Batch` process task running `mpirun` | | exit 1; not investigated | 2.11 s |

Times run from submitting the group or Batch task to its result, inside a runtime already
started (Dragon's start and stop add 10–20 s per runtime on rust).

Getting there (jobs 26070–26077):
- **julia has no Open MPI** (job 26070): ranks there could not load `libmpi.so.40`. The test
  moved to EESSI's Open MPI, the same build on every node.
- **EESSI 2025.06's PMIx did not load** (jobs 26072–26077): every `pmix` case aborted its
  runtime with the same "Unable to initialize PMIx server". The venv's Python runs under EESSI
  2023.06's own loader and C library. That loader does not search the host's `/usr/lib`, so
  haskell's system `libpmix.so` was not found, and 2025.06's `libpmix.so` needs `GLIBC_2.38`,
  which the 2023.06 C library lacks. A `ctypes` probe in that Python showed both. EESSI
  2023.06's PMIx 4.2.6 loads, and with it everything works.
- In those aborted runs the cleanup after each case cancelled a Slurm step the runtime left
  behind (26072.17, .21, .29; 26074.9; 26075.3). This is the case the kit's cleanup (fc2e0c2)
  handles.

## Findings

1. **Dragon's PMIx path works across nodes,** through `ProcessGroup` and `Batch.job`, at 4,
   8 and 16 ranks and with 8 jobs at once. Each launch costs 1.2–3.3 s more than
   `srun --mpi=pmix` for the same 8 ranks (4.9–6.8 s against 3.5–3.7 s). That is one sample per
   setting on one cluster, with Open MPI's UCX failing on haskell's RDMA device and falling
   back to TCP in every run, Dragon's and `srun`'s alike.
2. **Dragon finds PMIx by `dlopen("libpmix.so")` in its own Python process.** So it needs the
   unversioned name, which distributions ship only in `libpmix-dev` (three), and a
   `libpmix.so` that Dragon's Python can load (rust: one from the same EESSI version, since
   that Python neither searches the host's libraries nor has a new enough C library).
   `dragon-config` can name the CUDA, HIP and Level Zero libraries, but not PMIx.
3. **Any failure there is reported as the same generic error, and it aborts the whole
   runtime.** `pydragon_dpmix.pyx` raises "Unable to initialize PMIx server for launch of MPI
   application" and drops the C layer's reason, such as "Unable to open libpmix.so for PMIx
   code execution". Local services then end with "Abnormal Termination", so everything else in
   the runtime dies too, and processes, `/dev/shm` files and (under Slurm) job steps are left
   behind.
4. **A dead rank does not end its MPI job under Dragon.** When one rank exits right after
   `MPI_Init`, the other ranks wait in their collective until the group's walltime or the
   Batch timeout (30 s here). With no limit they would presumably wait indefinitely. Under
   `srun --mpi=pmix` Slurm's PMIx server sees the exit and aborts the step, so the job ends in
   about a second.
5. **Batch's PMI default is Cray on every platform, and a wrong PMI fails in different ways.**
   With Open MPI 5 (three) every rank runs as a one-rank job: no error, no warning, and only
   the program's own size check caught it. With Open MPI 4.1 (rust) every rank exits 1 at
   once. RHAPSODY's Dragon backend passes no PMI (`batch.options()` gets name, timeout and
   stdio only), so every multi-rank job it submits gets `cray`. That is wrong on rust and right
   on Perlmutter (Cray MPICH).
6. **Placement differs:** `ProcessGroup` spreads a job's ranks over the nodes, and `Batch.job`
   keeps a job on one node when it fits (all 8 ranks on julia), alternating nodes between jobs.
7. Starting `mpirun` as one Batch process task works on one node (three, Open MPI 5). Batch then
   counts the job as one process however many ranks run. Across rust's nodes with Open MPI
   4.1.6 it failed at once; not investigated.
8. Open MPI leaves a 16 MiB `sm_segment.*` file in `/dev/shm` for every job it starts without
   `mpirun` (three). This is Open MPI's own behaviour, not Dragon's.
9. Slurm's default `--mpi=pmix` on rust gives Dragon's own processes Slurm's PMIx client
   variables (`PMIX_NAMESPACE`, `PMIX_RANK`, ...). That did not stop Dragon's PMIx server
   (job 26084), so `--mpi=none` is not needed.

## For the next steps

- **Perlmutter:** Cray MPICH with `pmi=cray`, Batch's default, so RHAPSODY's current path is
  tested as it is. The same cases as rust: placement, 16 ranks, concurrent jobs, a dead rank,
  launch time against `srun`.
- **An MPI kit** in the experiments repo from `mpi_case.py` and `mpi2n.sbatch`, with the site's
  MPI and PMI as kit settings.
- **RHAPSODY:** pass the PMI through (a task-level `pmi`, or a backend default), and refuse or
  warn on a multi-rank job when the PMI can't be right for the site.
- **Dragon report:** findings 2–5 are candidates. 3 is high by the report's definition (it
  kills work other than the failing task).
- Not tested: finding 39 (a job with more ranks than Batch's capacity) on 0.15, since Dragon
  sees 512 CPUs per rust node and the test kept to 16; MPI from Python (mpi4py); GPUs.

## RHAPSODY's job PMI on rust (job 26108)

2026-11-01. The branch `feature/dragon-job-pmi` of RHAPSODY (local, 8607783) passes a PMI to
Batch jobs. It ran on haskell and julia through AsyncFlow 437a10d, with the experiments repo's
mpi-launch harness at a030f92 (its `pmi` preset with rust's PMI, pmix) and the same EESSI
2023.06 Open MPI 4.1.6 and PMIx 4.2.6. `mpi-check/pmi2n.sbatch`, venv by
`mpi-check/pmi2n-setup.sh`; run directory `rust:~/tmp/rhapsody-pmi/runs/26108/`.

| Case | Ranks | Result | Time per job | RHAPSODY warned of |
|---|---|---|---|---|
| no PMI (Batch's default, cray) | 4 + 4 | FAILED at the 120 s timeout: Open MPI's "A requested component was not found"; no rank wrote | 121.8 s | no PMI |
| task `pmi: pmix` | 4 + 4 | correct, 3 of 3 | 5.7-6.0 s | - |
| `DragonExecutionBackend(pmi="pmix")` | 4 + 4 | correct, 2 of 2 | 4.7-5.7 s | - |
| task `pmi: pmix` | 8 + 8 | correct, 2 of 2 | 6.8-7.1 s | - |
| Python function job, no PMI | 2 + 2 | DONE, 3 of 3 | 1.1-2.0 s | no PMI |
| Python function job, `pmi: none` | 2 + 2 | DONE, 3 of 3 | 1.1-1.9 s | - |
| task `pmi: pmix`, rank 1 dies | 4 + 4 | FAILED at the 30 s timeout | 33.3 s | - |

- `srun --mpi=pmix`: 8 ranks in 3.3-4.1 s.
- Through RHAPSODY the no-PMI job didn't fail at once, as `Batch.job` did when called directly
  (every rank exited 1 in 1.6 s): it ran to its timeout. Its second run was cut off by the
  case's 300 s limit (Dragon rc 124). One sample.
- The non-MPI function job that crashed on three with Batch's default PMI ("double free or
  corruption", 8 of 8) ran here, so that crash depends on the host.
- Cleanup: no processes or `/dev/shm` files left on either node.

## MPI on the native backend, from Python, and with GPUs (2026-11-02)

What the earlier checks left out:
- **MPI on RHAPSODY's native backend:** its `ProcessGroup`s had no PMI.
- **MPI from Python:** mpi4py processes, and AsyncFlow function tasks that use mpi4py.
- **MPI with GPUs:** one GPU per rank.

The pieces:
- **RHAPSODY:** the native backend's groups take a PMI, `DragonNativeExecutionBackend(pmi=...)`
  or the task's `"pmi"`, with the Batch backend's two one-time warnings. That is the local
  branch `feature/dragon-native-mpi` (off 88599b7; not committed yet).
- **The mpi-launch kit** (experiments repo, not committed yet):
  - the presets `native`, `mpi4py` and `gpu`;
  - `harness/mpi4py_hello.py`, and `mpi_gpu.c`, which loads the CUDA driver with `dlopen` and
    reduces rank+1 from device memory through host memory (`host`) or by handing MPI the device
    pointer (`device`);
  - a RHAPSODY archive of 757465d's tree plus the native backend's files
    (`vendor/rhapsody-native-mpi-pmi-757465d/`).

### three, one node (Open MPI 5.0.10, PMIx)

Runs `three:~/tmp/radical-runs/20261102T065238Z-three-mpi-launch-native-bef2e2f`,
`...T065355Z-...-mpi4py-...` and `...T065637Z-...` (the GPU code path), with mpi4py 4.1.2's
PyPI wheel. The kit at bef2e2f plus the uncommitted changes, from
`three:~/tmp/radical-experiments-mpi2`.

| Case | Ranks | Result | Time per job | Warned of |
|---|---|---|---|---|
| native, task `pmi: pmix` | 4 + 4 | correct, 3 of 3 | 2.66 s | - |
| native, `DragonNativeExecutionBackend(pmi="pmix")` | 4 + 4 | correct, 2 of 2 | 2.65 s | - |
| native, task `pmi: pmix` | 8 + 8 | correct, 2 of 2 | 2.7-3.0 s | - |
| native, no PMI | 4 + 4 | singletons; FAILED, exit code 4 | 1.1 s | no PMI |
| native, rank 1 dies | 4 + 4 | FAILED at the 30 s timeout | 32.4 s | - |
| mpi4py processes, `ProcessGroup`, PMIx | 4 + 4 | correct, 3 of 3 | 2.64 s | - |
| mpi4py function task, Batch backend, `pmi: pmix` | 4 + 4 | correct, 3 of 3 | 3.1-3.9 s | - |
| mpi4py function task, native backend, `pmi: pmix` | 4 + 4 | correct, 3 of 3 | 3.0-3.3 s | - |
| the same two at 8 + 8 | 8 + 8 | correct, 2 of 2 each | Batch 3.3-4.1 s, native 3.3 s | - |
| mpi4py function task, Batch backend, no PMI (cray) | 4 + 4 | singletons; FAILED ("Exception Occurred in User-Provided Code") | 2.3 s | no PMI |
| mpi4py, rank 1 dies | 4 + 4 | FAILED at the 30 s timeout | Batch 38.1 s, native 32.4 s | - |
| `mpi_gpu`, every launch path (three has no GPU) | 2 + 2 | no-gpu: every rank exited 5 at once | 2.6-3.3 s | - |

- `mpirun`: 8 ranks in 0.4 s.
- Nothing was left behind after any case.

### rust, two nodes (job 26140; Open MPI 4.1.6, PMIx 4.2.6)

haskell and julia, 16 CPUs each, 8 workers per node; dragonhpc 0.15.0 over TCP, mpi4py 4.1.2's
PyPI wheel with EESSI's `libmpi.so.40`. `mpi-check/mpi2.sbatch` (CPU mode), venv by
`mpi-check/mpi2-setup.sh`. Run directory: `rust:~/tmp/rhapsody-mpi2/runs/26140/`.

| Case | Ranks | Result | Time per job | Warned of |
|---|---|---|---|---|
| native, task `pmi: pmix` | 4 + 4 | correct, 3 of 3 | 5.8 s (median) | - |
| native, `DragonNativeExecutionBackend(pmi="pmix")` | 4 + 4 | correct, 2 of 2 | 5.3 s | - |
| native, task `pmi: pmix` | 8 + 8 | correct, 2 of 2 | 6.0 s | - |
| native, no PMI | 4 + 4 | FAILED at the 120 s timeout; no rank wrote; 4 ranks exited 1, the other 4 were killed | 121.0 s | no PMI |
| native, rank 1 dies | 4 + 4 | FAILED at the 30 s timeout | 32.6 s | - |
| mpi4py processes, `ProcessGroup`, PMIx | 4 + 4 | correct, 3 of 3 | 5.6-5.8 s | - |
| mpi4py function task, Batch backend, `pmi: pmix` | 4 + 4 | correct, 3 of 3 | 5.2-6.9 s | - |
| mpi4py function task, native backend, `pmi: pmix` | 4 + 4 | correct, 3 of 3 | 5.7-6.4 s | - |
| the same two at 8 + 8 | 8 + 8 | correct, 2 of 2 each | Batch 7.7-8.1 s, native 7.3-7.6 s | - |
| mpi4py function task, Batch backend, no PMI (cray) | 4 + 4 | FAILED at the 120 s timeout with Open MPI's error banner; no rank wrote | 126.6 s | no PMI |
| mpi4py, rank 1 dies | 4 + 4 | FAILED: Batch 38.5 s (a UCX "Failed to create UCP worker" error), native at the 30 s timeout | Batch 38.5 s, native 32.4 s | - |

- `srun --mpi=pmix`: 8 ranks in 3.7 s (median of 3). Through Dragon a launch costs about 2 s
  more, as in job 26108.
- Every MPI rank on haskell logs UCX's "failed to create UD QP" and falls back to TCP, as in
  the earlier runs (finding 1).
- Both no-PMI cases outlived their task: the runtime ran to the case's 300 s limit and was
  interrupted. On the native backend AsyncFlow's shutdown had returned (0.2 s), so Dragon's
  own teardown waited about 185 s; on the Batch backend the shutdown never returned. Job 26108's
  no-PMI case did the same.
- Cleanup: no processes or `/dev/shm` files left on either node.
- The GPU job that followed (26141) found no usable GPU on julia: `libcuda.so.1` didn't load
  there, and Dragon listed no GPUs for julia. The `mpi-gpu` session took it over.

### Perlmutter, two CPU nodes (jobs 60679775 and 60679776; Cray MPICH, Cray PMI)

The kit at 62994e1 (pushed), presets `native` and `mpi4py`, run by Aymen on 2026-11-02. Run
directories `three:/srv/radical-runs/perlmutter/20261102T073943Z-perlmutter-mpi-launch-native-62994e1`
and `...T073944Z-...-mpi4py-62994e1`. mpi4py is Cray Python's 4.1.1.

| Case | Ranks | Result | Time per job | Warned of |
|---|---|---|---|---|
| native, task `pmi: cray` | 4 + 4 | correct, 3 of 3 | 1.01 s (median) | - |
| native, `DragonNativeExecutionBackend(pmi="cray")` | 4 + 4 | correct, 2 of 2 | 1.03 s | - |
| native, task `pmi: cray` | 8 + 8 | correct, 2 of 2 | 1.01 s | - |
| native, no PMI | 4 + 4 | FAILED at the 120 s timeout; no rank wrote | 121.3 s | no PMI |
| native, rank 1 dies | 4 + 4 | FAILED at the 30 s timeout | 31.3 s | - |
| mpi4py processes, `ProcessGroup`, Cray PMI | 4 + 4 | correct, 3 of 3, every rank exit 0 | 1.0-1.3 s | - |
| mpi4py function task, Batch backend, `pmi: cray` | 4 + 4 | every rank wrote a correct result, then the task FAILED: "malloc_consolidate(): invalid chunk size" from every rank, 3 of 3 | 1.7-3.3 s | - |
| the same, native backend | 4 + 4 | the same: correct results, then exit code -6 with that message, 3 of 3 | 1.6-2.3 s | - |
| the same two at 8 + 8, and Batch with no PMI (Batch's cray) | 8 + 8; 4 + 4 | the same | 2.0-3.0 s | no PMI (Batch, unset) |
| mpi4py, rank 1 dies | 4 + 4 | FAILED: Batch 37.2 s ("failed with unknown error"), native at the 30 s timeout | Batch 37.2 s, native 31.3 s | - |

- `srun --mpi=cray_shasta`: 8 ranks in 0.8-1.3 s. The native backend's job (1.0 s) is as fast
  as `srun`, and faster than the Batch backend's in the `pmi` run (1.1-2.7 s).
- **mpi4py in function tasks crashes on Cray MPICH, on both backends,** after the work is done:
  glibc aborts at teardown with "malloc_consolidate(): invalid chunk size". The same mpi4py in a
  script that Dragon starts as an executable exits cleanly, and so did function tasks with
  Open MPI on three and rust. Not yet known: whether it is mpi4py's `MPI_Finalize` at exit,
  Python's teardown, or Dragon's function-process wrapper, and whether a Dragon `ProcessGroup`
  with a Python function target does it too. Three's crash of non-MPI function jobs with Batch's
  Cray PMI ("double free or corruption") is also heap corruption in a Dragon function process.
- The no-PMI native case ended its runtime at 138 s here: no long teardown, unlike rust.
- Cleanup: nothing found or left after any case.

## Files

`design/mpi-check/`: `mpi_hello.c`; three's drivers `pg_check.py`, `batch_check.py`,
`batchmpirun_check.py` and `run_case.sh <pg|batch|batchmpirun> <pmi> <ranks>`; rust's driver
`mpi_case.py` and job `mpi2n.sbatch` (settings in its header); the RHAPSODY PMI job `pmi2n.sbatch` and its venv's `pmi2n-setup.sh`. Run directories:
three `~/tmp/mpi-check/case-*` (`case-*-nolib` hold the two aborted runs);
rust `~/tmp/mpi-check/runs/<job>/`, logs in `~/tmp/mpi-check/logs/`.

Cleanup on both hosts:
- **Processes:** only processes of this user whose working directory was the case's were ended.
- **`/dev/shm`:** only files named in Dragon's exit warning (three), or made since the case or
  job started on a node where no other job of this user ran (rust), were removed.
- **Left in place:** sixteen 32–40-byte files of this user on haskell, from 2026-10-28, which
  belong to another session.

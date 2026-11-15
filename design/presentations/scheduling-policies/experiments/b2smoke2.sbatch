#!/bin/bash
# Bridges-2 smoke test 2 (fifth campaign): one Dragon runtime over two RM nodes.
#   sbatch -A dmr170002p -J d2 -d singleton -p RM -N 2 --ntasks-per-node=1 -c 128 -t 15 b2smoke2.sbatch
# Dragon's own srun on a plain two-node job (b2run.sbatch adds only --overlap), then
# smoke_twonode.py: Batch's pool on both nodes and where 64 functions run; then one rep of
# diag_twonode.py (Batch over 1 and 2 nodes). Also lists what an HSTA build would need.
#SBATCH --output=/ocean/projects/dmr170002p/mturilli/sched-exp/logs/%x-%j.out
BASE=/ocean/projects/dmr170002p/mturilli/sched-exp
echo "== $(hostname): $SLURM_JOB_NODELIST"
ip -br addr | grep -E '^ib|^ens' ; ibv_devinfo -l 2>&1 | head -5
ls /usr/lib64 2>/dev/null | grep -E 'libfabric|libucp|libmlx5' | head
module avail libfabric ucx 2>&1 | grep -iE 'libfabric|ucx' | head
"$BASE/envs/stock/bin/dragon-config" -s 2>&1 | head -3
RUNS=$(mktemp -p "$BASE/runs" smoke2.XXXX)
cat > "$RUNS" <<EOR
smoke_twonode.py
diag_twonode.py --reps 1 --tasks 2000 --chain 100
EOR
RUNS=$RUNS RUN_TIMEOUT_S=360 MIN_LEFT_S=200 RESULTS_DIR=$BASE/results/smoke bash "$BASE/experiments/b2run.sbatch"

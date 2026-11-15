#!/bin/bash
# Which part of a heterogeneous job batch environment makes srun hang (third campaign)?
# Each form runs hostname in component 1 with a 20 s timeout.
env | grep ^SLURM_ | grep -v PACK_GROUP | sort
try() { echo "-- $*"; timeout 20 "$@" 2>&1 | sed "s/^/   /"; echo "   rc=${PIPESTATUS[0]}"; }
try srun --overlap --het-group=1 -n1 hostname
try srun --overlap --het-group=1 -n1 --gres=none hostname
try srun --overlap --het-group=1 -n1 -c1 --mem=1G --gres=none hostname
try env -u SLURM_TRES_PER_TASK -u SLURM_CPUS_PER_TASK -u SLURM_GPUS_ON_NODE -u SLURM_JOB_GPUS srun --overlap --het-group=1 -n1 hostname
try env -i PATH=/usr/bin:/bin HOME=$HOME SLURM_JOB_ID=$SLURM_JOB_ID srun --overlap --het-group=1 -n1 hostname
try env -i PATH=/usr/bin:/bin HOME=$HOME SLURM_JOB_ID=$SLURM_JOB_ID srun --overlap --het-group=0-1 --nodes=1 --ntasks=1 -l hostname

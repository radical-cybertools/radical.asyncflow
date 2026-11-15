#!/bin/bash
# Bridges-2 smoke test 1 (fifth campaign): one V100 in GPU-shared, one Dragon runtime.
#   sbatch -A dmr170002p -J d2 -d singleton -p GPU-shared --gpus=v100-32:1 -N 1 --ntasks-per-node=1 -c 5 -t 15 b2smoke1.sbatch
# 1. the GPU the job holds (nvidia-smi) and gpu_task.py --calibrate outside Dragon: kernel rate
#    and the memory one CUDA context takes on a V100;
# 2. b2run.sbatch with two short E2 runs on that GPU through AsyncFlow + RHAPSODY: node-mode
#    placement with declared memory, then learned memory (observe_peaks, NVML). Both stop unless
#    Dragon lists exactly one GPU (so a placement on GPU 0 is the job's own GPU).
#SBATCH --output=/ocean/projects/dmr170002p/mturilli/sched-exp/logs/%x-%j.out
BASE=/ocean/projects/dmr170002p/mturilli/sched-exp
echo "== $(hostname) CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES SLURM_JOB_GPUS=$SLURM_JOB_GPUS"
nvidia-smi -L
nvidia-smi --query-gpu=index,pci.bus_id,name,memory.total,driver_version --format=csv
srun --ntasks=1 --cpu_bind=none nvidia-smi -L
for i in 1 2 3; do "$BASE/envs/stock/bin/python" "$BASE/experiments/gpu_task.py" --calibrate; done
RUNS=$(mktemp -p "$BASE/runs" smoke1.XXXX)
COMMON="--rep 0 --pipelines 4 --window 90 --mem-scale 0.667 --node-gpu-mem 30 --shapes-from-dragon --expect-gpus 1 --kind-per-tool --workers-per-node 8"
cat > "$RUNS" <<EOR
e2_gpu.py --config res-node $COMMON
e2_gpu.py --config res-learn $COMMON --unknown-per-node 2
EOR
RUNS=$RUNS RUN_TIMEOUT_S=360 MIN_LEFT_S=240 RESULTS_DIR=$BASE/results/smoke bash "$BASE/experiments/b2run.sbatch"

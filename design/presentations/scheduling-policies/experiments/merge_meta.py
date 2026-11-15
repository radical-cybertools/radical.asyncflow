"""Add the node state run.sbatch sampled to the result files of one Slurm job.

Usage: python merge_meta.py <run_dir> <results_dir> <job_id> <dragon_rc> <wall_s>

Adds ``node_state`` to every result JSON of the job: the jobs on the node at start and end
(other users' jobs are what may share its cores), the load samples (time, load average,
CPUs busy on the node, CPUs this job used) and, with GPUs, nvidia-smi's samples; for a
two-node run (run2n.sbatch), the same per host under ``per_node``. A job whose
script wrote no result gets a ``<job>_noresult.json`` with the same state, so that every
failed run stays on record.
"""

import glob
import json
import os
import sys


def read_squeue(path):
    rows = []
    if os.path.exists(path):
        for line in open(path):
            f = line.rstrip("\n").split("|")
            if len(f) in (8, 9):  # run2n.sbatch adds the node list
                rows.append(dict(zip(("job", "user", "partition", "state", "cpus", "tres", "time", "name", "nodes"), f)))
    return rows


def read_load(path):
    rows = []
    if os.path.exists(path):
        for line in open(path):
            try:
                t, load, node, me = line.strip().split(",")
                rows.append([float(t), float(load), float(node), float(me)])
            except ValueError:
                pass
    return rows


def read_gpu(path):
    rows = []
    if os.path.exists(path):
        for line in open(path):
            f = [x.strip() for x in line.split(",")]
            if len(f) == 4:
                try:
                    rows.append([f[0], int(f[1]), float(f[2]), float(f[3])])
                except ValueError:
                    pass
    return rows


def main():
    run_dir, results_dir, job, rc, wall = sys.argv[1:6]
    state = {
        "dragon_rc": int(rc),
        "job_wall_s": int(wall),
        "squeue_start": read_squeue(os.path.join(run_dir, "squeue_start.txt")),
        "squeue_end": read_squeue(os.path.join(run_dir, "squeue_end.txt")),
        "load_cols": ["epoch", "loadavg1", "node_cpus_busy", "job_cpus_busy"],
        "load": read_load(os.path.join(run_dir, "load.csv")),
    }
    tz = os.path.join(run_dir, "tz.txt")
    if os.path.exists(tz):
        state["tz"] = open(tz).read().strip()
    # run2n.sbatch (third campaign): one sampler per node, files named by host.
    per_node = {}
    for path in sorted(glob.glob(os.path.join(run_dir, "load_*.csv"))):
        host = os.path.basename(path)[len("load_"):-len(".csv")]
        node = {"load": read_load(path)}
        gpus = os.path.join(run_dir, f"gpus_{host}.txt")
        if os.path.exists(gpus):
            node["gpus"] = [line.strip() for line in open(gpus) if line.strip()]
            node["gpu"] = read_gpu(os.path.join(run_dir, f"gpu_{host}.csv"))
        per_node[host] = node
    if per_node:
        state["per_node"] = per_node
        state["gpu_cols"] = ["timestamp", "index", "util_pct", "mem_used_mib"]
    gpus = os.path.join(run_dir, "gpus.txt")
    if os.path.exists(gpus):
        state["gpus"] = [line.strip() for line in open(gpus) if line.strip()]
        state["gpu_cols"] = ["timestamp", "index", "util_pct", "mem_used_mib"]
        state["gpu"] = read_gpu(os.path.join(run_dir, "gpu.csv"))
    paths = [
        p
        for p in glob.glob(os.path.join(results_dir, f"*_{job}.json"))
        if json.load(open(p)).get("slurm_job_id") == job
    ]
    if not paths:
        p = os.path.join(results_dir, f"{job}_noresult.json")
        json.dump({"slurm_job_id": job, "node_state": state}, open(p, "w"), indent=1)
        print(f"== no result for job {job}; wrote {p}")
    for p in paths:
        rec = json.load(open(p))
        rec["node_state"] = state
        with open(p, "w") as f:
            json.dump(rec, f, indent=1)
        print(f"== node state merged into {p}")


if __name__ == "__main__":
    main()

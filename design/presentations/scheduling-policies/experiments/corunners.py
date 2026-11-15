"""Other users' jobs that overlapped each run, from Slurm accounting.

Usage (on rust): python corunners.py <results_dir>

For every result file, looks up the run's job (node, start, end) with ``sacct`` and lists
the jobs of other users on that node whose run overlapped it. Writes
``<results_dir>/corunners.json``: {job id: {"node", "start", "end", "others": [...]}}.
This covers the runs whose own node record missed jobs in hidden partitions.
"""

import glob
import json
import os
import subprocess
import sys

FIELDS = "jobid,user,partition,nodelist,alloccpus,alloctres,start,end,state"


def sacct(*args):
    out = subprocess.run(["sacct", "-X", "-P", "-n", "-o", FIELDS, *args], capture_output=True, text=True)
    return [dict(zip(FIELDS.split(","), line.split("|"))) for line in out.stdout.splitlines() if line]


def main():
    results = sys.argv[1]
    jobs = sorted({json.load(open(p)).get("slurm_job_id") for p in glob.glob(os.path.join(results, "*.json"))
                   if not p.endswith("corunners.json")} - {None})
    out = {}
    for job in jobs:
        rows = sacct("-j", job)
        if not rows:
            continue
        # A heterogeneous job (third campaign) has one row per component, each with its node.
        me = dict(rows[0], nodelist=",".join(r["nodelist"] for r in rows))
        end = me["end"] if me["end"] not in ("Unknown", "") else "now"
        others = [
            {k: j[k] for k in ("jobid", "user", "partition", "nodelist", "alloccpus", "alloctres", "start", "end",
                               "state")}
            for j in sacct("-a", "-N", me["nodelist"], "-S", me["start"], "-E", end, "-s", "R,CD,F,TO,CA,PR,NF,OOM")
            if j["user"] != me["user"] and j["start"] not in ("Unknown", "None") and j["start"] < end
            and (j["end"] in ("Unknown", "") or j["end"] > me["start"])
        ]
        out[job] = {"node": me["nodelist"], "start": me["start"], "end": me["end"], "state": me["state"],
                    "others": others}
    with open(os.path.join(results, "corunners.json"), "w") as f:
        json.dump(out, f, indent=1)
    print(f"{len(out)} jobs; with other users' jobs: {sum(1 for v in out.values() if v['others'])}")


if __name__ == "__main__":
    main()

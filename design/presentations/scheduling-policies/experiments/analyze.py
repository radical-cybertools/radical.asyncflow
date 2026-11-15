"""Tables and charts for the rust policy experiments (E1-E3).

Usage: python analyze.py <raw_dir> <out_dir>

Reads one JSON file per run (from run.sbatch and the e*_*.py scripts), and writes
``summary.md`` (every table, median [min-max] over repetitions), ``summary.json`` and the
charts as PDF and PNG. Needs matplotlib; nothing else.
"""

import datetime as dt
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

TZ = dt.timezone(dt.timedelta(hours=2))  # rust's nodes, for nvidia-smi's local timestamps
TREES = {
    ("b002de3", "d2cbc58"): "ordering + rh-fix",
    ("6285975", "d2cbc58"): "resource + rh-fix",
    ("6285975", "bd777d1"): "resource + rh-placement",
}
POLICY_ORDER = ["none", "fifo", "lineage", "fair", "res-agg", "res-node"]
LABEL = {
    "none": "No policy", "fifo": "FIFO", "lineage": "Lineage", "fair": "Fair",
    "res-agg": "Resource, aggregate", "res-node": "Resource, node",
}


def load(raw):
    runs = []
    for p in sorted(glob.glob(os.path.join(raw, "*.json"))):
        r = json.load(open(p))
        r["_file"] = os.path.basename(p)
        runs.append(r)
    return runs


def mmm(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return {"median": statistics.median(xs), "min": min(xs), "max": max(xs), "n": len(xs)}


def fmt(s, scale=1.0, digits=1, unit=""):
    if s is None:
        return "–"
    f = f"{{:.{digits}f}}"
    a, b, c = (f.format(s[k] * scale) for k in ("median", "min", "max"))
    return f"{a}{unit} [{b}–{c}]"


CORUNNERS = {}


def node_state(r):
    """Other users' jobs on the node and the CPUs they used, from run.sbatch's samples and,
    for every run, from Slurm accounting (corunners.json; the early runs' own record missed
    hidden partitions)."""
    ns = r.get("node_state") or {}
    others = {
        f"{j['user']}:{j['job']} ({j['partition']}, {j['cpus']} cpus{', ' + j['tres'] if j['tres'] not in ('', 'N/A') else ''})"
        for j in ns.get("squeue_start", []) + ns.get("squeue_end", [])
        if j["user"] != "mturilli"
    }
    for j in CORUNNERS.get(r.get("slurm_job_id"), {}).get("others", []):
        gpu = ", gpu" if "gres/gpu" in j["alloctres"] else ""
        if not any(o.startswith(f"{j['user']}:{j['jobid']} ") for o in others):
            others.add(f"{j['user']}:{j['jobid']} ({j['partition']}, {j['alloccpus']} cpus{gpu})")
    others = sorted(others)
    load = ns.get("load") or []
    other_cpus = [max(0.0, node - me) for _, _, node, me in load]
    return {
        "other_jobs": others,
        "other_cpus_mean": statistics.mean(other_cpus) if other_cpus else None,
        "other_cpus_max": max(other_cpus) if other_cpus else None,
        "job_cpus_mean": statistics.mean([me for *_, me in load]) if load else None,
        "loadavg_max": max((l for _, l, _, _ in load), default=None),
    }


def tree(r):
    return TREES.get((r.get("asyncflow_commit"), r.get("rhapsody_commit")), "?")


# -- E3 -----------------------------------------------------------------------------


def e3(runs):
    groups = defaultdict(list)
    for r in runs:
        if r.get("experiment") == "e3":
            groups[(tree(r), r["config"])].append(r)
    rows = []
    for (tr, cfg), rs in sorted(groups.items(), key=lambda kv: (list(TREES.values()).index(kv[0][0]), POLICY_ORDER.index(kv[0][1]))):
        rows.append(
            {
                "tree": tr,
                "config": cfg,
                "reps": len(rs),
                "jobs": sorted(r["slurm_job_id"] for r in rs),
                "per_task_ms": mmm([r["per_task_latency_ms"] for r in rs]),
                "step_p95_ms": mmm([r["step_ms"]["p95"] for r in rs]),
                "tasks_per_s": mmm([r["tasks_per_s"] for r in rs]),
                "workers": sorted({r["workers"] for r in rs}),
            }
        )
    return rows


# -- E1 -----------------------------------------------------------------------------


def e1(runs, workload):
    groups = defaultdict(list)
    for r in runs:
        exp = r.get("experiment", "")
        if exp.startswith(f"e1-{workload}-"):
            groups[(r["submission"], r["config"])].append(r)
    rows = []
    for (sub, cfg), rs in sorted(groups.items(), key=lambda kv: (kv[0][0] != "stock", POLICY_ORDER.index(kv[0][1]))):
        row = {"submission": sub, "config": cfg, "reps": len(rs), "jobs": sorted(r["slurm_job_id"] for r in rs)}
        if workload == "mixed":
            row["chain_p50_s"] = mmm([r["chain_p50_s"] for r in rs])
            row["chain_p95_s"] = mmm([r["chain_p95_s"] for r in rs])
            row["bulk_makespan_s"] = mmm([r["bulk_makespan_s"] for r in rs])
            row["bulk_busy_pct"] = mmm([busy_pct(r["bulk_worker_times"], r["workers"], r["bulk_makespan_s"]) for r in rs])
            parts = [chain_parts(r) for r in rs]
            for k in ("mean_latency_s", "first_wait_s", "step_gap_s", "run_s", "final_delivery_s"):
                row[k] = mmm([p[k] for p in parts])
        else:
            row["mean_wct_s"] = mmm([r["mean_wct_s"] for r in rs])
            row["first_s"] = mmm([r["first_s"] for r in rs])
            row["makespan_s"] = mmm([r["makespan_s"] for r in rs])
            row["peak_live_wf"] = mmm([r["peak_live_wf"] for r in rs])
        rows.append(row)
    return rows


def chain_parts(r):
    """Where a chain's latency goes, as means over the run's chains so that the parts add up
    to the mean latency: waiting for the first step to start, the gaps between a step's end
    and the next step's start (summed: completion delivery, dependency release, submission and
    start), the steps' own run time, and the delay from the last step's end until asyncflow
    saw it."""
    first, gaps, run, final, lat = [], [], [], [], []
    for c in r["chains"]:
        s, w = c["start_s"], c["worker_times"]
        first.append(w[0][0] - s)
        gaps.append(sum(w[k + 1][0] - w[k][1] for k in range(len(w) - 1)))
        run.append(sum(b - a for a, b in w))
        final.append(c["latency_s"] - (w[-1][1] - s))
        lat.append(c["latency_s"])
    m = statistics.mean
    return {"mean_latency_s": m(lat), "first_wait_s": m(first), "step_gap_s": m(gaps),
            "run_s": m(run), "final_delivery_s": m(final)}


def busy_pct(times, workers, makespan):
    """Share of worker time the bulk tasks kept busy over the bulk makespan."""
    return 100.0 * sum(b - a for a, b in times) / (workers * makespan)


# -- E2 -----------------------------------------------------------------------------


def gpu_window(r):
    """Mean GPU utilisation and memory per GPU over the measurement window."""
    ns = r.get("node_state") or {}
    t0, t1 = r["epoch0"], r["epoch0"] + r["params"]["window_s"]
    per = defaultdict(lambda: {"util": [], "mem": []})
    for ts, idx, util, mem in ns.get("gpu", []):
        t = dt.datetime.strptime(ts, "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=TZ).timestamp()
        if t0 <= t <= t1:
            per[idx]["util"].append(util)
            per[idx]["mem"].append(mem / 1024)
    return {
        idx: {
            "util_mean": statistics.mean(v["util"]) if v["util"] else None,
            "mem_mean_gib": statistics.mean(v["mem"]) if v["mem"] else None,
            "mem_max_gib": max(v["mem"]) if v["mem"] else None,
            "samples": len(v["util"]),
        }
        for idx, v in sorted(per.items())
    }


def lost_reasons(r):
    """Why each lost pipeline ended: the task record of its last task tells."""
    recs = {t["tag"]: t for t in r["tasks"]}
    out = defaultdict(int)
    for c in r["client"]:
        if c.get("ok") is False:
            t = recs.get(c["tag"])
            out["cuda_oom" if t and t["status"] == "oom" else (t["status"] if t else "no_task_record")] += 1
    return dict(out)


def e2(runs):
    groups = defaultdict(list)
    for r in runs:
        if r.get("experiment", "").startswith("e2"):
            groups[(r.get("submission", "stock"), r["config"])].append(r)
    rows = []
    for sub, cfg in sorted(groups, key=lambda k: (k[0] != "stock", POLICY_ORDER.index(k[1]))):
        rs = groups[(sub, cfg)]
        gw = [gpu_window(r) for r in rs]
        util = [statistics.mean(v["util_mean"] for v in g.values() if v["util_mean"] is not None) for g in gw]
        mem_max = [max(v["mem_max_gib"] for v in g.values() if v["mem_max_gib"] is not None) for g in gw]
        bus = {0: "0000:11:00.0", 1: "0000:61:00.0"}
        placed_ok = []
        for r in rs:
            ok = n = 0
            for t in r["tasks"]:
                if "bus" not in t:
                    continue
                n += 1
                ok += t["cvd"] is not None and bus.get(int(t["cvd"].split(",")[0])) == t["bus"]
            placed_ok.append((ok, n))
        wasted = [sum(t["t_end"] - t["t_start"] for t in r["tasks"] if t["status"] == "oom") for r in rs]
        rows.append(
            {
                "submission": sub,
                "config": cfg,
                "reps": len(rs),
                "jobs": sorted(r["slurm_job_id"] for r in rs),
                "lost_reasons": [lost_reasons(r) for r in rs],
                "cuda_ooms": mmm([r["cuda_ooms"] for r in rs]),
                "pipelines_lost": mmm([r["pipelines_lost"] for r in rs]),
                "tasks_per_hour": mmm([r["tasks_per_hour"] for r in rs]),
                "gpu_hours_per_task": mmm([r["gpu_hours_per_task"] for r in rs]),
                "gpu_util_pct": mmm(util),
                "gpu_mem_peak_gib": mmm(mem_max),
                "oom_task_s": mmm(wasted),
                "bus_matches_cvd": placed_ok,
                "per_run_gpu": gw,
            }
        )
    return rows


# -- charts -------------------------------------------------------------------------

INK, MUTED, GRID = "#202124", "#52514E", "#E4E2DC"
BLUE, ORANGE, GRAY = "#2a78d6", "#eb6834", "#9E9E9E"


def setup_mpl():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.family": "Arial",
            "font.size": 11,
            "axes.edgecolor": GRID,
            "axes.labelcolor": MUTED,
            "axes.linewidth": 0.8,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "xtick.major.size": 0,
            "ytick.major.size": 0,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.grid.axis": "x",
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "pdf.fonttype": 42,
            "legend.frameon": False,
        }
    )
    return plt


def save(fig, out_dir, name):
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(out_dir, f"{name}.{ext}"), dpi=200, bbox_inches="tight")


def hbars(plt, rows, value_key, labels, title, xlabel, highlight, out_dir, name, scale=1.0, digits=1, unit=" s",
          series=None):
    """Horizontal bars of the median with a min-max range line; one bar per row, or a pair
    of bars per row when ``series`` names two (key, label, color) of submissions."""
    import numpy as np

    n = len(labels)
    fig, ax = plt.subplots(figsize=(6.4, 0.55 * n + 1.1))
    groups = series or [(None, None, None)]
    h = 0.8 / len(groups)
    vmax = 0
    for gi, (gkey, glabel, gcolor) in enumerate(groups):
        labelled = False
        for i, (cfg, label) in enumerate(labels):
            row = next((r for r in rows if r["config"] == cfg and (gkey is None or r["submission"] == gkey)), None)
            if row is None or row[value_key] is None:
                continue
            s = row[value_key]
            med, lo, hi = (s[k] * scale for k in ("median", "min", "max"))
            vmax = max(vmax, hi)
            y = i + (gi - (len(groups) - 1) / 2) * h
            color = gcolor or (BLUE if cfg in highlight else GRAY)
            ax.barh(y, med, height=h * 0.86, color=color, label=None if labelled else glabel)
            labelled = True
            ax.plot([lo, hi], [y, y], color=INK, linewidth=1.2, solid_capstyle="butt")
            ax.text(hi + vmax * 0.012 if hi else 0, y, f" {med:.{digits}f}{unit}", va="center", ha="left",
                    color=INK, fontsize=10)
    ax.set_yticks(np.arange(n))
    ax.set_yticklabels([lab for _, lab in labels], color=INK)
    ax.invert_yaxis()
    ax.set_xlim(0, vmax * 1.22 if vmax else 1)
    reps = sorted({r["reps"] for r in rows})
    nrep = str(reps[0]) if len(reps) == 1 else f"{reps[0]}–{reps[-1]}"
    ax.set_xlabel(xlabel.replace("median of 3 runs", f"median of {nrep} runs"))
    ax.set_axisbelow(True)
    ax.set_title(title, loc="left", color=INK, fontsize=12, pad=26 if series else 10)
    if series:
        ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=len(series), fontsize=10,
                  borderaxespad=0.2, handlelength=1.2, columnspacing=1.5)
    save(fig, out_dir, name)
    plt.close(fig)


def chain_breakdown(plt, mixed, out_dir):
    """Stacked bars: where the mean chain latency goes, per policy and submission."""
    parts = [("first_wait_s", "Waiting for step 1 to start", BLUE),
             ("step_gap_s", "Between steps", ORANGE),
             ("run_s", "Steps running", "#1baf7a"),
             ("final_delivery_s", "Last step done, not yet reported", "#eda100")]
    rows = []
    for sub, sub_label in (("stock", "stock"), ("spaced8ms", "spaced")):
        for cfg in POLICY_ORDER[:4]:
            r = next((x for x in mixed if x["submission"] == sub and x["config"] == cfg), None)
            if r:
                rows.append((f"{LABEL[cfg]}, {sub_label}", r))
    fig, ax = plt.subplots(figsize=(7.2, 0.42 * len(rows) + 1.4))
    for i, (_, r) in enumerate(rows):
        left = 0.0
        for key, label, color in parts:
            v = r[key]["median"]
            ax.barh(i, v, left=left, height=0.7, color=color, edgecolor="white", linewidth=1.0,
                    label=label if i == 0 else None)
            left += v
        ax.text(left + 1, i, f"{r['mean_latency_s']['median']:.1f} s", va="center", ha="left", color=INK, fontsize=10)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([lab for lab, _ in rows], color=INK)
    ax.invert_yaxis()
    ax.set_xlim(0, max(r["mean_latency_s"]["median"] for _, r in rows) * 1.15)
    ax.set_axisbelow(True)
    reps = sorted({r["reps"] for _, r in rows})
    ax.set_xlabel(f"seconds, mean over a run's 12 chains (median of {reps[0] if len(reps) == 1 else reps} runs)")
    ax.set_title("Where an interactive chain's time goes", loc="left", color=INK, fontsize=12, pad=40)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, fontsize=9.5, borderaxespad=0.2,
              handlelength=1.2, columnspacing=1.5)
    save(fig, out_dir, "e1_chain_breakdown")
    plt.close(fig)


def gpu_memory_chart(plt, runs, submission, out_dir):
    """GPU memory used over the window, one panel per admission mode (repetition 1)."""
    modes = [("none", "No admission"), ("res-agg", "Aggregate"), ("res-node", "Node mode")]
    picked = []
    for cfg, title in modes:
        r = next((x for x in runs if x.get("experiment", "").startswith("e2") and x.get("submission", "stock") == submission
                  and x["config"] == cfg and x["rep"] == 1 and not x.get("failed")), None)
        if r:
            picked.append((title, r))
    if not picked:
        return
    fig, axes = plt.subplots(1, len(picked), figsize=(3.0 * len(picked) + 0.6, 2.9), sharey=True)
    axes = axes if len(picked) > 1 else [axes]
    for ax, (title, r) in zip(axes, picked):
        t0, win = r["epoch0"], r["params"]["window_s"]
        for idx, color in ((0, BLUE), (1, ORANGE)):
            pts = []
            for ts, i, util, mem in r["node_state"].get("gpu", []):
                if i != idx:
                    continue
                t = dt.datetime.strptime(ts, "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=TZ).timestamp() - t0
                if 0 <= t <= win:
                    pts.append((t / 60, mem / 1024))
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=color, linewidth=1.4, label=f"GPU {idx}")
        ax.axhline(47.37, color=INK, linewidth=0.8)
        ax.text(win / 60, 47.37, "47.4 GiB", va="bottom", ha="right", fontsize=8.5, color=MUTED)
        oom = r["cuda_ooms"]
        ax.set_title(f"{title}: {oom} OOM{'s' if oom != 1 else ''}", loc="left", fontsize=10.5, color=INK)
        ax.set_xlim(0, win / 60)
        ax.set_ylim(0, 52)
        ax.set_xlabel("minutes")
        ax.grid(axis="y")
        ax.set_axisbelow(True)
    axes[0].set_ylabel("GPU memory used, GiB")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", ncol=2, fontsize=10, frameon=False, bbox_to_anchor=(1.0, 1.0))
    sub = "stock RHAPSODY + Dragon" if submission == "stock" else "submissions 8 ms apart"
    fig.suptitle(f"GPU memory, 24 pipelines on 2 GPUs (run 1, {sub})", x=0.01, ha="left", fontsize=12, color=INK)
    fig.tight_layout()
    save(fig, out_dir, f"e2_gpu_memory_{submission}")
    plt.close(fig)


def charts(summary, out_dir, runs=()):
    plt = setup_mpl()
    mixed = summary["e1_mixed"]
    if mixed:
        chain_breakdown(plt, mixed, out_dir)
        labels = [(c, LABEL[c]) for c in POLICY_ORDER[:4]]
        series = [("stock", "Stock RHAPSODY + Dragon", BLUE), ("spaced8ms", "Submissions 8 ms apart", ORANGE)]
        series = [s for s in series if any(r["submission"] == s[0] for r in mixed)]
        hbars(plt, mixed, "chain_p50_s", labels, "Interactive chain latency, median (p50) per run",
              "seconds (median of 3 runs; line: min–max)", {"fair"}, out_dir, "e1_chain_latency", series=series)
        hbars(plt, mixed, "bulk_makespan_s", labels, "Bulk makespan beside the chains",
              "seconds (median of 3 runs; line: min–max)", set(), out_dir, "e1_bulk_makespan", digits=0,
              series=series)
    diamonds = summary["e1_diamonds"]
    if diamonds:
        labels = [(c, LABEL[c]) for c in POLICY_ORDER[:4]]
        series = [("stock", "Stock RHAPSODY + Dragon", BLUE), ("spaced8ms", "Submissions 8 ms apart", ORANGE)]
        series = [s for s in series if any(r["submission"] == s[0] for r in diamonds)]
        hbars(plt, diamonds, "mean_wct_s", labels, "Mean diamond-workflow completion",
              "seconds (median of 3 runs; line: min–max)", {"lineage"}, out_dir, "e1_diamonds_completion",
              series=series)
    e2rows = summary["e2"]
    if e2rows:
        labels = [(c, {"none": "No admission (app pins GPU)", "res-agg": "Aggregate (app pins GPU)",
                       "res-node": "Node mode (AsyncFlow pins GPU)"}[c]) for c in ("none", "res-agg", "res-node")
                  if any(r["config"] == c for r in e2rows)]
        series = [("stock", "Stock RHAPSODY + Dragon", BLUE), ("spaced8ms", "Submissions 8 ms apart", ORANGE)]
        series = [x for x in series if any(r["submission"] == x[0] for r in e2rows)]
        hbars(plt, e2rows, "cuda_ooms", labels, "CUDA out-of-memory failures per run (12-minute window and drain)",
              "failed tasks (median of 3 runs; line: min–max)", {"res-node"}, out_dir, "e2_cuda_ooms", digits=0,
              unit="", series=series)
        hbars(plt, e2rows, "gpu_hours_per_task", labels, "GPU time per completed task",
              "GPU-seconds (median of 3 runs; line: min–max)", {"res-node"}, out_dir, "e2_gpu_seconds_per_task",
              scale=3600, digits=1, unit=" s", series=series)
    for submission in ("stock", "spaced8ms"):
        gpu_memory_chart(plt, runs, submission, out_dir)
    e3rows = summary["e3"]
    if e3rows:
        order = [("ordering + rh-fix", "none", "No policy"), ("ordering + rh-fix", "fifo", "FIFO"),
                 ("ordering + rh-fix", "lineage", "Lineage"), ("ordering + rh-fix", "fair", "Fair"),
                 ("resource + rh-fix", "res-agg", "Resource, aggregate"),
                 ("resource + rh-fix", "res-node", "Resource, node (pool)"),
                 ("resource + rh-placement", "res-node", "Resource, node, placed")]
        rows = []
        for tr, cfg, lab in order:
            r = next((x for x in e3rows if x["tree"] == tr and x["config"] == cfg), None)
            if r:
                rows.append({**r, "config": lab})
        labels = [(r["config"], r["config"]) for r in rows]
        hbars(plt, rows, "tasks_per_s", labels, "Throughput of 2,000 independent trivial tasks",
              "tasks per second (median of 3 runs; line: min–max)", {"No policy"}, out_dir, "e3_throughput",
              digits=0, unit="")


# -- report -------------------------------------------------------------------------


def write_md(summary, runs, out_dir):
    L = []
    L.append("# rust policy experiments: summary tables\n")
    L.append("Generated by `experiments/analyze.py` from `results/raw/`. Median [min–max] over repetitions.\n")
    if summary["e3"]:
        L.append("## E3: cost per task\n")
        L.append("| Trees | Policy | Runs | Per-task latency, chain of 100 | Chain step p95 | Throughput, 2,000 tasks | Jobs |")
        L.append("|---|---|---|---|---|---|---|")
        for r in summary["e3"]:
            L.append(f"| {r['tree']} | {LABEL[r['config']]} | {r['reps']} | {fmt(r['per_task_ms'], digits=1, unit=' ms')} | "
                     f"{fmt(r['step_p95_ms'], digits=1, unit=' ms')} | {fmt(r['tasks_per_s'], digits=0, unit='/s')} | {', '.join(r['jobs'])} |")
        L.append("")
    for wl in ("mixed", "diamonds"):
        rows = summary[f"e1_{wl}"]
        if not rows:
            continue
        if wl == "mixed":
            L.append("## E1a: bulk tasks beside interactive chains\n")
            L.append("| Submission | Policy | Runs | Chain latency p50 | Chain latency p95 | Bulk makespan | Bulk worker busy | Jobs |")
            L.append("|---|---|---|---|---|---|---|---|")
            for r in rows:
                L.append(f"| {r['submission']} | {LABEL[r['config']]} | {r['reps']} | {fmt(r['chain_p50_s'], digits=2, unit=' s')} | "
                         f"{fmt(r['chain_p95_s'], digits=2, unit=' s')} | {fmt(r['bulk_makespan_s'], digits=1, unit=' s')} | "
                         f"{fmt(r['bulk_busy_pct'], digits=0, unit='%')} | {', '.join(r['jobs'])} |")
            L.append("")
            L.append("Where a chain's latency goes (per run, the mean over its 12 chains, so the parts add up to the mean; then median [min–max] over runs):\n")
            L.append("| Submission | Policy | Mean chain latency | Wait for step 1 to start | Gaps between steps (2) | Steps running | Last step end → asyncflow sees it |")
            L.append("|---|---|---|---|---|---|---|")
            for r in rows:
                L.append(f"| {r['submission']} | {LABEL[r['config']]} | {fmt(r['mean_latency_s'], digits=2, unit=' s')} | {fmt(r['first_wait_s'], digits=2, unit=' s')} | "
                         f"{fmt(r['step_gap_s'], digits=2, unit=' s')} | {fmt(r['run_s'], digits=2, unit=' s')} | "
                         f"{fmt(r['final_delivery_s'], digits=2, unit=' s')} |")
        else:
            L.append("## E1b: concurrent diamond workflows\n")
            L.append("| Submission | Policy | Runs | Mean workflow completion | First workflow done | Makespan | In progress at peak | Jobs |")
            L.append("|---|---|---|---|---|---|---|---|")
            for r in rows:
                L.append(f"| {r['submission']} | {LABEL[r['config']]} | {r['reps']} | {fmt(r['mean_wct_s'], digits=1, unit=' s')} | "
                         f"{fmt(r['first_s'], digits=1, unit=' s')} | {fmt(r['makespan_s'], digits=1, unit=' s')} | "
                         f"{fmt(r['peak_live_wf'], digits=0)} | {', '.join(r['jobs'])} |")
        L.append("")
    if summary["e2"]:
        L.append("## E2: GPU packing by memory\n")
        L.append("| Submission | Admission | Runs | CUDA OOMs | Pipelines lost | Tasks done per hour | GPU-seconds per task | GPU utilisation | GPU memory peak | GPU time in OOM tasks | Jobs |")
        L.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for r in summary["e2"]:
            L.append(f"| {r['submission']} | {LABEL[r['config']]} | {r['reps']} | {fmt(r['cuda_ooms'], digits=0)} | {fmt(r['pipelines_lost'], digits=0)} | "
                     f"{fmt(r['tasks_per_hour'], digits=0)} | {fmt(r['gpu_hours_per_task'], scale=3600, digits=2, unit=' s')} | "
                     f"{fmt(r['gpu_util_pct'], digits=0, unit='%')} | {fmt(r['gpu_mem_peak_gib'], digits=1, unit=' GiB')} | "
                     f"{fmt(r['oom_task_s'], digits=0, unit=' s')} | {', '.join(r['jobs'])} |")
        L.append("")
        L.append("Tasks whose GPU (PCI bus id) matched their CUDA_VISIBLE_DEVICES, per run: " + "; ".join(
            f"{r['submission']} {LABEL[r['config']]} " + ", ".join(f"{ok}/{n}" for ok, n in r["bus_matches_cvd"])
            for r in summary["e2"]) + "\n")
        L.append("Why pipelines were lost, per run: " + "; ".join(
            f"{r['submission']} {LABEL[r['config']]} " + ", ".join(json.dumps(x) for x in r["lost_reasons"])
            for r in summary["e2"]) + "\n")
    failed = [r for r in runs if r.get("failed") or "experiment" not in r]
    if failed:
        L.append("## Runs without a result\n")
        for r in failed:
            L.append(f"- `{r['_file']}`, job {r.get('slurm_job_id')}: {r.get('error', 'no result file (Dragon rc ' + str((r.get('node_state') or {}).get('dragon_rc')) + ')')}")
        L.append("")
    L.append("## Every run: job, node, co-runners\n")
    L.append("| File | Job | Node | CPUs | Dragon rc | Other users' jobs on the node | Other users' CPUs busy, mean / max | Load avg max |")
    L.append("|---|---|---|---|---|---|---|---|")
    for r in runs:
        ns = node_state(r)
        rc = (r.get("node_state") or {}).get("dragon_rc")
        om = "–" if ns["other_cpus_mean"] is None else f"{ns['other_cpus_mean']:.1f} / {ns['other_cpus_max']:.1f}"
        la = "–" if ns["loadavg_max"] is None else f"{ns['loadavg_max']:.1f}"
        L.append(f"| {r['_file']} | {r.get('slurm_job_id')} | {r.get('slurm_nodelist', '?')} | {r.get('slurm_cpus_on_node', '?')} | {rc} | "
                 f"{', '.join(ns['other_jobs']) or 'none'} | {om} | {la} |")
    open(os.path.join(out_dir, "summary.md"), "w").write("\n".join(L) + "\n")


def main():
    raw, out_dir = sys.argv[1], sys.argv[2]
    cr = os.path.join(raw, "corunners.json")
    if os.path.exists(cr):
        CORUNNERS.update(json.load(open(cr)))
    runs = [r for r in load(raw) if r["_file"] != "corunners.json"]
    ok = [r for r in runs if "experiment" in r and not r.get("failed")]
    summary = {"e3": e3(ok), "e1_mixed": e1(ok, "mixed"), "e1_diamonds": e1(ok, "diamonds"), "e2": e2(ok)}
    json.dump(summary, open(os.path.join(out_dir, "summary.json"), "w"), indent=1, default=str)
    write_md(summary, runs, out_dir)
    charts(summary, out_dir, ok)
    print(open(os.path.join(out_dir, "summary.md")).read())


if __name__ == "__main__":
    main()

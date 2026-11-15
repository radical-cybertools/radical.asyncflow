"""Tables and charts for the second rust campaign (prompt reports, priorities, capacity).

Usage: python analyze2.py <raw_dir> <out_dir>

Reads the result files of submit_campaign2.sh, groups them by configuration (Dragon build,
switches, policy, capacity), and writes ``summary.md``, ``summary.json`` and the charts (PDF
and PNG). The helpers come from analyze.py (the first campaign's analysis).
"""

import json
import os
import statistics
import sys
from collections import defaultdict

import analyze as a1

VARIANTS = {
    "stock": "Stock Dragon",
    "patch": "Stock + no-change patch",
    "compile": "Branch: one-task compiles (a)",
    "report": "Branch: per-task reports (b)",
    "reports": "Branch: (a) + (b)",
    "prio": "Branch: (a)+(b)+(d) + priorities",
}
VARIANT_ORDER = list(VARIANTS)
SHORT = {"stock": "stock", "patch": "stock + patch", "compile": "(a)", "report": "(b)",
         "reports": "(a)+(b)", "prio": "(a)+(b)+(d), prio"}
POLICY = {"none": "No policy", "fifo": "FIFO", "lineage": "Lineage", "fair": "Fair"}
CAP_ORDER = {"pool": 0, "32": 0, "64": 1, "128": 2, "inf": 3}


def variant(r):
    flags = set(r["spec"]["flags"])
    branch = not str(r.get("dragon", "")).startswith("dragonhpc")
    if not branch:
        return "patch" if "each" in flags else "stock"
    if "prio" in flags:
        return "prio"
    if {"a", "b"} <= flags:
        return "reports"
    return "report" if "b" in flags else "compile"


def cap_label(c):
    return {"pool": "32", "inf": "∞"}.get(c, c)


def key(r):
    spec = r["spec"]
    cap = spec["capacity"] if spec["policy"] != "none" else "-"
    return (variant(r), spec["policy"], cap)


def sort_key(k):
    v, p, c = k
    return (VARIANT_ORDER.index(v), list(POLICY).index(p), CAP_ORDER.get(c, -1))


def label(k):
    v, p, c = k
    pol = POLICY[p] if p == "none" else f"{POLICY[p]} {cap_label(c)}"
    return f"{pol} · {VARIANTS[v]}"


def group(runs, experiment_prefix):
    g = defaultdict(list)
    for r in runs:
        if r.get("experiment", "").startswith(experiment_prefix) and not r.get("failed"):
            g[key(r)].append(r)
    return dict(sorted(g.items(), key=lambda kv: sort_key(kv[0])))


def e1a(runs):
    rows = []
    for k, rs in group(runs, "e1-mixed").items():
        parts = [a1.chain_parts(r) for r in rs]
        rows.append({
            "key": k, "label": label(k), "reps": len(rs), "jobs": sorted(r["slurm_job_id"] for r in rs),
            "chain_p50_s": a1.mmm([r["chain_p50_s"] for r in rs]),
            "chain_p95_s": a1.mmm([r["chain_p95_s"] for r in rs]),
            "bulk_makespan_s": a1.mmm([r["bulk_makespan_s"] for r in rs]),
            "bulk_busy_pct": a1.mmm([a1.busy_pct(r["bulk_worker_times"], r["workers"], r["bulk_makespan_s"]) for r in rs]),
            **{p: a1.mmm([x[p] for x in parts]) for p in ("mean_latency_s", "first_wait_s", "step_gap_s", "run_s", "final_delivery_s")},
        })
    return rows


def e1b(runs):
    rows = []
    for k, rs in group(runs, "e1-diamonds").items():
        rows.append({
            "key": k, "label": label(k), "reps": len(rs), "jobs": sorted(r["slurm_job_id"] for r in rs),
            "mean_wct_s": a1.mmm([r["mean_wct_s"] for r in rs]),
            "first_s": a1.mmm([r["first_s"] for r in rs]),
            "makespan_s": a1.mmm([r["makespan_s"] for r in rs]),
            "peak_live_wf": a1.mmm([r["peak_live_wf"] for r in rs]),
        })
    return rows


def e3(runs):
    rows = []
    for k, rs in group(runs, "e3").items():
        rows.append({
            "key": k, "label": label(k), "reps": len(rs), "jobs": sorted({r["slurm_job_id"] for r in rs}),
            "per_task_ms": a1.mmm([r["per_task_latency_ms"] for r in rs]),
            "step_p95_ms": a1.mmm([r["step_ms"]["p95"] for r in rs]),
            "tasks_per_s": a1.mmm([r["tasks_per_s"] for r in rs]),
        })
    return rows


# -- charts -------------------------------------------------------------------------

COLORS = {"stock": a1.GRAY, "patch": "#c3c2b7", "compile": "#e8e6df", "report": "#eda100",
          "reports": a1.ORANGE, "prio": a1.BLUE}


def hbar_rows(plt, rows, value_key, title, xlabel, out_dir, name, scale=1.0, digits=1, unit=" s", keep=None):
    """One bar per configuration, coloured by its variant, median with a min–max line."""
    rows = [r for r in rows if keep is None or keep(r["key"])]
    if not rows:
        return
    fig, ax = plt.subplots(figsize=(7.2, 0.36 * len(rows) + 1.5))
    vmax = max(r[value_key]["max"] * scale for r in rows)
    seen = set()
    for i, r in enumerate(rows):
        s = r[value_key]
        med, lo, hi = (s[k] * scale for k in ("median", "min", "max"))
        v = r["key"][0]
        ax.barh(i, med, height=0.7, color=COLORS[v], label=None if v in seen else VARIANTS[v])
        seen.add(v)
        ax.plot([lo, hi], [i, i], color=a1.INK, linewidth=1.1, solid_capstyle="butt")
        ax.text(hi + vmax * 0.012, i, f" {med:.{digits}f}{unit}", va="center", ha="left", color=a1.INK, fontsize=9)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([f"{r['label'].split(' · ')[0]} · {SHORT[r['key'][0]]}" for r in rows], color=a1.INK,
                       fontsize=9.5)
    ax.invert_yaxis()
    ax.set_xlim(0, vmax * 1.2)
    ax.set_axisbelow(True)
    reps = sorted({r["reps"] for r in rows})
    ax.set_xlabel(f"{xlabel} (median of {reps[0] if len(reps) == 1 else f'{reps[0]}–{reps[-1]}'} runs; line: min–max)")
    legend_rows = (len(seen) + 1) // 2
    ax.set_title(title, loc="left", color=a1.INK, fontsize=12, pad=12 + 19 * legend_rows)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, fontsize=9.5, borderaxespad=0.2, handlelength=1.2)
    a1.save(fig, out_dir, name)
    plt.close(fig)


def capacity_chart(plt, e1a_rows, e1b_rows, e3_rows, out_dir):
    """Capacity above the pool size, on the branch with reports and priorities: E3 throughput,
    E1a chain latency (Fair) and E1b workflow completion (Lineage), against capacity, each with
    the branch's no-policy run as a reference."""
    caps = ["32", "64", "128", "inf"]
    xs = list(range(len(caps)))

    def series(rows, policy, k):
        pts = []
        for x, c in zip(xs, caps):
            r = next((r for r in rows if r["key"] == ("prio", policy, c)), None)
            if r:
                pts.append((x, r[k]["median"], r[k]["min"], r[k]["max"]))
        return pts

    def baseline(rows, k):
        r = next((r for r in rows if r["key"] == ("reports", "none", "-")), None)
        return r[k]["median"] if r else None

    panels = (
        (e3_rows, (("fair", a1.BLUE), ("lineage", "#1baf7a")), "tasks_per_s", "E3: throughput, 2,000 tasks",
         "tasks per second"),
        (e1a_rows, (("fair", a1.BLUE),), "chain_p50_s", "E1a: chain latency, p50", "seconds"),
        (e1b_rows, (("lineage", "#1baf7a"),), "mean_wct_s", "E1b: mean diamond completion", "seconds"),
    )
    fig, axes = plt.subplots(1, 3, figsize=(11.5, 3.3))
    for ax, (rows, policies, k, title, ylab) in zip(axes, panels):
        top = 0
        for policy, color in policies:
            pts = series(rows, policy, k)
            if not pts:
                continue
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=color, linewidth=2, marker="o", markersize=7,
                    label=f"{POLICY[policy]}, reports + priorities")
            for x, m, lo, hi in pts:
                ax.plot([x, x], [lo, hi], color=a1.INK, linewidth=1.1)
            top = max(top, max(p[3] for p in pts))
        b = baseline(rows, k)
        if b is not None:
            ax.axhline(b, color=a1.ORANGE, linewidth=1.4, label="No policy, (a)+(b)")
            top = max(top, b)
        ax.set_xticks(xs)
        ax.set_xticklabels(["32 (pool)", "64", "128", "∞"])
        ax.set_xlabel("policy capacity")
        ax.set_ylabel(ylab)
        ax.set_ylim(0, top * 1.15 if top else 1)
        ax.set_title(title, loc="left", fontsize=10.5, color=a1.INK)
        ax.grid(axis="y")
        ax.grid(axis="x", visible=False)
        ax.set_axisbelow(True)
    handles, labels = {}, {}
    for ax in axes:
        for h, l in zip(*ax.get_legend_handles_labels()):
            handles.setdefault(l, h)
    fig.suptitle("A capacity above the pool size, with Dragon's per-task reports and priorities", x=0.01, ha="left",
                 fontsize=12, color=a1.INK)
    fig.tight_layout()
    fig.legend(list(handles.values()), list(handles), loc="upper center", ncol=3, fontsize=9.5, frameon=False,
               bbox_to_anchor=(0.5, 0.0))
    a1.save(fig, out_dir, "r2_capacity_tradeoff")
    plt.close(fig)


def charts(s, out_dir):
    plt = a1.setup_mpl()
    hbar_rows(plt, s["e1a"], "chain_p50_s", "Interactive chain latency beside 1,000 bulk tasks, p50", "seconds",
              out_dir, "r2_e1a_chain_latency")
    hbar_rows(plt, s["e1a"], "bulk_makespan_s", "Bulk makespan beside the chains", "seconds", out_dir,
              "r2_e1a_bulk_makespan", digits=0)
    hbar_rows(plt, s["e1b"], "mean_wct_s", "Mean diamond-workflow completion", "seconds", out_dir,
              "r2_e1b_completion")
    hbar_rows(plt, s["e3"], "tasks_per_s", "Throughput of 2,000 independent trivial tasks", "tasks per second",
              out_dir, "r2_e3_throughput", digits=0, unit="")
    capacity_chart(plt, s["e1a"], s["e1b"], s["e3"], out_dir)


# -- report -------------------------------------------------------------------------


def write_md(s, runs, out_dir):
    f = a1.fmt
    L = ["# rust policy experiments, second campaign: summary tables\n",
         "Generated by `experiments/analyze2.py` from `results/r2/raw/`. Median [min–max] over repetitions. "
         "Capacity 32 is the Batch pool size.\n"]
    L += ["## E3: cost per task\n",
          "| Dragon | Policy | Capacity | Runs | Per-task latency, chain of 100 | Chain step p95 | Throughput, 2,000 tasks | Jobs |",
          "|---|---|---|---|---|---|---|---|"]
    for r in s["e3"]:
        v, p, c = r["key"]
        L.append(f"| {VARIANTS[v]} | {POLICY[p]} | {cap_label(c)} | {r['reps']} | {f(r['per_task_ms'], digits=1, unit=' ms')} | "
                 f"{f(r['step_p95_ms'], digits=1, unit=' ms')} | {f(r['tasks_per_s'], digits=0, unit='/s')} | {', '.join(r['jobs'])} |")
    L += ["", "## E1a: bulk tasks beside interactive chains\n",
          "| Dragon | Policy | Capacity | Runs | Chain latency p50 | Chain latency p95 | Bulk makespan | Bulk worker busy | Jobs |",
          "|---|---|---|---|---|---|---|---|---|"]
    for r in s["e1a"]:
        v, p, c = r["key"]
        L.append(f"| {VARIANTS[v]} | {POLICY[p]} | {cap_label(c)} | {r['reps']} | {f(r['chain_p50_s'], digits=2, unit=' s')} | "
                 f"{f(r['chain_p95_s'], digits=2, unit=' s')} | {f(r['bulk_makespan_s'], digits=1, unit=' s')} | "
                 f"{f(r['bulk_busy_pct'], digits=0, unit='%')} | {', '.join(r['jobs'])} |")
    L += ["", "Where a chain's latency goes (mean over a run's 12 chains; median [min–max] over runs):\n",
          "| Dragon | Policy | Capacity | Mean chain latency | Waiting for step 1 | Between steps (2 gaps) | Steps running | Done, not yet reported |",
          "|---|---|---|---|---|---|---|---|"]
    for r in s["e1a"]:
        v, p, c = r["key"]
        L.append(f"| {VARIANTS[v]} | {POLICY[p]} | {cap_label(c)} | {f(r['mean_latency_s'], digits=2, unit=' s')} | "
                 f"{f(r['first_wait_s'], digits=2, unit=' s')} | {f(r['step_gap_s'], digits=2, unit=' s')} | "
                 f"{f(r['run_s'], digits=2, unit=' s')} | {f(r['final_delivery_s'], digits=2, unit=' s')} |")
    L += ["", "## E1b: concurrent diamond workflows\n",
          "| Dragon | Policy | Capacity | Runs | Mean workflow completion | First workflow done | Makespan | In progress at peak | Jobs |",
          "|---|---|---|---|---|---|---|---|---|"]
    for r in s["e1b"]:
        v, p, c = r["key"]
        L.append(f"| {VARIANTS[v]} | {POLICY[p]} | {cap_label(c)} | {r['reps']} | {f(r['mean_wct_s'], digits=1, unit=' s')} | "
                 f"{f(r['first_s'], digits=1, unit=' s')} | {f(r['makespan_s'], digits=1, unit=' s')} | "
                 f"{f(r['peak_live_wf'], digits=0)} | {', '.join(r['jobs'])} |")
    failed = [r for r in runs if r.get("failed") or "experiment" not in r]
    if failed:
        L += ["", "## Runs without a result\n"]
        for r in failed:
            rc = (r.get("node_state") or {}).get("dragon_rc")
            L.append(f"- `{r['_file']}`, job {r.get('slurm_job_id')}: {r.get('error', f'no result file (Dragon rc {rc})')}")
    L += ["", "## Every run: job, node, co-runners\n",
          "| File | Job | Dragon | Dragon rc | Other users' jobs on the node | Other users' CPUs busy, mean / max |",
          "|---|---|---|---|---|---|"]
    for r in runs:
        ns = a1.node_state(r)
        rc = (r.get("node_state") or {}).get("dragon_rc")
        om = "–" if ns["other_cpus_mean"] is None else f"{ns['other_cpus_mean']:.1f} / {ns['other_cpus_max']:.1f}"
        L.append(f"| {r['_file']} | {r.get('slurm_job_id')} | {r.get('dragon', '?')} | {rc} | {', '.join(ns['other_jobs']) or 'none'} | {om} |")
    open(os.path.join(out_dir, "summary.md"), "w").write("\n".join(L) + "\n")


def main():
    raw, out_dir = sys.argv[1], sys.argv[2]
    cr = os.path.join(raw, "corunners.json")
    if os.path.exists(cr):
        a1.CORUNNERS.update(json.load(open(cr)))
    runs = [r for r in a1.load(raw) if r["_file"] != "corunners.json"]
    ok = [r for r in runs if "experiment" in r and not r.get("failed") and "spec" in r]
    s = {"e3": e3(ok), "e1a": e1a(ok), "e1b": e1b(ok)}
    json.dump(s, open(os.path.join(out_dir, "summary.json"), "w"), indent=1, default=str)
    write_md(s, runs, out_dir)
    charts(s, out_dir)
    print(open(os.path.join(out_dir, "summary.md")).read())


if __name__ == "__main__":
    main()

"""Tables and charts for the fourth rust campaign: Batch's completion hand-off (switch e).

Usage: python analyze4.py <raw_dir> <out_dir>

Reads the result files of submit_campaign4.sh (julia, 32 workers, capacity 32): E3 and E1a with
Dragon branch (a)+(b) against (a)+(b)+(e). Writes ``summary.md``, ``summary.json`` and the charts
``r4_e3_steps`` (the dependent step's distribution), ``r4_e3_e1a`` (throughput, chain latency,
makespan), PDF and PNG.
"""

import json
import os
import sys
from collections import defaultdict

import analyze as a1
from common import pct

VARIANT = {False: "(a)+(b)", True: "(a)+(b)+(e)"}
POLICY = {"none": "No policy", "fair": "Fair", "lineage": "Lineage"}


def key(r):
    return ("e" in r["spec"]["flags"], r["spec"]["policy"])


def group(runs, prefix):
    g = defaultdict(list)
    for r in runs:
        if r.get("experiment", "").startswith(prefix) and not r.get("failed"):
            g[key(r)].append(r)
    return dict(sorted(g.items(), key=lambda kv: (list(POLICY).index(kv[0][1]), kv[0][0])))


def e3(runs):
    rows = []
    for k, rs in group(runs, "e3").items():
        steps = [s for r in rs for s in r["steps_ms"][1:]]
        rows.append({
            "key": k, "reps": len(rs), "jobs": sorted({r["slurm_job_id"] for r in rs}),
            "per_task_ms": a1.mmm([r["per_task_latency_ms"] for r in rs]),
            "step_p50": a1.mmm([r["step_ms"]["p50"] for r in rs]),
            "step_p95": a1.mmm([r["step_ms"]["p95"] for r in rs]),
            "step_max": max(r["step_ms"]["max"] for r in rs),
            "steps_pooled": {"p50": pct(steps, 50), "p95": pct(steps, 95), "p99": pct(steps, 99),
                             "n": len(steps)},
            "tasks_per_s": a1.mmm([r["tasks_per_s"] for r in rs]),
            "_steps": steps,
        })
    return rows


def e1a(runs):
    rows = []
    for k, rs in group(runs, "e1-mixed").items():
        rows.append({
            "key": k, "reps": len(rs), "jobs": sorted(r["slurm_job_id"] for r in rs),
            "chain_p50_s": a1.mmm([r["chain_p50_s"] for r in rs]),
            "chain_p95_s": a1.mmm([r["chain_p95_s"] for r in rs]),
            "bulk_makespan_s": a1.mmm([r["bulk_makespan_s"] for r in rs]),
            "bulk_busy_pct": a1.mmm([a1.busy_pct(r["bulk_worker_times"], r["workers"], r["bulk_makespan_s"])
                                     for r in rs]),
        })
    return rows


def charts(s, out_dir):
    plt = a1.setup_mpl()
    colors = {False: a1.GRAY, True: a1.ORANGE}
    fig, ax = plt.subplots(figsize=(7.2, 3.2))
    bins = [i * 2 for i in range(0, 41)]
    for wake in (False, True):
        steps = [x for r in s["e3"] if r["key"][0] == wake for x in r["_steps"]]
        if steps:
            ax.hist([min(x, 80) for x in steps], bins=bins, color=colors[wake], alpha=0.85 if wake else 0.7,
                    label=f"{VARIANT[wake]}: p50 {pct(steps, 50):.1f} ms, p95 {pct(steps, 95):.1f} ms",
                    edgecolor="white", linewidth=0.5)
    ax.set_xlabel("dependent step, ms (chain of 100 through AsyncFlow + RHAPSODY; ≥80 ms in the last bin)")
    ax.set_ylabel("steps")
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    ax.set_axisbelow(True)
    ax.set_title("E3: a dependent step with and without Batch's 50 ms report wait", loc="left", fontsize=12,
                 color=a1.INK, pad=50)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=1, fontsize=9.5, borderaxespad=0.2,
              handlelength=1.2)
    a1.save(fig, out_dir, "r4_e3_steps")
    plt.close(fig)

    panels = (("e3", "tasks_per_s", "E3: throughput, 2,000 tasks", "tasks per second", 0),
              ("e1a", "chain_p50_s", "E1a: chain latency, p50", "seconds", 2),
              ("e1a", "bulk_makespan_s", "E1a: bulk makespan", "seconds", 1))
    fig, axes = plt.subplots(1, 3, figsize=(11.5, 3.0))
    for ax, (exp, k, title, xlab, digits) in zip(axes, panels):
        policies = [p for p in POLICY if any(r["key"][1] == p for r in s[exp])]
        for i, p in enumerate(policies):
            for j, wake in enumerate((False, True)):
                r = next((x for x in s[exp] if x["key"] == (wake, p)), None)
                if not r:
                    continue
                v = r[k]
                y = i + (j - 0.5) * 0.38
                ax.barh(y, v["median"], height=0.34, color=colors[wake], label=VARIANT[wake] if i == 0 else None)
                ax.plot([v["min"], v["max"]], [y, y], color=a1.INK, linewidth=1.0)
                ax.text(v["max"], y, f"  {v['median']:.{digits}f}", va="center", fontsize=8.5, color=a1.INK)
        ax.set_yticks(range(len(policies)))
        ax.set_yticklabels([POLICY[p] for p in policies], fontsize=9.5, color=a1.INK)
        ax.invert_yaxis()
        ax.set_xlim(0, ax.get_xlim()[1] * 1.2)
        ax.set_xlabel(xlab)
        ax.set_title(title, loc="left", fontsize=10.5, color=a1.INK)
        ax.set_axisbelow(True)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.suptitle("Switch (e) on julia, 32 workers, capacity 32 (median of 3 runs; line: min–max)", x=0.01,
                 ha="left", fontsize=12, color=a1.INK)
    fig.tight_layout()
    fig.legend(handles, labels, loc="upper center", ncol=2, fontsize=9.5, frameon=False, bbox_to_anchor=(0.5, 0.0))
    a1.save(fig, out_dir, "r4_e3_e1a")
    plt.close(fig)


def write_md(s, runs, out_dir):
    f = a1.fmt
    L = ["# rust policy experiments, fourth campaign: summary tables\n",
         "Generated by `experiments/analyze4.py` from `results/r4/raw/`. Median [min–max] over 3 runs. julia, "
         "64 CPUs, 32 Batch workers, capacity 32; Dragon branch 0a7d87d.\n",
         "## E3\n",
         "| Dragon | Policy | Runs | Per-task latency, chain of 100 | Step p50 | Step p95 | Steps pooled p50 / p95 / p99 "
         "| Step max | Throughput, 2,000 tasks | Jobs |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for r in s["e3"]:
        w, p = r["key"]
        sp = r["steps_pooled"]
        L.append(f"| {VARIANT[w]} | {POLICY[p]} | {r['reps']} | {f(r['per_task_ms'], digits=1, unit=' ms')} | "
                 f"{f(r['step_p50'], digits=1, unit=' ms')} | {f(r['step_p95'], digits=1, unit=' ms')} | "
                 f"{sp['p50']:.1f} / {sp['p95']:.1f} / {sp['p99']:.1f} ms (n={sp['n']}) | {r['step_max']:.0f} ms | "
                 f"{f(r['tasks_per_s'], digits=0, unit='/s')} | {', '.join(r['jobs'])} |")
    L += ["", "## E1a\n",
          "| Dragon | Policy | Runs | Chain p50 | Chain p95 | Bulk makespan | Bulk worker busy | Jobs |",
          "|---|---|---|---|---|---|---|---|"]
    for r in s["e1a"]:
        w, p = r["key"]
        L.append(f"| {VARIANT[w]} | {POLICY[p]} | {r['reps']} | {f(r['chain_p50_s'], digits=2, unit=' s')} | "
                 f"{f(r['chain_p95_s'], digits=2, unit=' s')} | {f(r['bulk_makespan_s'], digits=1, unit=' s')} | "
                 f"{f(r['bulk_busy_pct'], digits=0, unit='%')} | {', '.join(r['jobs'])} |")
    failed = [r for r in runs if r.get("failed") or "experiment" not in r]
    if failed:
        L += ["", "## Runs without a result\n"]
        L += [f"- `{r['_file']}`, job {r.get('slurm_job_id')}: {r.get('error', 'no result file')}" for r in failed]
    L += ["", "## Every run: job, co-runners\n", "| File | Job | Dragon rc | Other users' jobs on julia |", "|---|---|---|---|"]
    for r in runs:
        ns = a1.node_state(r)
        L.append(f"| {r['_file']} | {r.get('slurm_job_id')} | {(r.get('node_state') or {}).get('dragon_rc')} | "
                 f"{', '.join(ns['other_jobs']) or 'none'} |")
    open(os.path.join(out_dir, "summary.md"), "w").write("\n".join(L) + "\n")


def main():
    raw, out_dir = sys.argv[1], sys.argv[2]
    cr = os.path.join(raw, "corunners.json")
    if os.path.exists(cr):
        a1.CORUNNERS.update(json.load(open(cr)))
    runs = [r for r in a1.load(raw) if r["_file"] != "corunners.json"]
    ok = [r for r in runs if "experiment" in r and not r.get("failed") and "spec" in r]
    s = {"e3": e3(ok), "e1a": e1a(ok)}
    json.dump({k: v for k, v in s.items()}, open(os.path.join(out_dir, "summary.json"), "w"), indent=1,
              default=str)
    write_md(s, runs, out_dir)
    charts(s, out_dir)
    print(open(os.path.join(out_dir, "summary.md")).read())


if __name__ == "__main__":
    main()

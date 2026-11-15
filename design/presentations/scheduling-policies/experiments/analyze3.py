"""Tables and charts for the third rust campaign (one Dragon runtime over two nodes).

Usage: python analyze3.py <raw_dir> <out_dir> [<campaign 2 summary.json>]

Reads the result files of submit_campaign3.sh:
- E2 (``e2-2n``) on haskell (RTX 6000 Ada, 47.4 GiB) + fortran (RTX PRO 6000 Blackwell,
  95.0 GiB), a heterogeneous job: OOMs, tasks done, GPU busy and memory, per node;
- E1a, E1b and E3 on haskell + julia (32 Batch workers in all), next to the second
  campaign's one-node runs of the same configurations (julia, 32 workers), when its
  ``summary.json`` is given.
Writes ``summary.md``, ``summary.json`` and the charts (PDF and PNG). The helpers come from
analyze.py and analyze2.py.
"""

import datetime as dt
import json
import os
import statistics
import sys
from collections import Counter, defaultdict

import analyze as a1
import analyze2 as a2

GPUS = {  # host: (bus id, GiB the driver reports, model, GiB one CUDA context takes)
    "haskell": ("0000:61:00.0", 47.37, "RTX 6000 Ada", 0.42),
    "fortran": ("0000:42:00.0", 94.97, "RTX PRO 6000 Blackwell", 0.55),
}
HOST_COLOR = {"haskell": a1.BLUE, "fortran": a1.ORANGE, "julia": "#1baf7a"}
MODES = {"none": "No admission", "res-agg": "Aggregate", "res-node": "Node mode"}
DRAGON = {"branch": "branch (a)+(b)", "stock": "stock 0.14.1"}


def dragon(r):
    return "stock" if str(r.get("dragon", "")).startswith("dragonhpc") else "branch"


def tzinfo(r):
    z = (r.get("node_state") or {}).get("tz", "+0100")
    sign = -1 if z[0] == "-" else 1
    return dt.timezone(sign * dt.timedelta(hours=int(z[1:3]), minutes=int(z[3:5])))


def gpu_samples(r, host):
    """``[(seconds from epoch0, util %, GiB used)]`` of a host's GPU."""
    tz = tzinfo(r)
    node = ((r.get("node_state") or {}).get("per_node") or {}).get(host) or {}
    out = []
    for ts, _, util, mem in node.get("gpu", []):
        t = dt.datetime.strptime(ts, "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=tz).timestamp() - r["epoch0"]
        out.append((t, util, mem / 1024))
    return out


def peaks(tasks, ctx):
    """Peak concurrent tasks and peak GiB allocated (context included) over the tasks' runs."""
    ev = sorted([(t["t_start"], 1, t["mem_gib"] + ctx) for t in tasks if "t_end" in t]
                + [(t["t_end"], -1, t["mem_gib"] + ctx) for t in tasks if "t_end" in t])
    n = m = pn = pm = 0
    for _, d, g in ev:
        n += d
        m += d * g
        pn, pm = max(pn, n), max(pm, m)
    return pn, pm


def mean_running(tasks, t0, t1):
    """Time-weighted mean of the tasks running between t0 and t1."""
    busy = sum(max(0.0, min(t["t_end"], t1) - max(t["t_start"], t0)) for t in tasks if "t_end" in t)
    return busy / (t1 - t0)


# -- E2 ----------------------------------------------------------------------------


def e2_run(r):
    win = r["params"]["window_s"]
    hosts = {}
    for host in GPUS:
        ts = [t for t in r["tasks"] if t.get("host") == host]
        g = [s for s in gpu_samples(r, host) if 0 <= s[0] <= win]
        pn, pm = peaks(ts, GPUS[host][3])
        hosts[host] = {
            "tasks": len(ts),
            "done": sum(t["status"] == "done" and t["t_end"] <= win for t in ts),
            "ooms": sum(t["status"] == "oom" for t in ts),
            "other_errors": sum(t["status"] not in ("done", "oom") for t in ts),
            "bus_ok": sum(t.get("bus") == GPUS[host][0] for t in ts),
            "with_bus": sum("bus" in t for t in ts),
            "ctx_ooms": sum(t["status"] == "oom" and "bus" not in t for t in ts),
            "util_mean": statistics.mean(s[1] for s in g) if g else None,
            "mem_peak_gib": max((s[2] for s in g), default=None),
            "peak_tasks": pn,
            "peak_alloc_gib": pm,
            "mean_tasks": mean_running(ts, 0.0, win),
        }
    all_tasks = len(r["tasks"])
    return {
        "hosts": hosts,
        "haskell_share": hosts["haskell"]["tasks"] / all_tasks if all_tasks else None,
        "first_ooms_s": min((t["t_start"] for t in r["tasks"] if t["status"] == "oom"), default=None),
    }


def e2(runs):
    g = defaultdict(list)
    for r in runs:
        if r.get("experiment", "").startswith("e2-2n") and not r.get("failed") and r["rep"] > 0:
            g[(dragon(r), r["spec"]["policy"])].append(r)
    rows = []
    for k in sorted(g, key=lambda k: (k[0] != "branch", list(MODES).index(k[1]))):
        rs = g[k]
        per = [e2_run(r) for r in rs]

        def h(host, key):
            return a1.mmm([p["hosts"][host][key] for p in per])

        rows.append({
            "key": k, "reps": len(rs), "jobs": sorted(r["slurm_job_id"] for r in rs),
            "cuda_ooms": a1.mmm([r["cuda_ooms"] for r in rs]),
            "pipelines_lost": a1.mmm([r["pipelines_lost"] for r in rs]),
            "tasks_done": a1.mmm([r["tasks_done_in_window"] for r in rs]),
            "tasks_per_hour": a1.mmm([r["tasks_per_hour"] for r in rs]),
            "gpu_hours_per_task": a1.mmm([r["gpu_hours_per_task"] for r in rs]),
            "haskell_share": a1.mmm([p["haskell_share"] for p in per]),
            "lost_reasons": [a1.lost_reasons(r) for r in rs],
            **{f"{host}_{key}": h(host, key) for host in GPUS
               for key in ("tasks", "done", "ooms", "other_errors", "util_mean", "mem_peak_gib", "peak_tasks",
                           "peak_alloc_gib", "mean_tasks")},
            "bus_ok": [[(p["hosts"][host]["bus_ok"], p["hosts"][host]["with_bus"], p["hosts"][host]["ctx_ooms"])
                        for host in GPUS] for p in per],
        })
    return rows


# -- Part B ------------------------------------------------------------------------


def variant(r):
    return "reports" if dragon(r) == "branch" else ("patch" if "each" in r["spec"]["flags"] else "stock")


def bkey(r):
    return (variant(r), r["spec"]["policy"], "-" if r["spec"]["policy"] == "none" else r["spec"]["capacity"])


def host_share(hosts):
    c = Counter(hosts)
    return c.get("haskell", 0) / sum(c.values()) if c else None


def e1a(runs):
    rows = a2.e1a([r for r in runs if r.get("experiment", "").startswith("e1-mixed")])
    by = defaultdict(list)
    for r in runs:
        if r.get("experiment", "").startswith("e1-mixed") and not r.get("failed"):
            by[bkey(r)].append(r)
    for row in rows:
        rs = by[tuple(row["key"])]
        row["bulk_haskell_share"] = a1.mmm([host_share(r.get("bulk_hosts", [])) for r in rs])
        row["chain_steps_cross"] = a1.mmm([
            statistics.mean(sum(a != b for a, b in zip(c["hosts"], c["hosts"][1:])) for c in r["chains"])
            for r in rs if r["chains"] and "hosts" in r["chains"][0]])
    return rows


def e1b(runs):
    return a2.e1b([r for r in runs if r.get("experiment", "").startswith("e1-diamonds")])


def e3(runs):
    rows = a2.e3([r for r in runs if r.get("experiment", "").startswith("e3")])
    by = defaultdict(list)
    for r in runs:
        if r.get("experiment", "").startswith("e3") and not r.get("failed"):
            by[bkey(r)].append(r)
    for row in rows:
        rs = by[tuple(row["key"])]
        same, cross, shares = [], [], []
        for r in rs:
            hosts, steps = r.get("chain_hosts") or [], r["steps_ms"]
            # step i ends task i; it depends on task i-1 (the first step has no parent)
            s = [st for i, st in enumerate(steps) if 0 < i < len(hosts) and hosts[i] == hosts[i - 1]]
            c = [st for i, st in enumerate(steps) if 0 < i < len(hosts) and hosts[i] != hosts[i - 1]]
            same.append(statistics.median(s) if s else None)
            cross.append(statistics.median(c) if c else None)
            th = r.get("throughput_hosts") or {}
            shares.append(th.get("haskell", 0) / sum(th.values()) if th else None)
        row["step_same_node_ms"] = a1.mmm(same)
        row["step_cross_node_ms"] = a1.mmm(cross)
        row["chain_cross_steps"] = a1.mmm([sum(a != b for a, b in zip(h, h[1:]))
                                           for h in (r.get("chain_hosts") or [] for r in rs)])
        row["throughput_haskell_share"] = a1.mmm(shares)
    return rows


def one_node(r2, exp, k):
    """The second campaign's row of the same configuration (julia, 32 workers)."""
    if not r2:
        return None
    return next((row for row in r2.get(exp, []) if tuple(row["key"]) == tuple(k)), None)


# -- charts ------------------------------------------------------------------------


def e2_bars(plt, rows, out_dir):
    """OOMs, tasks done and GPU busy per admission mode, per node."""
    rows = [r for r in rows]
    if not rows:
        return
    fig, axes = plt.subplots(1, 3, figsize=(11.5, 0.42 * len(rows) + 1.9), sharey=True)
    labels = [f"{MODES[m]} · {DRAGON[d]}" for d, m in (r["key"] for r in rows)]
    panels = (("ooms", "CUDA OOMs per run", 0, ""), ("done", "Tasks done in 12 min", 0, ""),
              ("util_mean", "GPU busy, mean", 0, "%"))
    for ax, (key, title, digits, unit) in zip(axes, panels):
        vmax = max(max(r[f"{h}_{key}"]["max"] for h in GPUS if r[f"{h}_{key}"]) for r in rows) or 1
        for i, r in enumerate(rows):
            for j, host in enumerate(GPUS):
                s = r[f"{host}_{key}"]
                if s is None:
                    continue
                y = i + (j - 0.5) * 0.38
                ax.barh(y, s["median"], height=0.34, color=HOST_COLOR[host],
                        label=f"{host} ({GPUS[host][2]})" if i == 0 else None)
                ax.plot([s["min"], s["max"]], [y, y], color=a1.INK, linewidth=1.0)
                med = s["median"]
                text = f"{med:.1f}" if digits == 0 and med != round(med) else f"{med:.{digits}f}"
                ax.text(s["max"] + vmax * 0.02, y, f"{text}{unit}", va="center", fontsize=8.5, color=a1.INK)
        ax.set_xlim(0, vmax * 1.25)
        ax.set_title(title, loc="left", fontsize=10.5, color=a1.INK)
        ax.set_axisbelow(True)
    axes[0].set_yticks(range(len(rows)))
    axes[0].set_yticklabels(labels, fontsize=9.5, color=a1.INK)
    axes[0].invert_yaxis()
    handles, names = axes[0].get_legend_handles_labels()
    reps = sorted({r["reps"] for r in rows})
    n = f"{reps[0]} run{'s' if reps[0] > 1 else ''}" if len(reps) == 1 else f"{reps[0]}–{reps[-1]} runs"
    fig.suptitle(f"E2 on two nodes: 24 pipelines, one GPU per node (median of {n}; line: min–max)", x=0.01,
                 ha="left", fontsize=12, color=a1.INK)
    fig.tight_layout()
    fig.legend(handles, names, loc="upper center", ncol=2, fontsize=9.5, frameon=False, bbox_to_anchor=(0.5, 0.0))
    a1.save(fig, out_dir, "r3_e2_per_node")
    plt.close(fig)


def e2_memory(plt, runs, which, out_dir):
    """GPU memory used on each node over the window, one panel per admission mode (run 1)."""
    picked = []
    for m, title in MODES.items():
        r = next((x for x in runs if x.get("experiment", "").startswith("e2-2n") and dragon(x) == which
                  and x["spec"]["policy"] == m and x["rep"] == 1 and not x.get("failed")), None)
        if r:
            picked.append((title, r))
    if not picked:
        return
    fig, axes = plt.subplots(1, len(picked), figsize=(3.3 * len(picked) + 0.6, 3.0), sharey=True)
    axes = axes if len(picked) > 1 else [axes]
    for ax, (title, r) in zip(axes, picked):
        win = r["params"]["window_s"]
        for host in GPUS:
            pts = [(t / 60, mem) for t, _, mem in gpu_samples(r, host) if 0 <= t <= win]
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=HOST_COLOR[host], linewidth=1.4,
                    label=f"{host} ({GPUS[host][2]})")
            ax.axhline(GPUS[host][1], color=HOST_COLOR[host], linewidth=0.8, linestyle=(0, (3, 2)))
        per = e2_run(r)["hosts"]
        oom = ", ".join(f"{per[h]['ooms']} {h}" for h in GPUS)
        ax.set_title(f"{title}\nOOMs: {oom}", loc="left", fontsize=10.5, color=a1.INK)
        ax.set_xlim(0, win / 60)
        ax.set_ylim(0, 100)
        ax.set_xlabel("minutes")
        ax.grid(axis="y")
        ax.set_axisbelow(True)
    axes[0].set_ylabel("GPU memory used, GiB")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.suptitle(f"GPU memory per node, run 1, Dragon {DRAGON[which]} (dashed: the GPU's memory)", x=0.01,
                 ha="left", fontsize=12, color=a1.INK)
    fig.tight_layout()
    fig.legend(handles, labels, loc="upper center", ncol=2, fontsize=9.5, frameon=False, bbox_to_anchor=(0.5, 0.0))
    a1.save(fig, out_dir, f"r3_e2_gpu_memory_{which}")
    plt.close(fig)


def one_vs_two(plt, s, r2, out_dir):
    """E3 throughput, E1a chain p50 and E1b completion: one node (campaign 2) against two."""
    panels = (("e3", "tasks_per_s", "E3: throughput, 2,000 tasks", "tasks per second", 0),
              ("e1a", "chain_p50_s", "E1a: chain latency, p50", "seconds", 2),
              ("e1b", "mean_wct_s", "E1b: mean diamond completion", "seconds", 1))
    fig, axes = plt.subplots(1, 3, figsize=(12.0, 3.6))
    for ax, (exp, k, title, xlab, digits) in zip(axes, panels):
        rows = s[exp]
        for i, r in enumerate(rows):
            one = one_node(r2, exp, r["key"])
            for j, (src, color, lab) in enumerate(((one, "#c3c2b7", "one node (julia, 2nd campaign)"),
                                                  (r, a1.BLUE, "two nodes (haskell + julia)"))):
                if not src or not src.get(k):
                    continue
                v = src[k]
                y = i + (j - 0.5) * 0.38
                ax.barh(y, v["median"], height=0.34, color=color, label=lab if i == 0 else None)
                ax.plot([v["min"], v["max"]], [y, y], color=a1.INK, linewidth=1.0)
                ax.text(v["max"], y, f"  {v['median']:.{digits}f}", va="center", fontsize=8.5, color=a1.INK)
        ax.set_yticks(range(len(rows)))
        ax.set_yticklabels([f"{a2.POLICY[r['key'][1]]} · {a2.SHORT[r['key'][0]]}" for r in rows], fontsize=9.5,
                           color=a1.INK)
        ax.invert_yaxis()
        ax.set_xlabel(xlab)
        ax.set_title(title, loc="left", fontsize=10.5, color=a1.INK)
        ax.set_xlim(0, ax.get_xlim()[1] * 1.18)
        ax.set_axisbelow(True)
    handles, labels = {}, {}
    for ax in axes:
        for h_, l_ in zip(*ax.get_legend_handles_labels()):
            handles.setdefault(l_, h_)
    fig.suptitle("32 Batch workers on one node or on two (median; line: min–max)", x=0.01, ha="left", fontsize=12,
                 color=a1.INK)
    fig.tight_layout()
    fig.legend(list(handles.values()), list(handles), loc="upper center", ncol=2, fontsize=9.5, frameon=False,
               bbox_to_anchor=(0.5, 0.0))
    a1.save(fig, out_dir, "r3_one_vs_two_nodes")
    plt.close(fig)


def chain_steps(plt, rows, out_dir):
    """E3 chain: the time per dependent step when it stays on its parent's node or moves."""
    rows = [r for r in rows if r.get("step_same_node_ms") and r.get("step_cross_node_ms")]
    if not rows:
        return
    fig, ax = plt.subplots(figsize=(7.2, 0.5 * len(rows) + 1.6))
    vmax = max(max(r["step_same_node_ms"]["max"], r["step_cross_node_ms"]["max"]) for r in rows)
    for i, r in enumerate(rows):
        for j, (k, color, lab) in enumerate((("step_same_node_ms", a1.BLUE, "same node as its parent"),
                                             ("step_cross_node_ms", a1.ORANGE, "other node than its parent"))):
            v = r[k]
            y = i + (j - 0.5) * 0.38
            ax.barh(y, v["median"], height=0.34, color=color, label=lab if i == 0 else None)
            ax.plot([v["min"], v["max"]], [y, y], color=a1.INK, linewidth=1.0)
            ax.text(v["max"] + vmax * 0.015, y, f"{v['median']:.1f} ms", va="center", fontsize=8.5, color=a1.INK)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([f"{a2.POLICY[r['key'][1]]} · {a2.SHORT[r['key'][0]]}" for r in rows], fontsize=9.5,
                       color=a1.INK)
    ax.invert_yaxis()
    ax.set_xlim(0, vmax * 1.25)
    ax.set_xlabel("median step of a run, ms (median of runs; line: min–max)")
    ax.set_axisbelow(True)
    ax.set_title("E3: a dependent step costs the same on either node (chain of 100)", loc="left", fontsize=12, color=a1.INK,
                 pad=31)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, fontsize=9.5, borderaxespad=0.2, handlelength=1.2)
    a1.save(fig, out_dir, "r3_e3_chain_steps")
    plt.close(fig)


def charts(s, runs, r2, out_dir):
    plt = a1.setup_mpl()
    e2_bars(plt, s["e2"], out_dir)
    for which in ("branch", "stock"):
        e2_memory(plt, runs, which, out_dir)
    if s["e3"] or s["e1a"] or s["e1b"]:
        one_vs_two(plt, s, r2, out_dir)
        chain_steps(plt, s["e3"], out_dir)


# -- report ------------------------------------------------------------------------


def write_md(s, runs, r2, out_dir):
    f = a1.fmt
    L = ["# rust policy experiments, third campaign: summary tables\n",
         "Generated by `experiments/analyze3.py` from `results/r3/raw/`. Median [min–max] over repetitions.\n"]
    L += ["## E2: two GPU nodes (haskell RTX 6000 Ada 47.4 GiB + fortran RTX PRO 6000 Blackwell 95.0 GiB)\n",
          "| Dragon | Admission | Runs | CUDA OOMs | Pipelines lost | Tasks done in 720 s | GPU-hours per task | "
          "Tasks on haskell | Jobs |",
          "|---|---|---|---|---|---|---|---|---|"]
    for r in s["e2"]:
        d, m = r["key"]
        L.append(f"| {DRAGON[d]} | {MODES[m]} | {r['reps']} | {f(r['cuda_ooms'], digits=0)} | "
                 f"{f(r['pipelines_lost'], digits=0)} | {f(r['tasks_done'], digits=0)} | "
                 f"{f(r['gpu_hours_per_task'], digits=4)} | {f(r['haskell_share'], scale=100, digits=0, unit='%')} | "
                 f"{', '.join(r['jobs'])} |")
    L += ["", "Per node:\n",
          "| Dragon | Admission | Node | Tasks started | Done in window | OOMs | Tasks running, mean | Peak tasks at once | "
          "Peak GiB allocated (tasks) | Peak GiB used (nvidia-smi) | GPU busy, mean |",
          "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in s["e2"]:
        d, m = r["key"]
        for host in GPUS:
            L.append(f"| {DRAGON[d]} | {MODES[m]} | {host} | {f(r[f'{host}_tasks'], digits=0)} | "
                     f"{f(r[f'{host}_done'], digits=0)} | {f(r[f'{host}_ooms'], digits=0)} | "
                     f"{f(r[f'{host}_mean_tasks'], digits=1)} | {f(r[f'{host}_peak_tasks'], digits=0)} | {f(r[f'{host}_peak_alloc_gib'], digits=1)} | "
                     f"{f(r[f'{host}_mem_peak_gib'], digits=1)} | {f(r[f'{host}_util_mean'], digits=0, unit='%')} |")
    L += ["", "Task records whose GPU bus id is their node's GPU, of those that recorded one (haskell, fortran), "
          "per run; in brackets the tasks that failed creating a CUDA context (OOM), before reading the bus id:\n"]
    for r in s["e2"]:
        d, m = r["key"]
        L.append(f"- {DRAGON[d]}, {MODES[m]}: " + "; ".join(
            ", ".join(f"{ok}/{n}" + (f" (+{c})" if c else "") for ok, n, c in run) for run in r["bus_ok"])
            + f"; pipelines lost by cause: {r['lost_reasons']}")

    def side(exp, row, k, digits, unit):
        one = one_node(r2, exp, row["key"])
        return f"{f(one[k], digits=digits, unit=unit) if one and one.get(k) else '–'} | {f(row[k], digits=digits, unit=unit)}"

    L += ["", "## Part B: haskell + julia, 32 Batch workers in all\n",
          "One node: the second campaign's runs of the same configuration on julia (64 CPUs, 32 workers), "
          "`results/r2/summary.json`.\n",
          "### E3\n",
          "| Dragon | Policy | Runs | Per-task latency, 1 node | 2 nodes | Throughput, 1 node | 2 nodes | "
          "Step, same node | Step, other node | Chain steps that change node | Throughput tasks on haskell | Jobs |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in s["e3"]:
        v, p, c = r["key"]
        L.append(f"| {a2.VARIANTS[v]} | {a2.POLICY[p]} | {r['reps']} | {side('e3', r, 'per_task_ms', 1, ' ms')} | "
                 f"{side('e3', r, 'tasks_per_s', 0, '/s')} | {f(r['step_same_node_ms'], digits=1, unit=' ms')} | "
                 f"{f(r['step_cross_node_ms'], digits=1, unit=' ms')} | {f(r['chain_cross_steps'], digits=0)} of 99 | "
                 f"{f(r['throughput_haskell_share'], scale=100, digits=0, unit='%')} | {', '.join(r['jobs'])} |")
    L += ["", "### E1a\n",
          "| Dragon | Policy | Runs | Chain p50, 1 node | 2 nodes | Bulk makespan, 1 node | 2 nodes | "
          "Bulk worker busy, 1 node | 2 nodes | Bulk tasks on haskell | Jobs |",
          "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in s["e1a"]:
        v, p, c = r["key"]
        L.append(f"| {a2.VARIANTS[v]} | {a2.POLICY[p]} | {r['reps']} | {side('e1a', r, 'chain_p50_s', 2, ' s')} | "
                 f"{side('e1a', r, 'bulk_makespan_s', 1, ' s')} | {side('e1a', r, 'bulk_busy_pct', 0, '%')} | "
                 f"{f(r['bulk_haskell_share'], scale=100, digits=0, unit='%')} | {', '.join(r['jobs'])} |")
    L += ["", "### E1b\n",
          "| Dragon | Policy | Runs | Mean completion, 1 node | 2 nodes | Makespan, 1 node | 2 nodes | Jobs |",
          "|---|---|---|---|---|---|---|---|"]
    for r in s["e1b"]:
        v, p, c = r["key"]
        L.append(f"| {a2.VARIANTS[v]} | {a2.POLICY[p]} | {r['reps']} | {side('e1b', r, 'mean_wct_s', 1, ' s')} | "
                 f"{side('e1b', r, 'makespan_s', 1, ' s')} | {', '.join(r['jobs'])} |")
    failed = [r for r in runs if r.get("failed") or "experiment" not in r]
    if failed:
        L += ["", "## Runs without a result\n"]
        for r in failed:
            rc = (r.get("node_state") or {}).get("dragon_rc")
            L.append(f"- `{r['_file']}`, job {r.get('slurm_job_id')}: {r.get('error', f'no result file (Dragon rc {rc})')}")
    L += ["", "## Every run: job, nodes, co-runners\n",
          "| File | Job | Nodes | Dragon | Dragon rc | Other users' jobs on the nodes |", "|---|---|---|---|---|---|"]
    for r in runs:
        ns = r.get("node_state") or {}
        others = sorted({f"{j['user']}:{j['job']} ({j['partition']}, {j['cpus']} cpus"
                         f"{', ' + j['tres'] if j['tres'] not in ('', 'N/A') else ''}, {j.get('nodes', '?')})"
                         for j in ns.get("squeue_start", []) + ns.get("squeue_end", []) if j["user"] != "mturilli"})
        for j in a1.CORUNNERS.get(r.get("slurm_job_id"), {}).get("others", []):
            if not any(o.startswith(f"{j['user']}:{j['jobid']} ") for o in others):
                others.append(f"{j['user']}:{j['jobid']} ({j['partition']}, {j['alloccpus']} cpus, {j.get('nodelist', '?')})")
        nodes = ",".join(sorted((ns.get("per_node") or {}).keys())) or r.get("slurm_nodelist")
        L.append(f"| {r['_file']} | {r.get('slurm_job_id')} | {nodes} | {r.get('dragon', '?')} | "
                 f"{ns.get('dragon_rc')} | {', '.join(others) or 'none'} |")
    open(os.path.join(out_dir, "summary.md"), "w").write("\n".join(L) + "\n")


def main():
    raw, out_dir = sys.argv[1], sys.argv[2]
    r2 = json.load(open(sys.argv[3])) if len(sys.argv) > 3 else None
    cr = os.path.join(raw, "corunners.json")
    if os.path.exists(cr):
        a1.CORUNNERS.update(json.load(open(cr)))
    runs = [r for r in a1.load(raw) if r["_file"] != "corunners.json"]
    ok = [r for r in runs if "experiment" in r and not r.get("failed") and "spec" in r]
    s = {"e2": e2(ok), "e3": e3(ok), "e1a": e1a(ok), "e1b": e1b(ok)}
    json.dump(s, open(os.path.join(out_dir, "summary.json"), "w"), indent=1, default=str)
    write_md(s, runs, r2, out_dir)
    charts(s, ok, r2, out_dir)
    print(open(os.path.join(out_dir, "summary.md")).read())


if __name__ == "__main__":
    main()

"""Tables and charts for the Bridges-2 campaign (fifth): raw Batch across nodes, E1/E3 on 2 and 4
RM nodes, rhapsody#95 on RM-shared, and the GPU density and stall tests.

Usage: python analyze_b2.py <b2_dir>

Reads ``<b2_dir>/raw/{raw,af,i95,smoke,density4,stall}`` (result files of b2run.sbatch) and
``<b2_dir>/diag/{runs,logs}`` (run directories and job logs, for the runs that wrote no result).
Writes ``<b2_dir>/summary.md``, ``summary.json`` and, in ``<b2_dir>/charts``, ``b2_raw_batch``,
``b2_af`` and ``b2_i95`` as PDF and PNG.
"""

import collections
import datetime as dt
import glob
import json
import os
import re
import sys

import analyze as a1

POLICY = {"none": "No policy", "fair": "Fair", "lineage": "Lineage"}


def load(d):
    out = []
    for p in sorted(glob.glob(os.path.join(d, "*.json"))):
        r = json.load(open(p))
        r["_file"] = os.path.basename(p)
        out.append(r)
    return out


def nnodes(r):
    nl = r.get("slurm_nodelist", "")
    m = re.match(r"\w+\[(.*)\]", nl)
    if not m:
        return 1
    n = 0
    for part in m.group(1).split(","):
        lo, _, hi = part.partition("-")
        n += int(hi) - int(lo) + 1 if hi else 1
    return n


# 3a: raw Batch, one 2,000-task batch per layout inside 1-, 2- and 4-node runtimes
def raw_batch(runs):
    rows = []
    for r in runs:
        if "runs" not in r or r["tag"].endswith("debug"):
            continue
        env = r["tag"].split("-")[0]
        runtime = len(r["nodes"])
        by = collections.defaultdict(list)
        for x in r["runs"]:
            by[x["num_nodes"]].append(x)
        for used, xs in sorted(by.items()):
            rows.append({
                "env": env, "runtime": runtime, "used": used, "reps": len(xs),
                "tasks_per_s": a1.mmm([x["tasks_per_s"] for x in xs]),
                "step_p50_ms": a1.mmm([x["chain_p50_ms"] for x in xs]),
                "job": r["slurm_job_id"],
            })
    rows.sort(key=lambda x: (x["runtime"], x["used"], x["env"] != "stock"))
    return rows


def raw_hung(diag_dir, job="50006591"):
    """Layouts a raw-Batch runtime finished before it hung (no result file): from the job log."""
    import gzip

    text = gzip.open(os.path.join(diag_dir, "logs", f"d2r-{job}.out.gz"), "rt").read()
    out = []
    for m in re.finditer(r"^== run (\d+) at [^:]+:\d\d:\d\d \w+: (.*)$", text, re.M):
        k, cmd = m.group(1), m.group(2)
        body = text[m.end():].split("\n== run %s dragon rc=" % k, 1)
        rc = re.match(r"(\d+) wall=(\d+)s", body[1]) if len(body) > 1 else None
        if not rc or rc.group(1) == "0" or "debug" in cmd:
            continue
        layouts = [int(x) for x in re.search(r"--layouts (\S+)", cmd).group(1).split(",")]
        done = [(int(n), float(t), float(st)) for n, t, st in re.findall(
            r"Batch over (\d+) node\(s\), \d+ workers: (\d+) tasks/s .*?dependent step p50 ([\d.]+) ms", body[0])]
        order = layouts  # rep 1 runs the layouts forward
        hung_in = order[len(done)] if len(done) < len(order) else None
        out.append({"tag": re.search(r"--tag (\S+)", cmd).group(1), "rc": int(rc.group(1)), "wall_s": int(rc.group(2)),
                    "done": done, "hung_in": hung_in})
    return out


def raw_debug(runs):
    return [{"tag": r["tag"], "runtime": len(r["nodes"]), "tasks_per_s": r["runs"][0]["tasks_per_s"],
             "step_p50_ms": r["runs"][0]["chain_p50_ms"]} for r in runs if r.get("tag", "").endswith("debug")]


# 3b: E3, E1a, E1b on 2 and 4 nodes, branch (a)+(b)+(e); follow-ups 2-4: E1a on one node, 32, 64 and 128 workers
def af(runs):
    g = collections.defaultdict(list)
    for r in runs:
        if "experiment" not in r:
            continue
        g[(r["experiment"].split("-")[0] + ("-" + r["experiment"].split("-")[1] if r["experiment"].startswith("e1") else ""),
           nnodes(r), r["workers"], r["spec"]["policy"])].append(r)
    rows = []
    for (exp, n, _, pol), rs in sorted(g.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2],
                                                                   list(POLICY).index(kv[0][3]))):
        row = {"exp": exp, "nodes": n, "policy": pol, "reps": len(rs), "workers": rs[0]["workers"],
               "jobs": sorted({r["slurm_job_id"] for r in rs})}
        if exp == "e3":
            row.update(step_p50_ms=a1.mmm([r["step_ms"]["p50"] for r in rs]),
                       step_p95_ms=a1.mmm([r["step_ms"]["p95"] for r in rs]),
                       per_task_ms=a1.mmm([r["per_task_latency_ms"] for r in rs]),
                       tasks_per_s=a1.mmm([r["tasks_per_s"] for r in rs]))
        elif exp == "e1-mixed":
            row.update(bulk=rs[0]["params"]["bulk"],
                       chain_p50_s=a1.mmm([r["chain_p50_s"] for r in rs]),
                       chain_p95_s=a1.mmm([r["chain_p95_s"] for r in rs]),
                       bulk_makespan_s=a1.mmm([r["bulk_makespan_s"] for r in rs]))
        else:
            row.update(diamonds=rs[0]["params"]["diamonds"],
                       first_s=a1.mmm([r["first_s"] for r in rs]),
                       mean_wct_s=a1.mmm([r["mean_wct_s"] for r in rs]),
                       makespan_s=a1.mmm([r["makespan_s"] for r in rs]),
                       peak_live_wf=a1.mmm([r["peak_live_wf"] for r in rs]))
        rows.append(row)
    return rows


def i95(runs):
    g = collections.defaultdict(list)
    for r in runs:
        g[(int(r["slurm_cpus_on_node"]), r["config"])].append(r)
    rows = []
    for (c, cfg), rs in sorted(g.items()):
        rows.append({"cores": c, "config": cfg, "reps": len(rs), "workers": rs[0]["workers"],
                     "ok": sum(r["status"] == "ok" for r in rs), "jobs": sorted(r["slurm_job_id"] for r in rs),
                     **{k: a1.mmm([r[k] for r in rs]) for k in ("backend_s", "trivial_s", "workflow_s", "shutdown_s")}})
    return rows


# GPU: smoke 1, the density test and the stall tests, from results or, for a stalled run, its run dir
def gpu_runs(raw_dir, diag_dir):
    rows = []
    for log in sorted(glob.glob(os.path.join(diag_dir, "logs", "d2*-4982114[7].out")) +
                      glob.glob(os.path.join(diag_dir, "logs", "d2s-*.out"))):
        job = re.search(r"-(\d+)\.out", log).group(1)
        text = open(log).read()
        for m in re.finditer(r"^== run (\d+) at [^:]+:\d\d:\d\d \w+: (e2_gpu\.py .*)$", text, re.M):
            k, cmd = m.group(1), m.group(2)
            arg = lambda name, d=None: (re.search(rf"--{name} (\S+)", cmd) or [None, d])[1]  # noqa: E731
            run_dir = os.path.join(diag_dir, "runs", job, k)
            tasks = [json.load(open(p)) for p in glob.glob(os.path.join(run_dir, "tasks", "*.json"))]
            res = None
            for sub in ("smoke", "density4", "stall"):
                for p in glob.glob(os.path.join(raw_dir, sub, f"e2_*_{job}*.json")):
                    r = json.load(open(p))
                    if r["config"] in cmd.split()[2] and int(r["workers"]) == int(arg("workers-per-node", 0)) \
                            and r["params"]["pipelines"] == int(arg("pipelines")):
                        res = r
            rc = re.search(rf"^== run {k} dragon rc=(\d+) wall=(\d+)s", text, re.M)
            peak, per_gpu = concurrency(tasks)
            errs = collections.Counter()
            if res:
                for c in res["client"]:
                    if c.get("ok") is False:
                        errs[c["error"].split("(")[0]] += 1
            rows.append({
                "job": job, "run": int(k), "mode": cmd.split()[2], "gpus": int(arg("expect-gpus", 1)),
                "pipelines": int(arg("pipelines")), "workers": int(arg("workers-per-node", 0)),
                "rc": int(rc.group(1)) if rc else None, "wall_s": int(rc.group(2)) if rc else None,
                "tasks_started": len(tasks), "tasks_done": sum(t.get("status") == "done" for t in tasks),
                "peak_concurrent": peak, "peak_per_gpu": max(per_gpu.values()) if per_gpu else 0,
                "pipelines_lost": res["pipelines_lost"] if res else None, "ooms": res["cuda_ooms"] if res else None,
                "drained_s": res["drained_s"] if res else None, "errors": dict(errs),
                "outcome": "stalled (timeout)" if rc and rc.group(1) == "124" else
                           (f"{res['pipelines_lost']} lost" if res and res["pipelines_lost"] else "clean"),
            })
    return rows


def concurrency(tasks):
    ev = []
    for t in tasks:
        if t.get("t_end"):
            ev += [(t["t_start"], 1, t.get("cvd")), (t["t_end"], -1, t.get("cvd"))]
    ev.sort()
    cur, peak, tot, ptot = collections.Counter(), collections.Counter(), 0, 0
    for _, d, g in ev:
        cur[g] += d
        tot += d
        peak[g] = max(peak[g], cur[g])
        ptot = max(ptot, tot)
    return ptot, dict(peak)


# SU from the job logs (sacct timed out): first run start to "job done", plus a minute of setup
def su(diag_dir):
    rows = []
    for log in sorted(glob.glob(os.path.join(diag_dir, "logs", "d2*.out*"))):
        opener = __import__("gzip").open if log.endswith(".gz") else open
        text = opener(log, "rt").read()
        job = re.search(r"-(\d+)\.out", log).group(1)
        part = re.search(r"partition=(\S+) cpus/task=(\d+) gpus=(\S+)", text)
        nodes = re.search(r"^== job \d+ on ([^:]+):", text, re.M)
        start = re.search(r"^== run 1 at (\S+ \S+)", text, re.M)
        end = re.search(r"^== job done at (\S+ \S+)", text, re.M)
        if not (part and start and end):
            continue
        t = (dt.datetime.fromisoformat(end.group(1)) - dt.datetime.fromisoformat(start.group(1))).total_seconds() + 60
        n = len(nodes.group(1).split(",")) if nodes else 1
        p, cpus, gpus = part.group(1), int(part.group(2)), part.group(3)
        if p.startswith("GPU"):
            g = int(gpus.split(":")[-1]) if ":" in gpus else 8 * n
            units, kind = g * t / 3600, "GPU"
        else:
            units, kind = (128 * n if p == "RM" else cpus) * t / 3600, "RM"
        rows.append({"job": job, "partition": p, "nodes": n, "elapsed_min": round(t / 60, 1), "kind": kind,
                     "su": round(units, 1)})
    return rows


def chart_raw(plt, rows, out, hung=()):
    import numpy as np

    fig, axes = plt.subplots(1, 2, figsize=(11.0, 3.4))
    cats = [(r["runtime"], r["used"]) for r in rows if r["env"] == "stock"]
    for ax, key, xlab, digits in ((axes[0], "tasks_per_s", "tasks/s, 2,000 trivial tasks", 0),
                                 (axes[1], "step_p50_ms", "dependent step p50, ms (chain of 100)", 1)):
        for gi, (env, color, label) in enumerate((("stock", a1.GRAY, "Stock 0.14.1"),
                                                  ("branch", a1.ORANGE, "Branch (a)+(b)+(e)"))):
            for i, c in enumerate(cats):
                row = next((r for r in rows if r["env"] == env and (r["runtime"], r["used"]) == c), None)
                y = i + (gi - 0.5) * 0.4
                if row is None:
                    h = next((h for h in hung if h["tag"] == f"{env}-n{c[0]}"), None)
                    one = next((d for d in h["done"] if d[0] == c[1]), None) if h else None
                    if one is None:
                        ax.text(1, y, "  hung", va="center", fontsize=8.5, color=a1.MUTED)
                    else:
                        v = one[1] if key == "tasks_per_s" else one[2]
                        ax.barh(y, v, height=0.36, color=color, alpha=0.55)
                        ax.text(v, y, f"  {v:.{digits}f} (1 run, then hung)", va="center", fontsize=8.5,
                                color=a1.INK)
                    continue
                s = row[key]
                ax.barh(y, s["median"], height=0.36, color=color, label=label if i == 0 else None)
                ax.plot([s["min"], s["max"]], [y, y], color=a1.INK, linewidth=1.0)
                ax.text(s["max"], y, f"  {s['median']:.{digits}f}", va="center", fontsize=8.5, color=a1.INK)
        ax.set_yticks(np.arange(len(cats)))
        ax.set_yticklabels([f"{u} of {rt} node{'s' if rt > 1 else ''}" for rt, u in cats], color=a1.INK)
        ax.invert_yaxis()
        ax.set_xlim(0, ax.get_xlim()[1] * 1.15)
        ax.set_xlabel(xlab)
        ax.set_axisbelow(True)
    axes[0].set_ylabel("nodes used, of runtime", color=a1.MUTED)
    fig.suptitle("Raw Batch on Bridges-2 RM: one node alone vs inside a 2- or 4-node runtime "
                 "(median of 3; line: min–max)", x=0.01, ha="left", fontsize=12, color=a1.INK)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.tight_layout()
    fig.legend(handles, labels, loc="upper center", ncol=2, fontsize=9.5, bbox_to_anchor=(0.5, 0.0))
    a1.save(fig, out, "b2_raw_batch")
    plt.close(fig)


def chart_af(plt, rows, out):
    import numpy as np

    panels = [("e1-mixed", "chain_p50_s", "E1a: interactive chain p50, s (log)", ("none", "fair"), True),
              ("e1-diamonds", "first_s", "E1b: first diamond done, s", ("none", "lineage"), False),
              ("e3", "step_p50_ms", "E3: dependent step p50, ms", ("none", "fair", "lineage"), False)]
    fig, axes = plt.subplots(1, 3, figsize=(12.0, 3.0))
    colors = {"none": a1.GRAY, "fair": a1.BLUE, "lineage": a1.ORANGE}
    for ax, (exp, key, title, pols, log) in zip(axes, panels):
        ys = []
        for i, n in enumerate((2, 4)):
            for j, p in enumerate(pols):
                y = i * (len(pols) + 1) + j
                ys.append((y, f"{n} nodes, {POLICY[p]}"))
                row = next((r for r in rows if r["exp"] == exp and r["nodes"] == n and r["policy"] == p), None)
                if row is None:
                    continue
                s = row[key]
                ax.barh(y, s["median"], height=0.8, color=colors[p])
                ax.plot([s["min"], s["max"]], [y, y], color=a1.INK, linewidth=1.0)
                ax.text(s["max"] * (1.05 if log else 1), y, f"  {s['median']:.1f}"
                        + (f" (n={row['reps']})" if row["reps"] != 3 else ""), va="center", fontsize=8.5, color=a1.INK)
        ax.set_yticks([y for y, _ in ys])
        ax.set_yticklabels([lab for _, lab in ys], fontsize=9, color=a1.INK)
        ax.invert_yaxis()
        if log:
            ax.set_xscale("log")
            ax.set_xlim(1, 300)
        else:
            ax.set_xlim(0, ax.get_xlim()[1] * 1.3)
        ax.set_title(title, loc="left", fontsize=10.5, color=a1.INK)
        ax.set_axisbelow(True)
    fig.suptitle("AsyncFlow + RHAPSODY + Dragon branch (a)+(b)+(e) on 2 and 4 RM nodes, 64 workers per node "
                 "(median of 3; line: min–max)", x=0.01, ha="left", fontsize=12, color=a1.INK)
    fig.tight_layout()
    a1.save(fig, out, "b2_af")
    plt.close(fig)


def chart_i95(plt, rows, out):
    import numpy as np

    keys = [("backend_s", "backend up"), ("trivial_s", "one trivial task"), ("workflow_s", "#95's workflow"),
            ("shutdown_s", "shutdown")]
    fig, axes = plt.subplots(1, 4, figsize=(12.0, 2.4), sharey=True)
    cats = [(r["cores"], r["config"]) for r in rows]
    colors = {"rh-0.6.0": a1.GRAY, "rh-fix": a1.BLUE}
    for ax, (k, title) in zip(axes, keys):
        for i, (c, cfg) in enumerate(cats):
            s = next(r for r in rows if (r["cores"], r["config"]) == (c, cfg))[k]
            ax.barh(i, s["median"], height=0.7, color=colors[cfg])
            ax.plot([s["min"], s["max"]], [i, i], color=a1.INK, linewidth=1.0)
            ax.text(s["max"], i, f"  {s['median']:.1f}", va="center", fontsize=8.5, color=a1.INK)
        ax.set_xlim(0, ax.get_xlim()[1] * 1.35)
        ax.set_title(f"{title}, s", loc="left", fontsize=10.5, color=a1.INK)
        ax.set_axisbelow(True)
    axes[0].set_yticks(np.arange(len(cats)))
    axes[0].set_yticklabels([f"{c} cores, {'0.6.0' if cfg == 'rh-0.6.0' else 'pool cap'}" for c, cfg in cats],
                            color=a1.INK)
    axes[0].invert_yaxis()
    fig.suptitle("rhapsody#95 on RM-shared: stock RHAPSODY 0.6.0 vs the pool cap, stock Dragon "
                 "(median of 2; line: min–max)", x=0.01, ha="left", fontsize=12, color=a1.INK)
    fig.tight_layout()
    a1.save(fig, out, "b2_i95")
    plt.close(fig)


def write_md(s, path):
    f = a1.fmt
    L = ["# Bridges-2 policy experiments, fifth campaign: summary tables", "",
         "Generated by `experiments/analyze_b2.py` from `results/b2/raw/` and `results/b2/diag/`. "
         "Median [min–max] over the runs shown.", ""]
    L += ["## 3a. Raw Batch: 2,000 trivial tasks and a chain of 100, inside 1-, 2- and 4-node runtimes", "",
          "Job 50006591, 4 RM nodes. Stock: dragonhpc 0.14.1. Branch: 0.14.1 with batch.py 0a7d87d and "
          "(a)+(b)+(e). 64 workers per node.", "",
          "| Runtime | Nodes used | Dragon | Runs | Tasks/s | Step p50 |", "|---|---|---|---|---|---|"]
    for r in s["raw_batch"]:
        L.append(f"| {r['runtime']} | {r['used']} | {r['env']} | {r['reps']} | {f(r['tasks_per_s'], digits=0)} | "
                 f"{f(r['step_p50_ms'], unit=' ms')} |")
    L += [""] + [f"Missing: {h['tag']} (rc {h['rc']}, {h['wall_s']} s) finished " +
                 ", ".join(f"the {n}-node layout ({t:.0f} tasks/s, step {st:.1f} ms)" for n, t, st in h["done"]) +
                 f", then hung in the {h['hung_in']}-node layout; no result file." for h in s["raw_hung"]]
    L += ["", "DEBUG runs (stock, one rep each, not in the table): " +
          "; ".join(f"{d['tag']}: {d['tasks_per_s']:.0f} tasks/s, step {d['step_p50_ms']:.1f} ms"
                    for d in s["raw_debug"]) + "; stock-n4-debug: no result (transport receive timeouts, killed "
          "at 660 s).", ""]
    for exp, title, cols in (
            ("e3", "## 3b. E3: a chain of 100, then 2,000 independent tasks",
             [("step_p50_ms", "Step p50", " ms", 1), ("step_p95_ms", "Step p95", " ms", 1),
              ("tasks_per_s", "Tasks/s", "", 0)]),
            ("e1-mixed", "## 3b. E1a: bulk (1,000 per 32 workers) plus 12 interactive chains; the 1-node rows are "
                         "follow-ups 2 (32 workers, job 50008020), 3 (64, job 50018097) and 4 (128, job 50076655)",
             [("chain_p50_s", "Chain p50", " s", 2), ("chain_p95_s", "Chain p95", " s", 2),
              ("bulk_makespan_s", "Bulk makespan", " s", 1)]),
            ("e1-diamonds", "## 3b. E1b: diamonds (256 per 32 workers), 1 s tasks",
             [("first_s", "First done", " s", 1), ("mean_wct_s", "Mean completion", " s", 1),
              ("makespan_s", "Makespan", " s", 1), ("peak_live_wf", "Peak live", "", 0)])):
        L += [title, "", "| Nodes | Workers | Policy | Runs | " + " | ".join(c[1] for c in cols) + " | Jobs |",
              "|---|---|---|---|" + "---|" * len(cols) + "---|"]
        for r in (x for x in s["af"] if x["exp"] == exp):
            L.append(f"| {r['nodes']} | {r['workers']} | {POLICY[r['policy']]} | {r['reps']} | " +
                     " | ".join(f(r[k], unit=u, digits=d) for k, _, u, d in cols) + f" | {', '.join(r['jobs'])} |")
        L.append("")
    L += ["Missing: E3 rep 1 on 4 nodes (job 50006595) hung after the first pool started; no result.", ""]
    L += ["## 4. rhapsody#95 on RM-shared, stock Dragon", "",
          "| Cores | RHAPSODY | Runs | Workers | Backend up | One trivial task | Workflow | Shutdown | Jobs |",
          "|---|---|---|---|---|---|---|---|---|"]
    for r in s["i95"]:
        L.append(f"| {r['cores']} | {r['config']} | {r['ok']}/{r['reps']} | {r['workers']} | "
                 f"{f(r['backend_s'], unit=' s')} | {f(r['trivial_s'], unit=' s', digits=2)} | "
                 f"{f(r['workflow_s'], unit=' s', digits=2)} | {f(r['shutdown_s'], unit=' s')} | {', '.join(r['jobs'])} |")
    L += ["", "## GPU: node-mode E2 density and stall tests (GPU-shared V100-32GB)", "",
          "| Job | Run | Mode | GPUs | Pipelines | Workers | Peak tasks at once (per GPU) | Tasks done | Outcome | "
          "OOMs | Errors |", "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in s["gpu"]:
        L.append(f"| {r['job']} | {r['run']} | {r['mode']} | {r['gpus']} | {r['pipelines']} | {r['workers']} | "
                 f"{r['peak_concurrent']} ({r['peak_per_gpu']}) | {r['tasks_done']} | {r['outcome']} | "
                 f"{'–' if r['ooms'] is None else r['ooms']} | "
                 f"{', '.join(f'{k} ×{v}' for k, v in r['errors'].items()) or '–'} |")
    L += ["", "Workers: Batch workers per node; the GPU-shared jobs held 5 cores per GPU (20 for 4 GPUs).", ""]
    tot = collections.Counter()
    L += ["## SU, from the job logs", "",
          "sacct timed out (slurmdbd), so elapsed is first run to `job done`, plus a minute of setup. RM bills "
          "128 cores per node; RM-shared its cores; GPU 1 SU per GPU-hour.", "",
          "| Job | Partition | Nodes | Minutes | SU |", "|---|---|---|---|---|"]
    for r in s["su"]:
        tot[r["kind"]] += r["su"]
        L.append(f"| {r['job']} | {r['partition']} | {r['nodes']} | {r['elapsed_min']} | {r['su']} {r['kind']} |")
    L += ["", f"Total from logs: {tot['RM']:.0f} RM SU, {tot['GPU']:.1f} GPU SU (jobs that hung before a run "
          "started are not in the logs; `projects` showed 65 RM and 3 GPU SU used before the RM sections).", ""]
    open(path, "w").write("\n".join(L))


def main():
    b2 = sys.argv[1]
    raw, diag, out = os.path.join(b2, "raw"), os.path.join(b2, "diag"), os.path.join(b2, "charts")
    os.makedirs(out, exist_ok=True)
    rb = load(os.path.join(raw, "raw"))
    s = {"raw_batch": raw_batch(rb), "raw_debug": raw_debug(rb), "raw_hung": raw_hung(diag),
         "af": af(load(os.path.join(raw, "af")) + load(os.path.join(raw, "e1one"))),
         "i95": i95(load(os.path.join(raw, "i95"))), "gpu": gpu_runs(raw, diag), "su": su(diag)}
    json.dump(s, open(os.path.join(b2, "summary.json"), "w"), indent=1, default=str)
    write_md(s, os.path.join(b2, "summary.md"))
    plt = a1.setup_mpl()
    chart_raw(plt, s["raw_batch"], out, s["raw_hung"])
    chart_af(plt, s["af"], out)
    chart_i95(plt, s["i95"], out)


if __name__ == "__main__":
    main()

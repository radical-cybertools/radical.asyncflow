#!/usr/bin/env python3
"""
plot_workflow.py — AsyncFlow workflow telemetry visualisation.

Reads a RHAPSODY JSONL checkpoint file and produces a workflow-focused PNG:

Layout
------
Row 0 (3 panels): Workflow Gantt (task lifecycle bars) · Stage Duration Distribution · Concurrency
Row 1 (3 panels): Task Lifecycle Waterfall · Dependency Wait (resolved→started) · Stage Timer (custom events)
Row 2 (full row): Task lifecycle timeline — one swim-lane per task, coloured by stage

Usage
-----
    python plot_workflow.py telemetry-output/run.jsonl
    python plot_workflow.py telemetry-output/run.jsonl --out report.png
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# ── rcParams ──────────────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.family":       "DejaVu Sans",
    "font.size":         11,
    "axes.titlesize":    13,
    "axes.titleweight":  "bold",
    "axes.labelsize":    11,
    "axes.labelweight":  "bold",
    "xtick.labelsize":   9,
    "ytick.labelsize":   9,
    "legend.fontsize":   9,
    "legend.framealpha": 0.92,
    "legend.edgecolor":  "#cccccc",
    "lines.linewidth":   2.0,
    "figure.dpi":        140,
})

BG       = "#f4f6f9"
PANEL_BG = "#ffffff"
BORDER   = "#c8d0da"
GRID_CLR = "#e2e6ea"

# Lifecycle phase colours
PHASE_COLORS = {
    "Created":   "#a29bfe",   # purple  — task registered in DAG
    "Resolved":  "#74b9ff",   # light blue — deps satisfied
    "Submitted": "#fdcb6e",   # amber   — handed to session
    "Queued":    "#fd79a8",   # pink    — in backend queue
    "Running":   "#55efc4",   # teal    — executing
    "Completed": "#00b894",   # green   — done
    "Failed":    "#d63031",   # red
    "Canceled":  "#b2bec3",   # grey
}

# Stage colours for custom etl.StageTimer events
STAGE_COLORS = {
    "ingest":          "#2e86de",
    "validate":        "#ee5a24",
    "transform":       "#009432",
    "aggregate":       "#8854d0",
    "report":          "#e17055",
    "workflow_total":  "#2d3436",
}
DEFAULT_STAGE_COLOR = "#636e72"


# ── I/O ───────────────────────────────────────────────────────────────────────

def load(path: str):
    events = []
    with open(path) as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("section") in ("event", None):
                events.append(rec)
    if not events:
        raise ValueError(f"No events found in {path}")
    return events


def session_t0(events: list[dict]) -> float:
    ss = [e for e in events if e.get("event_type") == "SessionStarted"]
    return ss[0]["event_time"] if ss else min(e["event_time"] for e in events)


# ── Data helpers ──────────────────────────────────────────────────────────────

def build_task_timelines(events: list[dict], t0: float) -> dict[str, dict]:
    """
    Returns {task_id: {phase: t_seconds, ...}} for all lifecycle phases.
    Phase names match PHASE_COLORS keys (Created, Resolved, Submitted, …).
    """
    tl: dict[str, dict] = collections.defaultdict(dict)
    for e in events:
        tid = e.get("task_id")
        if not tid:
            continue
        et = e.get("event_type", "")
        t  = e["event_time"] - t0
        # asyncflow.TaskResolved → "Resolved"
        if et == "asyncflow.TaskResolved":
            tl[tid]["Resolved"] = t
        elif et.startswith("Task"):
            phase = et.replace("Task", "")   # TaskStarted → Started
            tl[tid][phase] = t
    return dict(tl)


def build_concurrency(events: list[dict], t0: float):
    deltas = []
    for e in events:
        et = e.get("event_type", "")
        t  = e["event_time"] - t0
        if et == "TaskStarted":
            deltas.append((t, +1))
        elif et in ("TaskCompleted", "TaskFailed", "TaskCanceled"):
            deltas.append((t, -1))
    if not deltas:
        return [], []
    deltas.sort(key=lambda x: (x[0], -x[1]))
    t_c, y_c, running = [], [], 0
    for tc, d in deltas:
        t_c += [tc, tc]
        y_c += [running, running + d]
        running += d
    t_c.append(t_c[-1]); y_c.append(0)
    return t_c, y_c


def stage_timers(events: list[dict], t0: float) -> dict[str, list[float]]:
    """Return {stage: [duration_ms, ...]} from etl.StageTimer custom events."""
    stages: dict[str, list] = collections.defaultdict(list)
    for e in events:
        if e.get("event_type") == "etl.StageTimer":
            stage = e.get("stage", "unknown")
            dur   = e.get("duration_ms")
            if dur is not None:
                stages[stage].append(float(dur))
    return dict(stages)


# ── Style ──────────────────────────────────────────────────────────────────────

def _decorate(ax, title, xlabel, ylabel, grid=True, rotate_x=False):
    ax.set_facecolor(PANEL_BG)
    ax.set_title(title, pad=8, color="#2d3436")
    ax.set_xlabel(xlabel, labelpad=5)
    ax.set_ylabel(ylabel, labelpad=5)
    if rotate_x:
        ax.tick_params(axis="x", rotation=15)
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_linewidth(1.2)
        spine.set_edgecolor(BORDER)
    if grid:
        ax.grid(color=GRID_CLR, linewidth=0.7, linestyle="-", zorder=0)
        ax.set_axisbelow(True)


def _legend_below(ax, handles, labels, ncol=None):
    ncol = ncol or min(len(labels), 8)
    leg = ax.legend(handles, labels, ncol=ncol, loc="upper center",
                    bbox_to_anchor=(0.5, -0.14), frameon=True,
                    handlelength=1.8, columnspacing=1.2)
    leg.get_frame().set_linewidth(0.8)


# ── Panels ────────────────────────────────────────────────────────────────────

def _panel_gantt(ax, tl: dict, max_tasks: int = 80):
    """Horizontal Gantt: one bar per task, coloured by execution phase."""
    tasks = sorted(
        tl.items(),
        key=lambda kv: kv[1].get("Started", kv[1].get("Submitted", 0)),
    )
    if len(tasks) > max_tasks:
        tasks = tasks[:max_tasks]

    for y, (tid, phases) in enumerate(tasks):
        # Draw a bar for each consecutive phase pair
        ordered = ["Created", "Resolved", "Submitted", "Queued",
                   "Started", "Completed", "Failed", "Canceled"]
        prev_t = None
        for phase in ordered:
            t = phases.get(phase)
            if t is None:
                continue
            if prev_t is not None:
                color = PHASE_COLORS.get(phase, "#b2bec3")
                ax.barh(y, t - prev_t, left=prev_t, height=0.7,
                        color=color, edgecolor="white", linewidth=0.3, zorder=3)
            prev_t = t

    ax.set_ylim(-0.5, len(tasks) - 0.5)
    ax.set_yticks([])
    patches = [mpatches.Patch(color=v, label=k) for k, v in PHASE_COLORS.items()
               if k not in ("Failed", "Canceled")]
    ax.legend(handles=patches, fontsize=7, ncol=4,
              loc="upper right", framealpha=0.9)
    _decorate(ax, f"Task Gantt  (first {len(tasks)} tasks)",
              "Time since start (s)", "Tasks", rotate_x=True)


def _panel_stage_dist(ax, tl: dict):
    """Box-per-phase: distribution of time spent in each lifecycle phase."""
    phase_pairs = [
        ("Created→Resolved",  "Created",   "Resolved"),
        ("Resolved→Submit",   "Resolved",  "Submitted"),
        ("Submit→Start",      "Submitted", "Started"),
        ("Queued→Start",      "Queued",    "Started"),
        ("Running",           "Started",   "Completed"),
    ]
    data, labels = [], []
    for label, p1, p2 in phase_pairs:
        vals = []
        for phases in tl.values():
            t1 = phases.get(p1)
            t2 = phases.get(p2)
            if t1 is not None and t2 is not None and t2 >= t1:
                vals.append((t2 - t1) * 1000)   # ms
        if vals:
            data.append(vals)
            labels.append(label)

    if data:
        bp = ax.boxplot(data, labels=labels, patch_artist=True,
                        medianprops=dict(color="#2d3436", linewidth=2),
                        whiskerprops=dict(linewidth=1.2),
                        flierprops=dict(marker=".", markersize=4, alpha=0.5))
        colors = ["#a29bfe", "#74b9ff", "#fdcb6e", "#fd79a8", "#55efc4"]
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.75)
    ax.tick_params(axis="x", rotation=20)
    _decorate(ax, "Phase Duration Distribution", "Lifecycle phase", "Duration (ms)")


def _panel_concurrency(ax, t_c, y_c):
    if t_c:
        ax.step(t_c, y_c, color="#ee5a24", linewidth=2.2, where="post", zorder=3)
        ax.fill_between(t_c, y_c, step="post", alpha=0.18, color="#ee5a24", zorder=2)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    _decorate(ax, "Task Concurrency", "Time since start (s)", "Tasks running",
              rotate_x=True)


def _panel_waterfall(ax, tl: dict):
    """
    Per-task stacked bar showing time in each phase, sorted by total e2e time.
    Shows the workflow critical path clearly.
    """
    phase_pairs = [
        ("Dep wait",   "Created",   "Resolved"),
        ("Queue",      "Resolved",  "Submitted"),
        ("Routing",    "Submitted", "Started"),
        ("Execution",  "Started",   "Completed"),
    ]
    tasks_e2e = []
    for tid, phases in tl.items():
        t_start = phases.get("Created", phases.get("Submitted", None))
        t_end   = phases.get("Completed", phases.get("Failed", None))
        if t_start is not None and t_end is not None:
            tasks_e2e.append((tid, t_end - t_start, phases))
    tasks_e2e.sort(key=lambda x: x[1], reverse=True)
    tasks_e2e = tasks_e2e[:60]

    bottoms = np.zeros(len(tasks_e2e))
    for label, p1, p2 in phase_pairs:
        widths = []
        for _, _, phases in tasks_e2e:
            t1 = phases.get(p1)
            t2 = phases.get(p2)
            widths.append((t2 - t1) * 1000 if (t1 is not None and t2 is not None and t2 >= t1) else 0)
        color = {"Dep wait": "#a29bfe", "Queue": "#fdcb6e",
                 "Routing": "#fd79a8",  "Execution": "#55efc4"}.get(label, "#b2bec3")
        ax.barh(range(len(tasks_e2e)), widths, left=bottoms,
                color=color, edgecolor="white", linewidth=0.3,
                label=label, zorder=3, height=0.75)
        bottoms += np.array(widths)

    ax.set_yticks([])
    ax.set_ylim(-0.5, len(tasks_e2e) - 0.5)
    ax.legend(fontsize=8, loc="lower right")
    _decorate(ax, "Task Waterfall  (top 60 by e2e time)",
              "Total time (ms)", "Tasks (sorted by e2e)", rotate_x=True)


def _panel_dep_wait(ax, tl: dict):
    """Histogram of dependency wait (asyncflow.TaskResolved → TaskStarted)."""
    waits = []
    for phases in tl.values():
        t1 = phases.get("Resolved")
        t2 = phases.get("Started")
        if t1 is not None and t2 is not None and t2 >= t1:
            waits.append((t2 - t1) * 1000)
    if waits:
        nb = min(40, max(8, len(waits) // 4))
        ax.hist(waits, bins=nb, color="#74b9ff", edgecolor="white",
                linewidth=0.6, zorder=3)
        mu = np.mean(waits)
        ax.axvline(mu, color="#ee5a24", linestyle="--", linewidth=2,
                   label=f"mean {mu:.1f} ms")
        ax.axvline(np.median(waits), color="#f9ca24", linestyle="--",
                   linewidth=2, label=f"median {np.median(waits):.1f} ms")
        ax.legend(fontsize=8)
    _decorate(ax, "Dependency Wait\n(Resolved → Started)",
              "Wait time (ms)", "Tasks", rotate_x=True)


def _panel_stage_timers(ax, stages: dict):
    """Bar chart of mean stage duration from custom etl.StageTimer events."""
    order = ["ingest", "validate", "transform", "aggregate", "report", "workflow_total"]
    names, means, stds = [], [], []
    for s in order:
        if s in stages and stages[s]:
            vals = stages[s]
            names.append(s)
            means.append(np.mean(vals))
            stds.append(np.std(vals))
    # also add any stages not in the fixed order
    for s, vals in stages.items():
        if s not in order and vals:
            names.append(s)
            means.append(np.mean(vals))
            stds.append(np.std(vals))

    if names:
        colors = [STAGE_COLORS.get(n, DEFAULT_STAGE_COLOR) for n in names]
        xs = range(len(names))
        bars = ax.bar(xs, means, yerr=stds, color=colors,
                      edgecolor="white", linewidth=0.6,
                      error_kw=dict(elinewidth=1.5, capsize=4), zorder=3)
        ax.set_xticks(list(xs))
        ax.set_xticklabels(names, rotation=20, ha="right")
        for bar, m in zip(bars, means):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(means) * 0.02,
                    f"{m:.0f}", ha="center", va="bottom", fontsize=8,
                    fontweight="bold")
    _decorate(ax, "Stage Timer  (mean ± std, ms)\netl.StageTimer events",
              "Stage", "Duration (ms)", rotate_x=False)


def _panel_swimlane(ax, tl: dict, max_tasks: int = 100):
    """
    Full-width swim-lane: each task is one horizontal row, coloured segments
    show Created→Resolved (dep wait), Resolved→Started (queue+routing),
    Started→Completed (execution).
    """
    tasks = sorted(
        tl.items(),
        key=lambda kv: kv[1].get("Created", kv[1].get("Submitted", 0)),
    )
    if len(tasks) > max_tasks:
        tasks = tasks[:max_tasks]

    seg_defs = [
        ("Dep wait",  "Created",   "Resolved",   "#a29bfe"),
        ("Pre-exec",  "Resolved",  "Started",    "#fdcb6e"),
        ("Execution", "Started",   "Completed",  "#00b894"),
        ("Failed",    "Started",   "Failed",     "#d63031"),
    ]

    for y, (tid, phases) in enumerate(tasks):
        for label, p1, p2, color in seg_defs:
            t1 = phases.get(p1)
            t2 = phases.get(p2)
            if t1 is not None and t2 is not None and t2 > t1:
                ax.barh(y, t2 - t1, left=t1, height=0.72,
                        color=color, edgecolor="none", zorder=3)

    # Legend patches
    patches = [mpatches.Patch(color=c, label=l)
               for l, _, _, c in seg_defs[:3]]
    handles, labels_l = patches, [p.get_label() for p in patches]
    _legend_below(ax, handles, labels_l, ncol=4)

    ax.set_ylim(-0.5, len(tasks) - 0.5)
    ax.set_yticks([])
    _decorate(ax,
              f"Task Lifecycle Swim-lanes  ({len(tasks)} tasks)  —  "
              "purple=dep wait · amber=pre-exec · green=execution",
              "Time since session start (s)", "Tasks (sorted by creation time)",
              rotate_x=False)


def _panel_conc_resources(ax, events: list[dict], t0: float, t_c, y_c):
    """
    Full-width bottom row: concurrency step (left axis, blue fill) +
    per-node CPU % (amber, dotted+circle markers) and Memory %
    (red, dotted+square markers) on the right axis.

    Mirrors the bottom panel of dag_dashboard_concurrent.png.
    """
    ax2 = ax.twinx()

    # ── Concurrency (left) ────────────────────────────────────────────────────
    if t_c:
        # convert to ms to match the reference image x-axis feel
        t_ms = [v * 1000 for v in t_c]
        ax.fill_between(t_ms, y_c, step="post", alpha=0.35,
                        color="#74b9ff", zorder=2)
        ax.step(t_ms, y_c, color="#2e86de", linewidth=2.2, where="post",
                label="Tasks running", zorder=3)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax.set_ylim(bottom=0, top=max(y_c) * 1.18 if y_c else 1)
    ax.set_ylabel("Tasks running concurrently", labelpad=8, color="#2e86de")
    ax.tick_params(axis="y", labelcolor="#2e86de")

    # ── Resource lines (right) ────────────────────────────────────────────────
    nodes = sorted({e["node_id"] for e in events
                    if e.get("event_type") == "ResourceUpdate" and e.get("node_id")})

    NODE_PALETTE_LOCAL = ["#ee5a24", "#009432", "#8854d0", "#2d3436"]
    CPU_STYLES = [
        dict(color="#f9ca24", linestyle=":",  marker="o", markersize=4),
        dict(color="#fdcb6e", linestyle=":",  marker="o", markersize=4),
        dict(color="#ffeaa7", linestyle=":",  marker="o", markersize=4),
    ]
    MEM_STYLES = [
        dict(color="#ee5a24", linestyle=":",  marker="s", markersize=4),
        dict(color="#d63031", linestyle=":",  marker="s", markersize=4),
        dict(color="#ff7675", linestyle=":",  marker="s", markersize=4),
    ]

    for k, node in enumerate(nodes):
        short = node.split(".")[0]
        ne = sorted(
            [e for e in events
             if e.get("event_type") == "ResourceUpdate"
             and e.get("node_id") == node
             and e.get("gpu_id") is None],
            key=lambda e: e["event_time"],
        )
        if not ne:
            continue
        ts  = [(e["event_time"] - t0) * 1000 for e in ne]
        cpu = [e.get("cpu_percent") or 0.0 for e in ne]
        mem = [e.get("memory_percent") or 0.0 for e in ne]

        cs = CPU_STYLES[k % len(CPU_STYLES)]
        ms = MEM_STYLES[k % len(MEM_STYLES)]

        ax2.plot(ts, cpu, linewidth=1.8, label=f"{short} CPU %",
                 alpha=0.9, zorder=4, **cs)
        ax2.plot(ts, mem, linewidth=1.8, label=f"{short} Memory %",
                 alpha=0.9, zorder=4, **ms)

    ax2.set_ylim(0, 110)
    ax2.set_ylabel("Resource utilization (%)", labelpad=10, color="#555")
    ax2.tick_params(axis="y", labelcolor="#555")
    for side, vis in [("top", False), ("left", False),
                      ("right", True), ("bottom", True)]:
        ax2.spines[side].set_visible(vis)
        if vis:
            ax2.spines[side].set_linewidth(1.2)
            ax2.spines[side].set_edgecolor(BORDER)

    # Unified legend
    lines1, labs1 = ax.get_legend_handles_labels()
    lines2, labs2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labs1 + labs2,
              fontsize=8.5, ncol=min(len(labs1 + labs2), 6),
              loc="upper right", framealpha=0.92)

    _decorate(ax, "Concurrency Profile  +  Node Resource Utilization",
              "Time since session start (ms)", "Tasks running concurrently",
              rotate_x=False)


# ── Main ──────────────────────────────────────────────────────────────────────

def plot(path: str, out: str | None = None, dpi: int = 140) -> None:
    events = load(path)
    t0     = session_t0(events)
    name   = Path(path).stem

    tl       = build_task_timelines(events, t0)
    t_c, y_c = build_concurrency(events, t0)
    stages   = stage_timers(events, t0)

    fig = plt.figure(figsize=(26, 38), facecolor=BG)
    fig.suptitle(
        f"AsyncFlow Workflow Telemetry  ·  {name}",
        fontsize=17, fontweight="bold", y=0.999, color="#2d3436",
    )

    gs = gridspec.GridSpec(
        4, 3, figure=fig,
        hspace=0.78, wspace=0.40,
        top=0.970, bottom=0.055,
        left=0.07, right=0.96,
        height_ratios=[1, 1, 1.4, 0.9],
    )

    # Row 0
    _panel_gantt(        fig.add_subplot(gs[0, 0]), tl)
    _panel_stage_dist(   fig.add_subplot(gs[0, 1]), tl)
    _panel_concurrency(  fig.add_subplot(gs[0, 2]), t_c, y_c)

    # Row 1
    _panel_waterfall(    fig.add_subplot(gs[1, 0]), tl)
    _panel_dep_wait(     fig.add_subplot(gs[1, 1]), tl)
    _panel_stage_timers( fig.add_subplot(gs[1, 2]), stages)

    # Row 2 — full-width swim-lane
    _panel_swimlane(fig.add_subplot(gs[2, :]), tl)

    # Row 3 — full-width concurrency + node resources
    _panel_conc_resources(fig.add_subplot(gs[3, :]), events, t0, t_c, y_c)

    out_path = out or str(Path(path).with_suffix(".png"))
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor=BG)
    print(f"Saved → {out_path}")
    plt.close(fig)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Plot AsyncFlow workflow telemetry from a JSONL file."
    )
    ap.add_argument("file", nargs="?",
                    default="telemetry-output/out.jsonl",
                    metavar="FILE.jsonl")
    ap.add_argument("--out", metavar="FILE",
                    help="Output PNG path (default: same name as input)")
    ap.add_argument("--dpi", type=int, default=140)
    args = ap.parse_args()

    p = Path(args.file)
    if not p.exists():
        print(f"error: file not found: {p}", file=sys.stderr)
        sys.exit(1)

    plot(str(p), out=args.out, dpi=args.dpi)


if __name__ == "__main__":
    main()

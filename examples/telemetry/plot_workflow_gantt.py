#!/usr/bin/env python3
"""plot_workflow_gantt.py — Workflow-grouped telemetry dashboard.

Reads a RHAPSODY JSONL checkpoint produced by ``01-workflow_grouping.py`` and
renders a six-panel PNG:

  1. Gantt          — one row per workflow instance, bars coloured by stage.
                      Lighter band shows dependency-wait before each stage.
  2. Stage Breakdown— stacked bars: execution time per stage per workflow.
  3. Workflow Concurrency
                    — how many workflows are in-flight simultaneously.
  4. Workflow Throughput
                    — workflows completed per time bin (completion histogram).
  5. E2E Latency Distribution
                    — histogram of total elapsed time per workflow instance.
  6. Stage × Time Heatmap
                    — active tasks per stage per time slice; reveals pipeline
                      saturation and bottleneck stages.

The ``asyncflow.workflow_id`` attribute stamped on every task event by
``workflow_scope()`` drives all grouping — no changes to task functions needed.

Usage
-----
    python plot_workflow_gantt.py <output_dir>/
    python plot_workflow_gantt.py <output_dir>/ --out dashboard.png
    python plot_workflow_gantt.py session.jsonl --dpi 180
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

# ── Theme ─────────────────────────────────────────────────────────────────────

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.labelsize": 10,
        "axes.labelweight": "bold",
        "xtick.labelsize": 8.5,
        "ytick.labelsize": 8.5,
        "legend.fontsize": 8.5,
        "legend.framealpha": 0.92,
        "lines.linewidth": 2.0,
        "figure.dpi": 140,
    }
)

BG = "#f4f6f9"
PANEL_BG = "#ffffff"
BORDER = "#c8d0da"
GRID = "#e2e6ea"

STAGE_NAMES = ["load", "preprocess", "train", "evaluate"]
STAGE_COLORS = ["#2e86de", "#ee5a24", "#009432", "#8854d0"]
DEP_WAIT_COLOR = "#dfe6e9"


# ── I/O helpers ───────────────────────────────────────────────────────────────


def _find_jsonl(path: str) -> Path:
    p = Path(path)
    if p.is_file():
        return p
    matches = sorted(p.glob("*.jsonl"))
    if not matches:
        print(f"error: no .jsonl file found in {p}", file=sys.stderr)
        sys.exit(1)
    return matches[-1]


def load_events(jsonl: Path) -> list[dict]:
    events = []
    with open(jsonl) as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("section") in ("event", None):
                events.append(rec)
    if not events:
        print(f"error: no events in {jsonl}", file=sys.stderr)
        sys.exit(1)
    return events


def session_t0(events: list[dict]) -> float:
    ss = [e for e in events if e.get("event_type") == "SessionStarted"]
    return ss[0]["event_time"] if ss else min(e["event_time"] for e in events)


# ── Data model ────────────────────────────────────────────────────────────────


def build_workflow_tasks(events: list[dict], t0: float) -> dict[str, list[dict]]:
    """Group task lifecycle dicts by asyncflow.workflow_id.

    Returns {wf_id: [task_dict, ...]}, tasks sorted by TaskCreated time.
    Each task_dict: task_id, workflow_id, created, resolved, submitted,
                   started, completed, failed, canceled  (seconds rel. to t0).
    """
    ET_MAP = {
        "TaskCreated": "created",
        "asyncflow.TaskResolved": "resolved",
        "TaskSubmitted": "submitted",
        "TaskStarted": "started",
        "TaskCompleted": "completed",
        "TaskFailed": "failed",
        "TaskCanceled": "canceled",
    }

    by_task: dict[str, dict] = collections.defaultdict(
        lambda: dict(
            task_id=None,
            workflow_id=None,
            created=None,
            resolved=None,
            submitted=None,
            started=None,
            completed=None,
            failed=None,
            canceled=None,
        )
    )

    for e in events:
        et = e.get("event_type", "")
        tid = e.get("task_id")
        if not tid or et not in ET_MAP:
            continue
        td = by_task[tid]
        td["task_id"] = tid
        td[ET_MAP[et]] = e["event_time"] - t0
        wid = e.get("attributes", {}).get("asyncflow.workflow_id")
        if wid:
            td["workflow_id"] = wid

    by_wf: dict[str, list[dict]] = collections.defaultdict(list)
    for td in by_task.values():
        if td["workflow_id"]:
            by_wf[td["workflow_id"]].append(td)

    for wid in by_wf:
        by_wf[wid].sort(key=lambda x: x["created"] or 0)

    return dict(by_wf)


def _wf_span(tasks: list[dict]) -> tuple[float | None, float | None]:
    """(earliest created, latest completed) for a workflow's task list."""
    starts = [t["created"] for t in tasks if t["created"] is not None]
    ends = [t["completed"] or t["failed"] for t in tasks if (t["completed"] or t["failed"])]
    return (min(starts) if starts else None, max(ends) if ends else None)


def build_wf_concurrency(by_wf: dict[str, list[dict]]):
    """Step function: number of in-flight workflows over time."""
    deltas = []
    for tasks in by_wf.values():
        t_s, t_e = _wf_span(tasks)
        if t_s is not None:
            deltas.append((t_s, +1))
        if t_e is not None:
            deltas.append((t_e, -1))
    if not deltas:
        return [], []
    deltas.sort(key=lambda x: (x[0], -x[1]))
    t_out, y_out, running = [], [], 0
    for tc, d in deltas:
        t_out += [tc, tc]
        y_out += [running, running + d]
        running += d
    t_out.append(t_out[-1])
    y_out.append(0)
    return t_out, y_out


def build_stage_heatmap(
    by_wf: dict[str, list[dict]], n_bins: int = 40
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """2-D array [stages × time_bins] counting tasks active per cell.

    Returns: (matrix, time_edges, stage_names)
    """
    all_t: list[float] = []
    for tasks in by_wf.values():
        for td in tasks:
            if td["started"] is not None:
                all_t.append(td["started"])
            t_e = td.get("completed") or td.get("failed")
            if t_e is not None:
                all_t.append(t_e)
    if not all_t:
        return np.zeros((len(STAGE_NAMES), n_bins)), np.linspace(0, 1, n_bins + 1), STAGE_NAMES

    t_min, t_max = min(all_t), max(all_t)
    edges = np.linspace(t_min, t_max, n_bins + 1)
    bin_w = edges[1] - edges[0]
    mat = np.zeros((len(STAGE_NAMES), n_bins))

    for tasks in by_wf.values():
        for stage_idx, td in enumerate(tasks):
            if stage_idx >= len(STAGE_NAMES):
                break
            t_s = td.get("started")
            t_e = td.get("completed") or td.get("failed")
            if t_s is None or t_e is None:
                continue
            # Distribute task's execution uniformly across the bins it overlaps
            for b in range(n_bins):
                b_lo, b_hi = edges[b], edges[b + 1]
                overlap = max(0.0, min(t_e, b_hi) - max(t_s, b_lo))
                if overlap > 0:
                    mat[stage_idx, b] += overlap / bin_w  # fractional occupancy

    return mat, edges, STAGE_NAMES


# ── Style helpers ─────────────────────────────────────────────────────────────


def _decorate(ax, title, xlabel="Time since start (s)", ylabel="", grid=True):
    ax.set_facecolor(PANEL_BG)
    ax.set_title(title, pad=7, color="#2d3436")
    ax.set_xlabel(xlabel, labelpad=4)
    ax.set_ylabel(ylabel, labelpad=4)
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_linewidth(1.1)
        spine.set_edgecolor(BORDER)
    if grid:
        ax.grid(color=GRID, linewidth=0.6, linestyle="-", zorder=0)
        ax.set_axisbelow(True)


# ── Panels ────────────────────────────────────────────────────────────────────


def _panel_gantt(ax, by_wf: dict[str, list[dict]]):
    wf_ids = sorted(by_wf.keys())
    stage_patches = [
        mpatches.Patch(color=STAGE_COLORS[i], label=STAGE_NAMES[i])
        for i in range(len(STAGE_NAMES))
    ]
    dep_patch = mpatches.Patch(color=DEP_WAIT_COLOR, label="dep wait")

    for row, wid in enumerate(wf_ids):
        for stage_idx, td in enumerate(by_wf[wid]):
            color = STAGE_COLORS[stage_idx % len(STAGE_COLORS)]
            t_s, t_e = td.get("started"), td.get("completed") or td.get("failed")
            t_c = td.get("created")

            if t_s is None:
                continue
            if t_c is not None and t_s > t_c:
                ax.barh(
                    row, t_s - t_c, left=t_c, height=0.55,
                    color=DEP_WAIT_COLOR, alpha=0.85, edgecolor="none", zorder=2,
                )
            if t_e is not None and t_e > t_s:
                ax.barh(
                    row, t_e - t_s, left=t_s, height=0.65,
                    color=color, edgecolor="white", linewidth=0.4, zorder=3,
                )

    ax.set_yticks(range(len(wf_ids)))
    ax.set_yticklabels(wf_ids, fontsize=9)
    ax.set_ylim(-0.6, len(wf_ids) - 0.4)
    ax.legend(
        handles=stage_patches + [dep_patch],
        ncol=len(stage_patches) + 1, fontsize=8,
        loc="upper right", framealpha=0.92,
    )
    _decorate(ax, "Workflow Gantt  —  tasks grouped by workflow_scope()", ylabel="Workflow")


def _panel_stage_breakdown(ax, by_wf: dict[str, list[dict]]):
    wf_ids = sorted(by_wf.keys())
    n = len(wf_ids)
    lefts = np.zeros(n)

    for stage_idx, stage_name in enumerate(STAGE_NAMES):
        widths = []
        for wid in wf_ids:
            tasks = by_wf[wid]
            td = tasks[stage_idx] if stage_idx < len(tasks) else {}
            t_s, t_e = td.get("started"), td.get("completed") or td.get("failed")
            widths.append((t_e - t_s) * 1000 if (t_s and t_e) else 0.0)

        ax.barh(
            range(n), widths, left=lefts,
            color=STAGE_COLORS[stage_idx], edgecolor="white", linewidth=0.5,
            label=stage_name, height=0.55, zorder=3,
        )
        for row, (left, w) in enumerate(zip(lefts, widths)):
            if w > 8:
                ax.text(
                    left + w / 2, row, f"{w:.0f}",
                    ha="center", va="center", fontsize=7, color="white",
                    fontweight="bold", zorder=4,
                )
        lefts += np.array(widths)

    ax.set_yticks(range(n))
    ax.set_yticklabels(wf_ids, fontsize=9)
    ax.set_ylim(-0.6, n - 0.4)
    ax.legend(ncol=len(STAGE_NAMES), fontsize=8, loc="lower right")
    _decorate(ax, "Stage Execution Time per Workflow  (ms)", xlabel="Execution time (ms)", ylabel="Workflow")


def _panel_wf_concurrency(ax, t_wf, y_wf):
    if t_wf:
        ax.step(t_wf, y_wf, color="#6c5ce7", linewidth=2.2, where="post", zorder=3)
        ax.fill_between(t_wf, y_wf, step="post", alpha=0.22, color="#6c5ce7", zorder=2)
        ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
        ax.set_ylim(bottom=0)
    _decorate(ax, "Workflow Concurrency\n(in-flight simultaneously)", ylabel="Workflows running")


def _panel_wf_throughput(ax, by_wf: dict[str, list[dict]], n_bins: int = 12):
    """Histogram of workflow completion times."""
    completions = []
    for tasks in by_wf.values():
        _, t_e = _wf_span(tasks)
        if t_e is not None:
            completions.append(t_e)

    if completions:
        ax.hist(
            completions, bins=n_bins,
            color="#00b894", edgecolor="white", linewidth=0.7, zorder=3,
        )
        ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    _decorate(
        ax,
        "Workflow Throughput\n(completions per time bin)",
        ylabel="Workflows completed",
    )


def _panel_e2e_latency(ax, by_wf: dict[str, list[dict]]):
    """Histogram of end-to-end elapsed time per workflow."""
    latencies_ms = []
    for wid, tasks in by_wf.items():
        t_s, t_e = _wf_span(tasks)
        if t_s is not None and t_e is not None:
            latencies_ms.append((t_e - t_s) * 1000)

    if latencies_ms:
        n_bins = min(max(6, len(latencies_ms)), 20)
        ax.hist(
            latencies_ms, bins=n_bins,
            color="#fdcb6e", edgecolor="white", linewidth=0.7, zorder=3,
        )
        mu = np.mean(latencies_ms)
        ax.axvline(mu, color="#d63031", linestyle="--", linewidth=1.8,
                   label=f"mean {mu:.0f} ms", zorder=4)
        if len(latencies_ms) > 1:
            ax.axvline(np.median(latencies_ms), color="#2d3436", linestyle=":",
                       linewidth=1.6, label=f"median {np.median(latencies_ms):.0f} ms", zorder=4)
        ax.legend(fontsize=8)
        ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    _decorate(
        ax,
        "E2E Workflow Latency Distribution",
        xlabel="Elapsed time (ms)",
        ylabel="Workflows",
    )


def _panel_heatmap(ax, mat: np.ndarray, edges: np.ndarray, stage_names: list[str]):
    """Stage × time heatmap: fractional task occupancy per cell."""
    if mat.max() == 0:
        ax.text(0.5, 0.5, "no data", ha="center", va="center",
                transform=ax.transAxes, fontsize=12, color="#636e72")
        _decorate(ax, "Stage × Time Throughput Heatmap", grid=False)
        return

    im = ax.imshow(
        mat,
        aspect="auto",
        origin="lower",
        cmap="YlOrRd",
        vmin=0,
        extent=[edges[0], edges[-1], -0.5, len(stage_names) - 0.5],
        interpolation="nearest",
    )
    ax.set_yticks(range(len(stage_names)))
    ax.set_yticklabels(stage_names, fontsize=9)
    ax.set_ylim(-0.5, len(stage_names) - 0.5)

    cbar = ax.get_figure().colorbar(im, ax=ax, pad=0.01, fraction=0.025)
    cbar.set_label("Tasks active (fractional)", fontsize=8)
    cbar.ax.tick_params(labelsize=7)

    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_linewidth(1.1)
        spine.set_edgecolor(BORDER)
    ax.set_facecolor(PANEL_BG)
    ax.set_title("Stage × Time Throughput Heatmap", pad=7, color="#2d3436")
    ax.set_xlabel("Time since start (s)", labelpad=4)
    ax.set_ylabel("Pipeline stage", labelpad=4)


# ── Main ──────────────────────────────────────────────────────────────────────


def plot(jsonl: Path, out: str | None, dpi: int) -> None:
    events = load_events(jsonl)
    t0 = session_t0(events)
    name = jsonl.stem

    by_wf = build_workflow_tasks(events, t0)
    if not by_wf:
        print(
            "warning: no asyncflow.workflow_id attributes found — "
            "did you run 01-workflow_grouping.py?",
            file=sys.stderr,
        )

    t_wf, y_wf = build_wf_concurrency(by_wf)
    mat, edges, snames = build_stage_heatmap(by_wf)
    n_wf = len(by_wf)
    n_tasks = sum(len(t) for t in by_wf.values())

    gantt_h = max(1.6, n_wf * 0.52 + 0.8)
    fig = plt.figure(figsize=(20, gantt_h * 2 + 14), facecolor=BG)
    fig.suptitle(
        f"AsyncFlow Workflow Telemetry  ·  {name}"
        f"  ({n_wf} workflows · {n_tasks} tasks)",
        fontsize=15, fontweight="bold", y=0.999, color="#2d3436",
    )

    gs = gridspec.GridSpec(
        3, 2,
        figure=fig,
        hspace=0.62,
        wspace=0.38,
        top=0.965,
        bottom=0.05,
        left=0.10,
        right=0.97,
        height_ratios=[gantt_h, gantt_h, 2.2],
    )

    _panel_gantt(fig.add_subplot(gs[0, :]), by_wf)
    _panel_stage_breakdown(fig.add_subplot(gs[1, 0]), by_wf)

    # Right column: three stacked panels
    gs_right = gridspec.GridSpecFromSubplotSpec(
        3, 1, subplot_spec=gs[1, 1], hspace=0.75, height_ratios=[1, 1, 1]
    )
    _panel_wf_concurrency(fig.add_subplot(gs_right[0]), t_wf, y_wf)
    _panel_wf_throughput(fig.add_subplot(gs_right[1]), by_wf)
    _panel_e2e_latency(fig.add_subplot(gs_right[2]), by_wf)

    _panel_heatmap(fig.add_subplot(gs[2, :]), mat, edges, snames)

    out_path = out or str(jsonl.with_suffix(".png"))
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor=BG)
    print(f"Saved → {out_path}")
    plt.close(fig)


# ── CLI ───────────────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Plot workflow-grouped AsyncFlow telemetry from a JSONL checkpoint."
    )
    ap.add_argument(
        "path",
        metavar="DIR_OR_FILE",
        help=(
            "Output directory produced by 01-workflow_grouping.py "
            "(the .jsonl file inside is auto-detected), or a .jsonl file directly."
        ),
    )
    ap.add_argument(
        "--out",
        metavar="FILE",
        default=None,
        help="Output PNG path (default: <jsonl_stem>.png next to the input).",
    )
    ap.add_argument("--dpi", type=int, default=140, help="Image DPI (default: 140).")
    args = ap.parse_args()

    jsonl = _find_jsonl(args.path)
    plot(jsonl, out=args.out, dpi=args.dpi)


if __name__ == "__main__":
    main()

"""Diagnostic D (third campaign): the cost of one Dragon process per task (V1-style) vs Batch.

Raw Dragon, no AsyncFlow or RHAPSODY, on one node. Four task kinds: a Python function
``f(x) -> x + 1`` and the same after ``time.sleep(1)``; the executables ``/bin/true`` and
``sleep 1``. A closed loop keeps ``--inflight`` tasks running: a new task starts as soon as one
completes.

``--mode v1`` (RHAPSODY <= 0.4.0's DragonExecutionBackendV1 style): one
``dragon.native.process.Process`` per task, no placement policy. Its target is ``body()``, which
records when it starts and ends and puts ``(uid, t_start, t_end, pid)`` on one shared
``dragon.native.queue.Queue``. An executable runs inside ``body()`` with ``subprocess.run``: a
small Python launcher that times the child. A reaper thread joins every process.

``--mode batch``: ``Batch.function(body_fn)`` for functions and ``Batch.process`` with
``launch_exe.py`` (the same launcher, writing its times to a file) for executables, with
``--batch-kwargs``; completions come from ``Batch.poll()``.

Per configuration: the first task's latency (cold start), then, without the first 5 tasks:
launch latency (client submit or ``start()`` to the body's first line), report latency (body
end to the client seeing it), ``start()`` blocking time (v1), steady tasks per second, busy %
(sum of body times / (in-flight x wall)), failures and processes left alive.
"""

import argparse
import json
import multiprocessing as mp
import os
import queue
import statistics
import subprocess
import sys
import threading
import time

import dragon  # noqa: F401
from common import log, pct
from dragon.native.machine import Node

HERE = os.path.dirname(os.path.abspath(__file__))
KINDS = {  # kind: (executable?, seconds)
    "func": (False, 0.0), "func_1s": (False, 1.0), "exe_true": (True, 0.0), "exe_sleep1": (True, 1.0),
}


def f(x):
    return x + 1


def run_kind(kind):
    exe, secs = KINDS[kind]
    if exe:
        return subprocess.run(["/bin/true"] if secs == 0 else ["sleep", "1"]).returncode
    if secs:
        time.sleep(secs)
    return f(1) - 2  # 0


def body(q, uid, kind):
    t0 = time.time()
    rc = run_kind(kind)
    q.put((uid, t0, time.time(), os.getpid(), rc))


def body_fn(kind):
    t0 = time.time()
    rc = run_kind(kind)
    return (t0, time.time(), os.getpid(), rc)


def stats(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return {"p50": pct(xs, 50), "p95": pct(xs, 95), "max": max(xs), "mean": statistics.mean(xs), "n": len(xs)}


def summarize(rows, inflight, warm=5):
    rows = sorted(rows, key=lambda r: r["seen"])
    steady = rows[warm:]
    out = {"tasks": len(rows), "failures": sum(r["rc"] != 0 for r in rows),
           "cold_launch_ms": (rows[0]["t0"] - rows[0]["submit"]) * 1e3 if rows else None,
           "cold_report_ms": (rows[0]["seen"] - rows[0]["t1"]) * 1e3 if rows else None}
    if len(steady) > 1:
        wall = steady[-1]["seen"] - steady[0]["seen"]
        out["launch_ms"] = stats([(r["t0"] - r["submit"]) * 1e3 for r in steady])
        out["report_ms"] = stats([(r["seen"] - r["t1"]) * 1e3 for r in steady])
        out["start_block_ms"] = stats([r.get("start_block_ms") for r in steady])
        out["tasks_per_s"] = (len(steady) - 1) / wall if wall > 0 else None
        busy = sum(r["t1"] - r["t0"] for r in steady[1:])
        out["busy_pct"] = 100.0 * busy / (inflight * wall) if wall > 0 else None
    return out


def v1_loop(kind, inflight, count, limit_s):
    from dragon.native.process import Process
    from dragon.native.queue import Queue

    q = Queue()
    procs, rows, alive, every = {}, {}, set(), []
    reap_q = queue.Queue()

    def reaper():
        while True:
            p = reap_q.get()
            if p is None:
                return
            p.join()

    th = threading.Thread(target=reaper, daemon=True)
    th.start()
    nxt = 0

    def start_one():
        nonlocal nxt
        uid = nxt
        nxt += 1
        p = Process(target=body, args=(q, uid, kind))
        t = time.time()
        p.start()
        rows[uid] = {"submit": t, "start_block_ms": (time.time() - t) * 1e3}
        procs[uid] = p
        every.append(p)
        alive.add(uid)

    t_end = time.time() + limit_s
    for _ in range(min(inflight, count)):
        start_one()
    done = 0
    while done < nxt:
        uid, t0, t1, pid, rc = q.get(timeout=120)
        seen = time.time()
        rows[uid].update(t0=t0, t1=t1, seen=seen, rc=rc, pid=pid)
        alive.discard(uid)
        reap_q.put(procs.pop(uid))
        done += 1
        if nxt < count and time.time() < t_end:
            start_one()
    reap_q.put(None)
    th.join(timeout=120)
    def is_alive(p):
        a = p.is_alive
        return a() if callable(a) else a

    left = sum(1 for p in every if is_alive(p))
    return list(rows.values()), {"left_alive": left, "reaper_alive": th.is_alive()}


def batch_loop(batch, kind, inflight, count, limit_s, out_dir):
    from dragon.native.process import ProcessTemplate

    exe, secs = KINDS[kind]
    tasks, rows = {}, {}
    nxt = 0

    def submit_one():
        nonlocal nxt
        uid = nxt
        nxt += 1
        t = time.time()
        if exe:
            path = os.path.join(out_dir, f"{kind}_{uid}.json")
            cmd = ["/bin/true"] if secs == 0 else ["sleep", "1"]
            task = batch.process(ProcessTemplate(target=sys.executable,
                                                 args=[os.path.join(HERE, "launch_exe.py"), path] + cmd))
            rows[uid] = {"submit": t, "path": path}
        else:
            task = batch.function(body_fn, kind)
            rows[uid] = {"submit": t}
        tasks[task.uid] = (uid, task)

    t_end = time.time() + limit_s
    for _ in range(min(inflight, count)):
        submit_one()
    done = 0
    while done < nxt:
        tuid = batch.poll(timeout=120)
        if tuid is None:
            raise TimeoutError("no completion in 120 s")
        seen = time.time()
        if tuid not in tasks:
            continue
        uid, task = tasks.pop(tuid)
        r = rows[uid]
        if exe:
            rc = task.get(block=True)
            rec = json.load(open(r["path"]))
            r.update(t0=rec["t0"], t1=rec["t1"], rc=rc if isinstance(rc, int) and rc else rec["rc"], pid=rec["pid"])
            os.remove(r["path"])
        else:
            t0, t1, pid, rc = task.get(block=True)
            r.update(t0=t0, t1=t1, rc=rc, pid=pid)
        r["seen"] = seen
        done += 1
        if nxt < count and time.time() < t_end:
            submit_one()
    return list(rows.values()), {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("v1", "batch"), required=True)
    ap.add_argument("--inflight", default="1,8,32")
    ap.add_argument("--kinds", default="func,func_1s,exe_true,exe_sleep1")
    ap.add_argument("--trivial-count", type=int, default=300)
    ap.add_argument("--limit-s", type=float, default=150.0)
    ap.add_argument("--batch-kwargs", default="{}")
    ap.add_argument("--rep", type=int, default=1)
    args = ap.parse_args()
    mp.set_start_method("dragon")
    num_cpus = Node.num_cpus
    Node.num_cpus = property(lambda n: min(num_cpus.fget(n), len(n.cpus) or num_cpus.fget(n)))
    run_dir = os.environ.get("RUN_DIR", ".")
    out = {"mode": args.mode, "rep": args.rep, "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
           "dragon": os.environ.get("DRAGON_VERSION"), "batch_kwargs": json.loads(args.batch_kwargs),
           "configs": []}
    batch = None
    if args.mode == "batch":
        from dragon.workflows.batch import Batch

        batch = Batch(**json.loads(args.batch_kwargs))
        out["workers"] = batch.num_workers
        for t in [batch.function(body_fn, "func") for _ in range(batch.num_workers)]:
            t.get(block=True)
        while batch.poll(timeout=0.2) is not None:  # drop the warm-up's completions
            pass
    kinds = args.kinds.split(",")
    if args.rep % 2 == 0:
        kinds = kinds[::-1]
    for kind in kinds:
        for k in [int(x) for x in args.inflight.split(",")]:
            secs = KINDS[kind][1]
            count = args.trivial_count if secs == 0 else max(30, 10 * k)
            t = time.time()
            try:
                if args.mode == "v1":
                    rows, extra = v1_loop(kind, k, count, args.limit_s)
                else:
                    rows, extra = batch_loop(batch, kind, k, count, args.limit_s, run_dir)
                err = None
            except Exception as e:  # noqa: BLE001 - record and go on
                rows, extra, err = [], {}, repr(e)[:500]
            s = summarize(rows, k)
            s.update(kind=kind, inflight=k, planned=count, wall_s=time.time() - t, error=err, **extra)
            s["raw"] = [{kk: r.get(kk) for kk in ("submit", "t0", "t1", "seen", "rc", "start_block_ms")} for r in rows]
            out["configs"].append(s)
            lm, rm = s.get("launch_ms") or {}, s.get("report_ms") or {}
            log(f"{args.mode} {kind} x{k}: {s['tasks']} tasks, launch p50 {lm.get('p50', float('nan')):.1f} ms "
                f"p95 {lm.get('p95', float('nan')):.1f}, report p50 {rm.get('p50', float('nan')):.1f} ms, "
                f"{(s.get('tasks_per_s') or 0):.1f} tasks/s, busy {(s.get('busy_pct') or 0):.0f}%, "
                f"cold {s['cold_launch_ms'] or 0:.0f} ms, failures {s['failures']}, err {err}")
    if batch is not None:
        batch.join()
        batch.destroy()
    path = os.path.join(os.environ.get("RESULTS_DIR", "."), f"launch_{args.mode}_r{args.rep}_{out['slurm_job_id']}.json")
    with open(path, "w") as f:
        json.dump(out, f)
    log(f"wrote {path}")


if __name__ == "__main__":
    main()

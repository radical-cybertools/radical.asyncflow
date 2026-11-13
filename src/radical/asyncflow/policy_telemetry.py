"""Telemetry for ready-queue policies: what the policy decided, when, and why.

With telemetry on (``WorkflowEngine.start_telemetry()``) and a policy set, the engine
wraps its policy in a :class:`TracedPolicy`. The proxy forwards every operation and
emits, next to the engine's own task events:

- ``asyncflow.TaskDispatched``: the policy dispatched the task and the engine submits
  it. ``held_seconds`` is its time in the policy since ``enqueue`` (None for a task
  queued before tracing started); ``key`` is the policy's ordering key or reason;
  ``placement`` and ``charge`` say where and with what the policy admitted it;
  ``waiting``, ``inflight`` and ``occupancy`` describe the policy after the dispatch
  round.
- ``asyncflow.TaskReleased``: the task left the policy. ``state`` and ``observed``
  are what the engine passed to ``release``: how the task ended on the backend
  (``"DONE"``, ``"FAILED"``, ``"CANCELLED"``, or None if the backend never got it)
  and the resource use it reported. ``run_seconds`` is the time since dispatch.
  ``dispatched`` is False for a task that never reached the backend: cancelled while
  queued, or dispatched by the policy and dropped by the engine.
- ``asyncflow.TaskRejected``: ``enqueue`` raised; the engine fails the task.
- ``asyncflow.TaskStarving``: the oldest waiting task waited ``starve_after`` seconds
  and younger tasks were dispatched before it. Once per task.
- ``asyncflow.PolicyBlocked`` / ``asyncflow.PolicyUnblocked``: the policy reports,
  through its optional ``blocked()`` hook, that waiting tasks cannot start although
  capacity is free. Emitted on transitions only.
- ``asyncflow.PolicyLayout``: the policy's capacities, each time telemetry starts.

Without telemetry the engine calls the bare policy, so tracing costs nothing then.

Every event carries the task's uid as ``task_id``, the uid the backend runs it under,
so it joins the engine's ``TaskCreated`` ... ``TaskCompleted`` events and the
backend's reports. :func:`timeline` builds one row per task from such events, and
:func:`occupancy` the policy's occupancy over time; :func:`load_events` reads the
events back from a telemetry checkpoint file.

A policy may implement these optional hooks to say more; the proxy calls them only
when traced, and stores a copy of what they return:

- ``describe_dispatch(desc) -> dict``: ``key`` and ``charge`` for ``TaskDispatched``,
  called after ``dispatch()`` returned ``desc``. Other keys are dropped, with a
  warning.
- ``occupancy() -> dict``: what the policy has admitted, for example the cores and
  memory in use per pool. Called once per dispatch round and per release, so it
  should not walk every task.
- ``layout() -> dict``: the capacities.
- ``blocked() -> dict | None``: why waiting tasks cannot start now, or None.

Without hooks, ``key`` holds the declared ``priority`` and the description's
``_lineage``, ``_seq`` and ``_scope``, and ``occupancy`` is empty: ``waiting`` and
``inflight`` are always there. Events name the backend in ``_backend``. Tracing
never changes what the policy decides: an error in it is logged, and costs at most
that one event.

Telemetry keeps every event in memory until it stops (RHAPSODY's event log); for a
long run, bound it on RHAPSODY's side.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import logging
import math
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from typing import Any, Optional

__all__ = [
    "POLICY_EVENTS",
    "TracedPolicy",
    "inner_policy",
    "load_events",
    "occupancy",
    "policy_events",
    "timeline",
]

logger = logging.getLogger(__name__)

# event name -> fields beyond RHAPSODY's BaseEvent
POLICY_EVENTS = {
    "asyncflow.TaskDispatched": {
        "policy": str,
        "held_seconds": (object, None),
        "order": int,
        "key": (object, None),
        "placement": (object, None),
        "charge": (object, None),
        "waiting": int,
        "inflight": int,
        "occupancy": (object, None),
    },
    "asyncflow.TaskReleased": {
        "policy": str,
        "state": (object, None),
        "dispatched": (bool, True),
        "held_seconds": (object, None),
        "run_seconds": (object, None),
        "observed": (object, None),
        "waiting": int,
        "inflight": int,
        "occupancy": (object, None),
    },
    "asyncflow.TaskRejected": {"policy": str, "error": str},
    "asyncflow.TaskStarving": {
        "policy": str,
        "held_seconds": float,
        "overtaken": int,
        "waiting": int,
    },
    "asyncflow.PolicyBlocked": {
        "policy": str,
        "reason": (object, None),
        "waiting": int,
        "inflight": int,
    },
    "asyncflow.PolicyUnblocked": {"policy": str, "blocked_seconds": float},
    "asyncflow.PolicyLayout": {
        "policy": str,
        "layout": (object, None),
        "starve_after": (object, None),
    },
}

# The TaskDispatched fields a policy's describe_dispatch() may set
HOOK_FIELDS = ("key", "charge")

_event_classes: dict = {}


def policy_events() -> dict:
    """Return the policy event classes by name, defined once with RHAPSODY's
    ``define_event``."""
    if not _event_classes:
        from rhapsody.telemetry.events import define_event  # noqa: PLC0415

        for name, fields in POLICY_EVENTS.items():
            _event_classes[name] = define_event(name, **fields)
    return _event_classes


def inner_policy(policy: Any) -> Any:
    """The policy a :class:`TracedPolicy` traces, or ``policy`` itself."""
    return policy._trace_inner if isinstance(policy, TracedPolicy) else policy


def _copy(value: Any) -> Any:
    """A deep copy of what a hook or a backend handed over, so later changes to it do
    not rewrite past events; the value itself if it cannot be copied."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


# Indices into a queued task's record
_T, _DESC, _OVERTAKEN, _REPORTED = range(4)


class TracedPolicy:
    """Forwards the policy operations to ``policy`` and emits what it decided.

    Args:
        policy: The policy to trace.
        emit: The engine's ``_emit(event_name, *, task_id, backend, workflow_id,
            **fields)``.
        default_backend: The backend name of tasks whose description names none.
        starve_after: Seconds the oldest waiting task may wait, overtaken, before
            ``TaskStarving``; None disables it.
        clock: Wall-clock time, as the other telemetry events use.
        submits: Whether the engine submits a description that ``dispatch()``
            returned. One it drops (its future was resolved while held, or it is not
            this engine's) gets no ``TaskDispatched``.

    Attribute reads and writes go to the wrapped policy, as do ``len()`` and
    ``bool()``, so ``engine._policy.state()`` still works while traced. The proxy's
    own attributes start with ``_trace``, so they hide none of the policy's;
    :func:`inner_policy` returns the wrapped policy.

    Tracing never changes what the policy decides: if emitting an event or a hook
    raises, the proxy logs it and returns what the policy returned. Each event is
    guarded on its own, so one failure costs at most that event.
    """

    _trace_proxy = True  # marks the proxy without going through __getattr__

    def __init__(
        self,
        policy: Any,
        emit: Callable[..., None],
        *,
        default_backend: Optional[str] = None,
        starve_after: Optional[float] = 60.0,
        clock: Callable[[], float] = time.time,
        submits: Optional[Callable[[dict], bool]] = None,
    ):
        self._trace_inner = policy
        self._trace_rebind(
            emit, default_backend=default_backend, starve_after=starve_after
        )
        self._trace_clock = clock
        self._trace_submits = submits
        self._trace_name = type(policy).__name__
        # uid -> [enqueue time or None, desc, overtaken, starving reported], in order
        self._trace_queued: OrderedDict = OrderedDict()
        # uid -> (enqueue time or None, dispatch time or None, desc)
        self._trace_inflight: dict = {}
        # uid -> (enqueue time or None, desc): dispatched, but the engine drops it
        self._trace_dropped: dict = {}
        self._trace_order = 0
        self._trace_blocked: Optional[tuple] = None  # (since, reason) while blocked
        self._trace_errors = 0
        self._trace_dropped_keys: set = set()  # describe_dispatch keys warned about
        self._trace_describe = getattr(policy, "describe_dispatch", None)
        self._trace_occupancy = getattr(policy, "occupancy", None)
        self._trace_blocked_hook = getattr(policy, "blocked", None)

    def __getattr__(self, name):
        # Only called for attributes TracedPolicy lacks
        if name.startswith("_trace"):  # not set yet (copying, unpickling)
            raise AttributeError(name)
        return getattr(self._trace_inner, name)

    def __setattr__(self, name, value):
        if name.startswith("_trace"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._trace_inner, name, value)

    def __delattr__(self, name):
        if name.startswith("_trace"):
            object.__delattr__(self, name)
        else:
            delattr(self._trace_inner, name)

    def __len__(self) -> int:
        return len(self._trace_inner)

    def __bool__(self) -> bool:
        return bool(self._trace_inner)

    def __repr__(self) -> str:
        return f"TracedPolicy({self._trace_inner!r})"

    # -- the policy operations -------------------------------------------------

    def enqueue(self, uid: str, desc: dict) -> None:
        try:
            self._trace_inner.enqueue(uid, desc)
        except Exception as e:
            self._trace_guard(
                self._trace_event,
                "asyncflow.TaskRejected",
                uid,
                desc,
                error=f"{type(e).__name__}: {e}",
            )
            raise
        self._trace_guard(self._trace_note_enqueue, uid, desc)

    def runnable(self) -> bool:
        return self._trace_inner.runnable()

    def dispatch(self) -> list:
        out = self._trace_inner.dispatch()
        if not isinstance(out, list):
            out = list(out)  # an iterator is read once: the engine gets the same tasks
        self._trace_guard(self._trace_dispatch, out)
        return out

    def release(
        self, uid: str, state: Optional[str], observed: Optional[dict] = None
    ) -> Any:
        try:
            return self._trace_inner.release(uid, state, observed)
        finally:  # the engine counts the task as gone, also when the policy raised
            self._trace_guard(self._trace_release, uid, state, observed)

    # -- set up by the engine --------------------------------------------------

    def _trace_rebind(
        self,
        emit: Callable[..., None],
        *,
        default_backend: Optional[str],
        starve_after: Optional[float],
    ) -> None:
        """Emit to ``emit`` from now on: telemetry was started (again)."""
        self._trace_emit = emit
        self._trace_backend = default_backend
        self._trace_starve_after = starve_after

    def _trace_adopt(self, held: dict, inflight: dict) -> None:
        """Take on the tasks the policy held or had in flight before tracing started
        (``{uid: desc}`` each).

        Their times in the policy are unknown, so their
        ``held_seconds`` and ``run_seconds`` are None.
        """
        for uid, desc in held.items():
            self._trace_queued.setdefault(uid, [None, desc, 0, False])
        for uid, desc in inflight.items():
            self._trace_inflight.setdefault(uid, (None, None, desc))

    # -- what the proxy emits --------------------------------------------------

    def _trace_note_enqueue(self, uid: str, desc: dict) -> None:
        self._trace_queued[uid] = [self._trace_clock(), desc, 0, False]

    def _trace_dispatch(self, out: list) -> None:
        # First the records of every task, which cannot fail half-way; then one guarded
        # event per task, so one failure costs only its own event.
        now = self._trace_clock()
        queued, inflight = self._trace_queued, self._trace_inflight
        new = []
        for desc in out:
            uid = desc["uid"]
            if uid in inflight:  # in flight already (a policy bug): nothing changes
                continue
            oldest = next(iter(queued), None)
            rec = queued.pop(uid, None)
            t_enqueue = rec[_T] if rec is not None else None
            if rec is not None and oldest is not None and oldest != uid:
                queued[oldest][_OVERTAKEN] += 1
            if not self._trace_will_submit(desc):
                if rec is not None:
                    self._trace_dropped[uid] = (t_enqueue, desc)
                continue
            inflight[uid] = (t_enqueue, now, desc)
            new.append((uid, desc, t_enqueue))
        if new:
            occupancy = self._trace_snapshot()
            waiting, n_inflight = len(queued), len(inflight)
            for uid, desc, t_enqueue in new:
                self._trace_guard(
                    self._trace_dispatched,
                    uid,
                    desc,
                    t_enqueue,
                    now,
                    occupancy,
                    waiting,
                    n_inflight,
                )
                self._trace_order += 1
        if queued and self._trace_starve_after is not None:
            self._trace_guard(self._trace_check_starving, now)
        if self._trace_blocked_hook is not None:
            self._trace_guard(self._trace_check_blocked, now)

    def _trace_will_submit(self, desc: dict) -> bool:
        if self._trace_submits is None:
            return True
        try:
            return bool(self._trace_submits(desc))
        except Exception:
            self._trace_failed()
            return True

    def _trace_dispatched(
        self, uid, desc, t_enqueue, now, occupancy, waiting, n_inflight
    ) -> None:
        placement = _copy(desc.get("placement"))
        fields = {
            "event_time": now,
            "node_id": placement.get("host") if isinstance(placement, dict) else None,
            "held_seconds": None if t_enqueue is None else now - t_enqueue,
            "order": self._trace_order,
            "key": _default_key(desc),
            "placement": placement,
            "waiting": waiting,
            "inflight": n_inflight,
            "occupancy": occupancy,
        }
        if self._trace_describe is not None:
            fields.update(self._trace_hook_fields(desc))
        self._trace_event("asyncflow.TaskDispatched", uid, desc, **fields)

    def _trace_hook_fields(self, desc: dict) -> dict:
        """What ``describe_dispatch(desc)`` adds; a hook that raises adds nothing."""
        try:
            extra = self._trace_describe(desc)
        except Exception:
            self._trace_failed()
            return {}
        if not extra:
            return {}
        if not isinstance(extra, dict):
            extra = {"": extra}
        unknown = set(extra) - set(HOOK_FIELDS) - self._trace_dropped_keys
        if unknown:
            self._trace_dropped_keys |= unknown
            logger.warning(
                "Policy %r: describe_dispatch() returned %s, which TaskDispatched does "
                "not have; they are dropped. Return %s.",
                self._trace_inner,
                sorted(map(repr, unknown)),
                " and ".join(HOOK_FIELDS),
            )
        return {k: _copy(v) for k, v in extra.items() if k in HOOK_FIELDS}

    def _trace_release(
        self, uid: str, state: Optional[str], observed: Optional[dict]
    ) -> None:
        rec = self._trace_inflight.pop(uid, None)
        if rec is not None:
            t_enqueue, t_dispatch, desc = rec
            dispatched = True
        else:
            dropped = self._trace_dropped.pop(uid, None)
            if dropped is not None:
                t_enqueue, desc = dropped
            else:
                queued = self._trace_queued.pop(uid, None)
                if queued is None:  # released already, or never seen
                    return
                t_enqueue, desc = queued[_T], queued[_DESC]
            t_dispatch, dispatched = None, False
        now = self._trace_clock()
        until = t_dispatch if dispatched else now
        self._trace_event(
            "asyncflow.TaskReleased",
            uid,
            desc,
            event_time=now,
            state=state,
            dispatched=dispatched,
            held_seconds=None
            if t_enqueue is None or until is None
            else until - t_enqueue,
            run_seconds=(None if t_dispatch is None else now - t_dispatch)
            if dispatched
            else 0.0,
            observed=_copy(observed),
            waiting=len(self._trace_queued),
            inflight=len(self._trace_inflight),
            occupancy=self._trace_snapshot(),
        )

    def _trace_emit_layout(self) -> None:
        """Emit ``asyncflow.PolicyLayout`` with the policy's capacities."""
        try:
            hook = getattr(self._trace_inner, "layout", None)
            layout = _copy(hook()) if hook is not None else None
            if layout is None:
                capacity = getattr(self._trace_inner, "capacity", None)
                layout = {"capacity": capacity} if capacity is not None else {}
            self._trace_emit(
                "asyncflow.PolicyLayout",
                task_id=None,
                backend=self._trace_backend,
                policy=self._trace_name,
                layout=layout,
                starve_after=self._trace_starve_after,
            )
        except Exception:
            self._trace_failed()

    def _trace_check_starving(self, now: float) -> None:
        uid, rec = next(iter(self._trace_queued.items()))
        if (
            rec[_T] is not None
            and not rec[_REPORTED]
            and rec[_OVERTAKEN]
            and now - rec[_T] >= self._trace_starve_after
        ):
            rec[_REPORTED] = True
            self._trace_event(
                "asyncflow.TaskStarving",
                uid,
                rec[_DESC],
                event_time=now,
                held_seconds=now - rec[_T],
                overtaken=rec[_OVERTAKEN],
                waiting=len(self._trace_queued),
            )

    def _trace_check_blocked(self, now: float) -> None:
        reason = _copy(self._trace_blocked_hook())
        if self._trace_blocked is not None:
            since, was = self._trace_blocked
            if reason is not None and _head(reason) == _head(was):
                return
            self._trace_blocked = None
            self._trace_emit(
                "asyncflow.PolicyUnblocked",
                task_id=_head(was),
                backend=self._trace_backend,
                event_time=now,
                policy=self._trace_name,
                blocked_seconds=now - since,
            )
        if reason is not None:
            self._trace_blocked = (now, reason)
            self._trace_emit(
                "asyncflow.PolicyBlocked",
                task_id=_head(reason),
                backend=self._trace_backend,
                event_time=now,
                policy=self._trace_name,
                reason=reason,
                waiting=len(self._trace_queued),
                inflight=len(self._trace_inflight),
            )

    # -- helpers ---------------------------------------------------------------

    def _trace_snapshot(self) -> Optional[dict]:
        """A copy of what the policy's ``occupancy()`` reports; None without the hook or
        when it raises."""
        hook = self._trace_occupancy
        if hook is None:
            return None
        try:
            return _copy(hook())
        except Exception:
            self._trace_failed()
            return None

    def _trace_event(self, name: str, uid: str, desc: dict, **fields) -> None:
        self._trace_emit(
            name,
            task_id=uid,
            backend=desc.get("_backend")
            or desc.get("target_backend")
            or self._trace_backend,
            workflow_id=desc.get("workflow_id"),
            attributes={
                "executable": desc.get("_tel_executable", desc.get("name", "")),
                "task_type": desc.get("_tel_task_type", ""),
            },
            policy=self._trace_name,
            **fields,
        )

    def _trace_guard(self, fn: Callable, *args, **kwargs) -> Any:
        """``fn(*args, **kwargs)``; None, logged, if it raises."""
        try:
            return fn(*args, **kwargs)
        except Exception:
            self._trace_failed()
            return None

    def _trace_failed(self) -> None:
        """Logs a tracing error: the first with its traceback, the others briefly."""
        self._trace_errors += 1
        if self._trace_errors == 1:
            logger.exception(
                "Policy telemetry for %r failed; the policy runs on", self._trace_inner
            )
        else:
            logger.debug(
                "Policy telemetry for %r failed again (%d)",
                self._trace_inner,
                self._trace_errors,
                exc_info=True,
            )


def _head(reason) -> Optional[str]:
    return reason.get("head") if isinstance(reason, dict) else None


def _default_key(desc: dict) -> dict:
    """The ordering fields a policy without ``describe_dispatch`` may have used: the
    declared priority, ``_lineage``, ``_seq`` and ``_scope``."""
    key = {}
    priority = (desc.get("task_backend_specific_kwargs") or {}).get("priority")
    if priority is not None:
        key["priority"] = priority
    for field in ("_lineage", "_seq", "_scope"):
        if desc.get(field) is not None:
            key[field[1:]] = desc[field]
    return key


# -- reading events back ---------------------------------------------------------

# event type -> timeline column, the first occurrence counts
_MOMENTS = {
    "TaskCreated": "created",
    "asyncflow.TaskResolved": "ready",
    "TaskQueued": "queued",
    "asyncflow.TaskDispatched": "dispatched",
    "TaskSubmitted": "submitted",
    "TaskStarted": "started",
    "asyncflow.TaskReleased": "released",
    "TaskCompleted": "ended",
    "TaskFailed": "ended",
    "TaskCanceled": "ended",
}
_STATES = {"TaskCompleted": "DONE", "TaskFailed": "FAILED", "TaskCanceled": "CANCELED"}


def _as_dict(event) -> dict:
    if isinstance(event, dict):
        return event
    return {f.name: getattr(event, f.name) for f in dataclasses.fields(event)}


def load_events(path: str) -> list[dict]:
    """Read the events of a telemetry checkpoint file (JSONL), in file order."""
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("section", "event") == "event" and "event_type" in record:
                events.append(record)
    return events


def timeline(events: Iterable) -> list[dict]:
    """One row per task: when it was created, ready, queued, dispatched, submitted,
    started and ended, joined by uid, with the policy's and the backend's details.

    ``exec_start`` and ``exec_end`` are the worker-side times the backend reported in
    ``observed`` (``"started"``, ``"ended"``), when it did. Otherwise ``started`` is
    when the backend accepted the task, which may be before it ran.
    ``release_state`` is the ``state`` the task left the policy with, None for one the
    backend never got, spelled as ``state`` is (``"CANCELED"``). Rows are ordered by
    creation.
    """
    rows: dict = {}
    for event in events:
        e = _as_dict(event)
        etype = e.get("event_type")
        column = _MOMENTS.get(etype)
        uid = e.get("task_id")
        if column is None or uid is None:
            continue
        row = rows.get(uid)
        if row is None:
            row = rows[uid] = {"uid": uid}
        row.setdefault(column, e.get("event_time"))
        attrs = e.get("attributes") or {}
        if "executable" in attrs:
            row.setdefault("executable", attrs["executable"])
        if "task_type" in attrs:
            row.setdefault("task_type", attrs["task_type"])
        if "asyncflow.workflow_id" in attrs:
            row.setdefault("workflow_id", attrs["asyncflow.workflow_id"])
        row.setdefault("backend", e.get("backend"))
        if etype in _STATES:
            row.setdefault("state", _STATES[etype])
        elif etype == "asyncflow.TaskDispatched":
            for field in ("policy", "held_seconds", "order", "key", "placement"):
                row[field] = e.get(field)
            if e.get("charge") is not None:
                row["charge"] = e["charge"]
            if e.get("node_id") is not None:
                row["node"] = e["node_id"]
        elif etype == "asyncflow.TaskReleased":
            row["run_seconds"] = e.get("run_seconds")
            # The policy protocol spells it as futures do; task events as RHAPSODY does
            state = e.get("state")
            row["release_state"] = "CANCELED" if state == "CANCELLED" else state
            if not e.get("dispatched", True):
                row.setdefault("held_seconds", e.get("held_seconds"))
            observed = e.get("observed")
            if isinstance(observed, dict):
                row["observed"] = observed
                if observed.get("started") is not None:
                    row["exec_start"] = observed["started"]
                if observed.get("ended") is not None:
                    row["exec_end"] = observed["ended"]
                if observed.get("host") is not None:
                    row.setdefault("node", observed["host"])
    return sorted(
        rows.values(),
        key=lambda r: min(
            (r[c] for c in ("created", "ready", "queued") if r.get(c) is not None),
            default=math.inf,
        ),
    )


def occupancy(events: Iterable, policy: Optional[str] = None) -> list[dict]:
    """The policy's occupancy over time, from its dispatch and release events.

    Each point is ``{"t", "inflight", "waiting", ...}`` plus the policy's
    ``occupancy`` snapshot, flattened (``{"pool": {"cpus": 3}}`` becomes
    ``"pool.cpus"``). It holds from ``t`` until the next point: a step function.
    Events at the same time keep the last. ``policy`` selects one policy's events
    by class name.
    """
    points: list[dict] = []
    for event in events:
        e = _as_dict(event)
        if e.get("event_type") not in (
            "asyncflow.TaskDispatched",
            "asyncflow.TaskReleased",
        ):
            continue
        if policy is not None and e.get("policy") != policy:
            continue
        point = {
            "t": e.get("event_time"),
            "inflight": e.get("inflight"),
            "waiting": e.get("waiting"),
        }
        _flatten(e.get("occupancy"), "", point)
        points.append(point)
    points.sort(key=lambda p: p["t"])  # stable: same-time points stay in emit order
    steps: list[dict] = []
    for point in points:
        if steps and steps[-1]["t"] == point["t"]:
            steps[-1] = point
        else:
            steps.append(point)
    return steps


def _flatten(value, prefix: str, out: dict) -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten(v, f"{prefix}{k}.", out)
    elif prefix:
        out[prefix[:-1]] = value

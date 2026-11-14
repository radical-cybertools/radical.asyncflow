import asyncio
import math
from itertools import count

# Shared by every engine in the process and never reset, so a uid names one component
# across engines: upper layers identify tasks by uid alone
_uid_counter = count(1)


def get_next_uid():
    """Return the next unique component ID."""
    return f"{next(_uid_counter):06d}"


def get_event_loop_or_raise(
    context_name: str = "AsyncWorkflowEngine",
) -> asyncio.AbstractEventLoop:
    """Get the current running event loop or raise a helpful error.

    Args:
        context_name: Name of the class/context for error messages

    Returns:
        asyncio.AbstractEventLoop: The current running event loop

    Raises:
        RuntimeError: If no event loop is running with helpful guidance
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError as e:
        raise RuntimeError(
            f"{context_name} must be created within an async context. "
            f"Use 'await {context_name}.create(...)' from within an async function, "
            "or run within asyncio.run()."
        ) from e


# A backend's ``task["observed"]`` (what the task used) flattened for the engine's
# TaskCompleted and TaskFailed attributes, memory in bytes. The names are RHAPSODY's
# (its telemetry manager flattens the same dict for the tasks a Session runs), so the
# figures read the same whichever layer emitted the event.
_OBSERVED_BYTES = (
    ("host_mem", "memory_peak_bytes"),
    ("host_mem_max", "memory_peak_rank_max_bytes"),
    ("host_mem_growth", "memory_growth_bytes"),
    ("gpu_mem", "gpu_memory_peak_bytes"),
)
_OBSERVED_AS_IS = (
    ("cpu_seconds", "cpu_seconds"),
    ("started", "exec_start"),
    ("ended", "exec_end"),
    ("host", "exec_host"),
    ("ranks", "ranks"),
    ("killed", "killed_signal"),
    ("timed_out", "timed_out"),
)


def _number(value) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def observed_attributes(observed) -> dict:
    """Flatten a backend's ``observed`` dict into task-event attributes.

    Memory figures, which backends report in GiB, become bytes: ``host_mem`` is
    ``memory_peak_bytes``, ``host_mem_max`` ``memory_peak_rank_max_bytes``,
    ``host_mem_growth`` ``memory_growth_bytes`` and ``gpu_mem``
    ``gpu_memory_peak_bytes``. ``cpu_seconds``, ``ranks`` and ``timed_out`` keep
    their names; ``started``, ``ended``, ``host`` and ``killed`` become
    ``exec_start``, ``exec_end``, ``exec_host`` and ``killed_signal``, and ``hosts``
    ``exec_hosts``. ``cpu_loadavg`` is ``cpu_seconds`` over the run's window, the
    average number of busy cores. ``resource_usage_scope`` is ``"worker"`` when the
    figures were taken in a pool worker that earlier tasks also used
    (``observed["approximate"]``), else ``"job"``.

    Returns an empty dict when there is nothing to report; unknown keys and values of
    the wrong type are left out.
    """
    if not isinstance(observed, dict):
        return {}
    attrs: dict = {}
    for key, name in _OBSERVED_BYTES:
        if _number(observed.get(key)):
            attrs[name] = int(observed[key] * (1 << 30))
    for key, name in _OBSERVED_AS_IS:
        value = observed.get(key)
        if isinstance(value, (str, bool)) or _number(value):
            attrs[name] = value
    hosts = observed.get("hosts")
    if isinstance(hosts, (list, tuple)) and hosts:
        attrs["exec_hosts"] = [str(h) for h in hosts]
    cpu, started, ended = (observed.get(k) for k in ("cpu_seconds", "started", "ended"))
    if _number(cpu) and _number(started) and _number(ended) and ended > started:
        attrs["cpu_loadavg"] = cpu / (ended - started)
    if attrs:
        attrs["resource_usage_scope"] = (
            "worker" if observed.get("approximate") else "job"
        )
    return attrs

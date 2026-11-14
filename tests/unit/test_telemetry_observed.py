"""Tests that the engine's TaskCompleted and TaskFailed carry what the task used."""

import asyncio

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.policy_telemetry import load_events
from radical.asyncflow.utils import observed_attributes

GIB = 1 << 30

# What a backend's observe_peaks reports for an executable that ran as two ranks
JOB = {
    "host_mem": 1.5,
    "host_mem_max": 1.0,
    "ranks": 2,
    "gpu_mem": 0.25,
    "cpu_seconds": 6.0,
    "started": 100.0,
    "ended": 104.0,
    "hosts": ["node-a", "node-b"],
}
# A function that ran in a pool worker earlier tasks also used
POOL = {
    "host_mem": 0.5,
    "host_mem_growth": 0.125,
    "gpu_mem": None,
    "cpu_seconds": 0.5,
    "started": 10.0,
    "ended": 11.0,
    "host": "node-a",
    "approximate": True,
}
# A task whose worker was killed: when and where, but no memory figure
KILLED = {
    "host_mem": None,
    "gpu_mem": None,
    "started": 20.0,
    "ended": 25.0,
    "host": "node-b",
    "killed": "SIGKILL",
    "exit_code": -9,
    "timed_out": True,
}


def test_a_jobs_usage_is_flattened_with_memory_in_bytes():
    assert observed_attributes(JOB) == {
        "memory_peak_bytes": int(1.5 * GIB),
        "memory_peak_rank_max_bytes": GIB,
        "gpu_memory_peak_bytes": GIB // 4,
        "ranks": 2,
        "cpu_seconds": 6.0,
        "exec_start": 100.0,
        "exec_end": 104.0,
        "exec_hosts": ["node-a", "node-b"],
        "cpu_loadavg": 1.5,
        "resource_usage_scope": "job",
    }


def test_a_pool_tasks_usage_is_the_workers():
    assert observed_attributes(POOL) == {
        "memory_peak_bytes": GIB // 2,
        "memory_growth_bytes": GIB // 8,
        "cpu_seconds": 0.5,
        "exec_start": 10.0,
        "exec_end": 11.0,
        "exec_host": "node-a",
        "cpu_loadavg": 0.5,
        "resource_usage_scope": "worker",
    }


def test_a_killed_task_reports_how_without_memory():
    assert observed_attributes(KILLED) == {
        "exec_start": 20.0,
        "exec_end": 25.0,
        "exec_host": "node-b",
        "killed_signal": "SIGKILL",
        "timed_out": True,
        "resource_usage_scope": "job",
    }


@pytest.mark.parametrize("observed", [None, {}, "1.5", [1.5], {"approximate": True}])
def test_nothing_observed_adds_nothing(observed):
    assert observed_attributes(observed) == {}


def test_values_of_the_wrong_type_are_left_out():
    observed = {
        "host_mem": float("nan"),
        "host_mem_max": True,
        "gpu_mem": "1",
        "cpu_seconds": float("inf"),
        "started": 5.0,
        "ended": 5.0,  # an empty window: no cpu_loadavg
        "ranks": [2],
        "hosts": [],
    }
    assert observed_attributes(observed) == {
        "exec_start": 5.0,
        "exec_end": 5.0,
        "resource_usage_scope": "job",
    }


@pytest.mark.parametrize("observed", [JOB, POOL, KILLED])
def test_the_names_and_values_are_rhapsodys(observed):
    """Where RHAPSODY flattens ``observed`` itself, it gives the same attributes."""
    manager = pytest.importorskip("rhapsody.telemetry.manager")
    flatten = getattr(manager, "_observed_attributes", None)
    if flatten is None:
        pytest.skip("this RHAPSODY does not flatten observed")
    assert observed_attributes(observed) == flatten({"observed": observed})


# --- through the engine -----------------------------------------------------------


class _HoldingBackend(NoopExecutionBackend):
    """Accepts tasks and leaves them running; the test reports their states."""

    async def submit_tasks(self, tasks):
        for task in tasks:
            self.tasks[task["uid"]] = task


async def _held(backend, n):
    for _ in range(200):
        if len(backend.tasks) >= n:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{len(backend.tasks)} of {n} tasks reached the backend")


@pytest.mark.asyncio
async def test_task_completed_and_failed_carry_what_the_task_used(tmp_path):
    pytest.importorskip("rhapsody.telemetry", reason="RHAPSODY telemetry not installed")
    backend = _HoldingBackend()
    engine = await WorkflowEngine.create(backend=backend)
    telemetry = await engine.start_telemetry(checkpoint_path=str(tmp_path))
    events = []
    telemetry.subscribe(events.append)

    @engine.function_task
    async def simulate():
        return 1

    futs = [simulate() for _ in range(4)]
    await _held(backend, 4)
    job, pool, killed, plain = (backend.tasks[f.uid] for f in futs)

    # The backend reports each task's end, with what it used where it knows
    engine.task_callbacks({**job, "return_value": 1, "observed": JOB}, "DONE")
    engine.task_callbacks({**pool, "exception": "boom", "observed": POOL}, "FAILED")
    engine.task_callbacks({**killed, "exception": "oom", "observed": KILLED}, "FAILED")
    engine.task_callbacks({**plain, "return_value": 1}, "DONE")
    await asyncio.gather(*futs, return_exceptions=True)
    await telemetry.stop()  # delivers the events still queued
    await engine.shutdown()

    ended = {
        e.task_id: e for e in events if e.event_type in ("TaskCompleted", "TaskFailed")
    }
    assert ended.keys() == {job["uid"], pool["uid"], killed["uid"], plain["uid"]}
    assert ended[job["uid"]].event_type == "TaskCompleted"
    assert ended[pool["uid"]].event_type == "TaskFailed"
    for uid, observed in (
        (job["uid"], JOB),
        (pool["uid"], POOL),
        (killed["uid"], KILLED),
    ):
        attrs = ended[uid].attributes
        assert attrs["task_type"] == ended[plain["uid"]].attributes["task_type"]
        assert {k: attrs[k] for k in attrs if k in observed_attributes(observed)} == (
            observed_attributes(observed)
        ), uid
    assert ended[job["uid"]].attributes["memory_peak_bytes"] == int(1.5 * GIB)
    assert ended[killed["uid"]].attributes["killed_signal"] == "SIGKILL"
    # A task the backend reported nothing for gets no usage attributes
    assert "resource_usage_scope" not in ended[plain["uid"]].attributes
    assert "memory_peak_bytes" not in ended[plain["uid"]].attributes

    # The attributes survive the checkpoint file
    (path,) = tmp_path.glob("*.jsonl")
    done = [
        e
        for e in load_events(str(path))
        if e["event_type"] == "TaskCompleted" and e.get("task_id") == job["uid"]
    ]
    assert len(done) == 1
    assert done[0]["attributes"]["exec_hosts"] == ["node-a", "node-b"]
    assert done[0]["attributes"]["memory_peak_rank_max_bytes"] == GIB

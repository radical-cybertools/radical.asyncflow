# tests/conftest.py
import asyncio
import gc
import logging
import os
import shutil
import sys
import warnings
from pathlib import Path

import pytest

from radical.asyncflow import WorkflowEngine
from tests.harness import (
    EAGER_TASKS_SUPPORTED,
    ErrorRecords,
    eager_task_failure,
    engine_violations,
    held_futures,
    log_violations,
)

try:
    from hypothesis import HealthCheck, settings
except ImportError:  # the property tests are then skipped
    pass
else:
    # The engine_invariants fixture checks once per test, after all its examples.
    _common = {
        "deadline": None,
        "suppress_health_check": [HealthCheck.function_scoped_fixture],
    }
    settings.register_profile("ci", max_examples=150, print_blob=True, **_common)
    settings.register_profile("nightly", max_examples=2000, **_common)
    settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))

_previous_policy = pytest.StashKey()
_violations = pytest.StashKey()
_call_failed = pytest.StashKey()


class EngineInvariantWarning(UserWarning):
    """A test left behind what the engine invariants forbid."""


def pytest_addoption(parser):
    group = parser.getgroup("asyncflow")
    group.addoption(
        "--eager-tasks",
        action="store_true",
        default=os.environ.get("ASYNCFLOW_EAGER_TASKS", "") not in ("", "0"),
        help="create every event loop with asyncio.eager_task_factory (Python "
        "3.12+); also ASYNCFLOW_EAGER_TASKS=1",
    )
    group.addoption(
        "--engine-invariants",
        choices=("strict", "warn", "off"),
        default=os.environ.get("ASYNCFLOW_INVARIANTS", "warn"),
        help="strict fails a test that leaves ERROR logs or engine records after "
        "shutdown, warn reports it, off skips the checks; also "
        "ASYNCFLOW_INVARIANTS (default: warn)",
    )


def pytest_configure(config):
    config.stash[_violations] = []
    if config.getoption("eager_tasks"):
        if not EAGER_TASKS_SUPPORTED:
            raise pytest.UsageError("--eager-tasks needs Python 3.12 or later")
        from tests.harness import EagerTaskPolicy

        config.stash[_previous_policy] = asyncio.get_event_loop_policy()
        asyncio.set_event_loop_policy(EagerTaskPolicy())


def pytest_unconfigure(config):
    if _previous_policy in config.stash:
        asyncio.set_event_loop_policy(config.stash[_previous_policy])


def pytest_report_header(config):
    factory = "eager" if config.getoption("eager_tasks") else "default"
    invariants = config.getoption("engine_invariants")
    return f"asyncflow: {factory} task factory, engine invariants {invariants}"


def pytest_collection_modifyitems(config, items):
    if config.getoption("eager_tasks"):
        for item in items:
            reason = eager_task_failure(item.nodeid)
            if reason:
                item.add_marker(pytest.mark.xfail(reason=reason, strict=True))
    # Scale tests run only when selected, as in `pytest -m stress tests/stress`.
    if "stress" in (config.getoption("markexpr") or ""):
        return
    skip = pytest.mark.skip(reason="stress test: select with -m stress")
    for item in items:
        if item.get_closest_marker("stress"):
            item.add_marker(skip)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    if report.when == "call" and (report.failed or hasattr(report, "wasxfail")):
        item.stash[_call_failed] = True
    return report


@pytest.fixture(autouse=True)
def engine_invariants(request, monkeypatch):
    """Checks what a test leaves behind, after its own fixtures are torn down.

    It tracks every WorkflowEngine the test creates. At teardown it reports ERROR log
    records, futures an engine held that its shutdown() left unfinished, and records
    it keeps after shutdown. A test marked ``no_engine_invariants`` is not checked,
    nor is one that already failed.
    """
    mode = request.config.getoption("engine_invariants")
    if mode == "off" or request.node.get_closest_marker("no_engine_invariants"):
        yield
        return

    engines, unfinished, kept = [], {}, set()
    init, shutdown = WorkflowEngine.__init__, WorkflowEngine.shutdown

    def tracked_init(self, *args, **kwargs):
        init(self, *args, **kwargs)
        engines.append(self)

    async def tracked_shutdown(self, skip_execution_backend=False):
        if skip_execution_backend:
            kept.add(self)
        futures = held_futures(self)
        await shutdown(self, skip_execution_backend)
        unfinished[self] = [fut for fut in futures if not fut.done()]

    monkeypatch.setattr(WorkflowEngine, "__init__", tracked_init)
    monkeypatch.setattr(WorkflowEngine, "shutdown", tracked_shutdown)
    # What earlier tests left is not this test's: pytest keeps the traceback of the
    # last failure, and the futures in its frames, until this test's call phase.
    for name in ("last_exc", "last_type", "last_value", "last_traceback"):
        if hasattr(sys, name):
            delattr(sys, name)
    gc.collect()
    errors = ErrorRecords()
    root = logging.getLogger()
    root.addHandler(errors)
    try:
        yield
        violations = engine_violations(engines, unfinished, kept)
        # Unreferenced futures and tasks report "never retrieved" and "destroyed but
        # pending" when collected.
        engines.clear()
        unfinished.clear()
        kept.clear()
        gc.collect()
    finally:
        root.removeHandler(errors)
    violations += log_violations(errors.records)

    if not violations or request.node.stash.get(_call_failed, False):
        return
    request.config.stash[_violations].append((request.node.nodeid, violations))
    message = "engine invariants:\n  " + "\n  ".join(violations)
    if mode == "strict":
        pytest.fail(message, pytrace=False)
    warnings.warn(EngineInvariantWarning(message), stacklevel=1)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    found = config.stash.get(_violations, [])
    if not found:
        return
    terminalreporter.section("engine invariants")
    for nodeid, violations in found:
        terminalreporter.write_line(nodeid)
        for line in violations:
            terminalreporter.write_line(f"    {line}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as out:
            out.write(f"### Engine invariants: {len(found)} tests\n\n")
            for nodeid, violations in found:
                out.write(f"- `{nodeid}`\n")
                out.writelines(f"  - {line}\n" for line in violations)
            out.write("\n")


def pytest_sessionfinish(session, exitstatus):
    root = Path(__file__).parent
    for pycache_dir in root.rglob("__pycache__"):
        shutil.rmtree(pycache_dir)

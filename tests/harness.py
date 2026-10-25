"""Checks the test suite runs around every test, wired up in tests/conftest.py.

- An event loop policy whose loops run new tasks eagerly, as with
  ``loop.set_task_factory(asyncio.eager_task_factory)``. A task then runs up to its
  first suspension inside ``create_task``, before the caller continues: an ordering
  that broke every task once.
- Engine invariants: what a test leaves behind that no test should. ERROR log records
  (asyncio's "Future exception was never retrieved" and "Task was destroyed but it is
  pending!" among them), futures that shutdown() leaves unfinished, and records that an
  engine keeps after its shutdown.
"""

import asyncio
import logging
import re
import sys

EAGER_TASKS_SUPPORTED = sys.version_info >= (3, 12)

_BLOCK_STATE = (
    "engine: _submit_blocks sets block_fut.state = 'RUNNING' after create_task, "
    "which has already run a block that does not suspend to the end"
)
_STARTS_IN_CALL = (
    "test assumes a task does not start inside the call: the component it expects "
    "to be pending, or to fail registering, has already registered"
)
# Tests that fail under an eager task factory, and why, by node id regex. They are
# strict expected failures with --eager-tasks: once fixed they XPASS, which fails, and
# leave the list.
EAGER_TASK_FAILURES = {
    r"test_block_execution\.py::test_block_success_and_failure_states_unchanged": (
        _BLOCK_STATE
    ),
    r"test_future_state\.py::test_block_future_state_transitions_to_failed": (
        _BLOCK_STATE
    ),
    r"test_foreign_engine_future\.py::test_foreign_engine_future_is_rejected"
    r"\[.*-pending-.*\]": _STARTS_IN_CALL,
    r"test_pending_dependency\.py::test_registration_failing_after_storing_.*": (
        _STARTS_IN_CALL
    ),
    r"test_pending_dependency\.py::test_registration_failing_inside_a_block_.*": (
        _STARTS_IN_CALL
    ),
}


def eager_task_failure(nodeid: str):
    """Returns why the test fails under an eager task factory, or None."""
    for pattern, reason in EAGER_TASK_FAILURES.items():
        if re.fullmatch(f"tests/unit/{pattern}", nodeid):
            return reason
    return None


# What WorkflowEngine.shutdown() empties, unless skip_execution_backend=True
RECORDS = (
    "components",
    "dependencies",
    "_dependency_count",
    "_dependents_map",
    "_ready_queue",
    "_pending_registrations",
)


if EAGER_TASKS_SUPPORTED:

    class EagerTaskPolicy(asyncio.DefaultEventLoopPolicy):
        """Creates event loops with asyncio.eager_task_factory as task factory.

        pytest-asyncio and asyncio.run() create their loops through the policy, so
        installing it covers async tests, async fixtures and tests that call
        asyncio.run() themselves.
        """

        def new_event_loop(self):
            loop = super().new_event_loop()
            loop.set_task_factory(asyncio.eager_task_factory)
            return loop


class ErrorRecords(logging.Handler):
    """Keeps the log records of level ERROR and above."""

    def __init__(self):
        super().__init__(logging.ERROR)
        self.records = []

    def emit(self, record):
        self.records.append(record)


def log_violations(records) -> list:
    """Describes each ERROR record on one line."""
    lines = []
    for record in records:
        message = " ".join(record.getMessage().split())
        lines.append(f"{record.levelname} from {record.name}: {message[:300]}")
    return lines


def held_futures(engine) -> list:
    """The futures of the components an engine holds, registered or not yet."""
    futures = [comp["future"] for comp in engine.components.values()]
    return futures + list(getattr(engine, "_pending_registrations", ()))


def engine_violations(engines, unfinished, kept=()) -> list:
    """Describes what each shut-down engine left unfinished or still keeps.

    Args:
        engines: Engines a test created.
        unfinished: Maps an engine to the futures it held when shutdown() was called
            that were not done when it returned.
        kept: Engines shut down with skip_execution_backend=True, which keep their
            records by design.
    """
    lines = []
    for engine in engines:
        if not engine._shutdown_event.is_set():
            continue
        left = unfinished.get(engine)
        if left:
            sample = ", ".join(_describe(fut) for fut in left[:3])
            lines.append(
                f"engine {engine.uid}: {len(left)} futures not done after "
                f"shutdown: {sample}"
            )
        if engine in kept:
            continue
        for name in RECORDS:
            record = getattr(engine, name, None)
            if record:
                sample = ", ".join(_describe(item) for item in list(record)[:3])
                lines.append(
                    f"engine {engine.uid}: {name} keeps {len(record)} after "
                    f"shutdown: {sample}"
                )
    return lines


def _describe(item) -> str:
    """Names a record entry: a uid, or the uid of a pending future."""
    return getattr(item, "uid", None) or str(item)

"""Properties of random workflows on a Noop backend.

Hypothesis draws a graph (a chain, fan-out, fan-in, diamond or any DAG), a component for
each node (a function, executable or prompt task, or a block), whether each dependency
is passed positionally, as a keyword or both, and faults: a body that raises, an empty
command or prompt, a registration step that raises after the component is stored, a
task the backend fails, a cancel before or after registration, and a shutdown while the
workflow runs.

For every example:
- every future finishes within TIMEOUT seconds;
- each node ends as `expected_outcomes` says from its own fault and how its
  dependencies ended: results match the model, a dependent of failed nodes fails with
  a DependencyFailureError naming them, and cancellation propagates;
- a node that failed to register leaves no engine record; once every future is done
  the engine keeps no transient record, and after shutdown no record at all;
- nothing is logged at ERROR.

The Hypothesis profile comes from HYPOTHESIS_PROFILE: ci (default) or nightly, see
tests/conftest.py.
"""

import asyncio
import gc
import logging
import os
from dataclasses import dataclass
from typing import Optional

import pytest

pytest.importorskip("hypothesis")

from hypothesis import event, example, given, note  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine  # noqa: E402
from radical.asyncflow.errors import DependencyFailureError  # noqa: E402
from tests.harness import RECORDS, ErrorRecords, log_violations  # noqa: E402

MAX_NODES = 16 if os.environ.get("HYPOTHESIS_PROFILE") == "nightly" else 8
TIMEOUT = 5

FUNCTION, EXECUTABLE, PROMPT, BLOCK = "function", "executable", "prompt", "block"
# body_raises: an executable or prompt body raises before the task registers, a block
# body while the block runs. step_raises: a step of _register_component raises after
# the component is stored. backend_fails: the backend reports the task FAILED.
FAULTS = {
    FUNCTION: ("backend_fails", "step_raises", "cancel_before", "cancel_after"),
    EXECUTABLE: (
        "body_raises",
        "empty_command",
        "backend_fails",
        "step_raises",
        "cancel_before",
        "cancel_after",
    ),
    PROMPT: (
        "body_raises",
        "empty_prompt",
        "backend_fails",
        "step_raises",
        "cancel_before",
        "cancel_after",
    ),
    BLOCK: ("body_raises", "step_raises", "cancel_before", "cancel_after"),
}
STEPS = ("_update_dependency_tracking", "_emit")
# Records of components that are not finished yet
TRANSIENT = (
    "_ready_queue",
    "_dependency_count",
    "_dependents_map",
    "running",
    "_pending_registrations",
    "_block_members",
    "_block_asyncio_tasks",
)
CANCELLED = ("cancelled",)


class InjectedError(Exception):
    """Raised by an injected fault."""


@dataclass
class Node:
    deps: list  # (index of the dependency, "pos", "kw" or "both")
    kind: str
    fault: Optional[str] = None
    step: Optional[str] = None  # the step that raises, for step_raises
    delay: int = 0  # ms the body waits before it returns
    inner: bool = False  # a block that calls and awaits a function task
    cancel_delay: int = 0  # loop iterations from registration to cancel_after


@dataclass
class Workflow:
    nodes: list
    completion: int  # loop iterations the backend takes per task; 0: within submit
    shutdown_ms: Optional[int]  # shut down this long after the calls; None: at the end


@st.composite
def graphs(draw):
    """The dependencies of each node: indices of earlier nodes."""
    n = draw(st.integers(1, MAX_NODES))
    shape = draw(st.sampled_from(("chain", "fan_out", "fan_in", "diamond", "dag")))
    if shape == "fan_out":
        return [[]] + [[0] for _ in range(n - 1)]
    if shape == "fan_in":
        return [[] for _ in range(n - 1)] + [list(range(n - 1))]
    if shape == "diamond" and n >= 3:
        return [[]] + [[0] for _ in range(n - 2)] + [list(range(1, n - 1))]
    if shape == "dag":
        return [
            draw(st.lists(st.integers(0, i - 1), max_size=3, unique=True)) if i else []
            for i in range(n)
        ]
    return [[i - 1] if i else [] for i in range(n)]


@st.composite
def workflows(draw):
    nodes = []
    for deps in draw(graphs()):
        kind = draw(st.sampled_from((FUNCTION, EXECUTABLE, PROMPT, BLOCK)))
        how = st.sampled_from(("pos", "kw", "both"))
        node = Node(deps=[(d, draw(how)) for d in deps], kind=kind)
        node.fault = draw(st.sampled_from((None,) * 6 + FAULTS[kind]))
        if node.fault == "step_raises":
            node.step = draw(st.sampled_from(STEPS))
        if node.fault == "cancel_after":
            node.cancel_delay = draw(st.integers(0, 3))
        node.delay = draw(st.sampled_from((0, 0, 0, 1, 5, 20)))
        node.inner = kind == BLOCK and draw(st.booleans())
        nodes.append(node)
    shutdown = draw(st.integers(0, 2)) == 2  # a third of the examples
    return Workflow(
        nodes,
        completion=draw(st.integers(0, 3)),
        shutdown_ms=draw(st.integers(0, 50)) if shutdown else None,
    )


class FaultyNoopBackend(NoopExecutionBackend):
    """A NoopExecutionBackend that fails the tasks named in `failing`.

    With completion > 0 it completes each task that many loop iterations after
    submit_tasks returns, so tasks are still running when cancelled or shut down.
    """

    def __init__(self, failing, completion):
        super().__init__()
        self.failing = failing
        self.completion = completion
        self.inflight = {}

    async def submit_tasks(self, tasks):
        for task in tasks:
            if task["name"] in self.failing:
                task["exception"] = InjectedError(f"{task['name']} backend")
                state = "FAILED"
            else:
                out = "Dummy Prompt Output" if task.get("prompt") else "Dummy Output"
                task["stdout"] = task["return_value"] = out
                state = "DONE"
            if self.completion:
                self.inflight[task["uid"]] = task
                self._complete(self.completion, task, state)
            else:
                self._callback_func(task, state)

    def _complete(self, iterations, task, state):
        if task["uid"] not in self.inflight:
            return  # cancelled
        if iterations:
            loop = asyncio.get_running_loop()
            loop.call_soon(self._complete, iterations - 1, task, state)
            return
        del self.inflight[task["uid"]]
        self._callback_func(task, state)

    async def cancel_task(self, uid):
        task = self.inflight.pop(uid, None)
        if task is not None:
            self._callback_func(task, "CANCELED")

    async def shutdown(self):
        self.inflight.clear()


def inject_step_faults(flow, steps):
    """Makes the registration step steps[name] raise for the component name."""

    def name_of(uid):
        comp = flow.components.get(uid)
        return comp["description"]["name"] if comp else None

    tracking, emit = flow._update_dependency_tracking, flow._emit

    def failing_tracking(uid):
        if steps.get(name_of(uid)) == "_update_dependency_tracking":
            raise InjectedError(f"{name_of(uid)} step")
        return tracking(uid)

    def failing_emit(event, *, task_id, **kwargs):
        if event == "TaskCreated" and steps.get(name_of(task_id)) == "_emit":
            raise InjectedError(f"{name_of(task_id)} step")
        return emit(event, task_id=task_id, **kwargs)

    flow._update_dependency_tracking = failing_tracking
    flow._emit = failing_emit


def make_component(flow, i, node, inner, inner_task):
    """Decorates the body of node i, named n<i>, as its kind of component."""
    name = f"n{i}"

    async def body(*args, **kwargs):
        await asyncio.sleep(node.delay / 1000)
        if node.kind == BLOCK:
            value = None
            if node.inner:
                inner[i] = inner_task(i)
                value = await inner[i]
            if node.fault == "body_raises":
                raise InjectedError(f"{name} body")
            return (name, args, kwargs, value)
        if node.fault == "body_raises":
            raise InjectedError(f"{name} body")
        if node.kind == EXECUTABLE:
            return "" if node.fault == "empty_command" else f"echo {name}"
        if node.kind == PROMPT:
            return "" if node.fault == "empty_prompt" else f"say {name}"
        return name  # a Noop backend does not run function tasks

    body.__name__ = body.__qualname__ = name
    decorate = {
        FUNCTION: flow.function_task,
        EXECUTABLE: flow.executable_task,
        PROMPT: flow.prompt_task,
        BLOCK: flow.block,
    }[node.kind]
    return decorate(body)


async def cancel_after(flow, i, node, fut, cancels):
    """Cancels fut cancel_delay loop iterations after it registers."""
    attribute = "block" if node.kind == BLOCK else "task"
    while not (hasattr(fut, attribute) or fut.done()):
        await asyncio.sleep(0)
    for _ in range(node.cancel_delay):
        await asyncio.sleep(0)
    # The backend may finish a task it is running before it cancels it.
    running = node.kind != BLOCK and fut.uid in flow.running
    cancels[i] = (fut.cancel(), running)


def outcome(fut):
    if fut.cancelled():
        return CANCELLED
    if fut.exception() is not None:
        return ("error", fut.exception())
    return ("ok", fut.result())


def own_error(i, node):
    if node.fault == "empty_command":
        return ("raises", ValueError, "non-empty command string")
    if node.fault == "empty_prompt":
        return ("raises", ValueError, "non-empty string prompt")
    where = {"body_raises": "body", "backend_fails": "backend", "step_raises": "step"}
    return ("raises", InjectedError, f"n{i} {where[node.fault]}")


def result_of(i, node, values):
    """The result of node i when it and its dependencies succeed."""
    if node.kind == PROMPT:
        return "Dummy Prompt Output"
    if node.kind != BLOCK:
        return "Dummy Output"
    args = (i, *(values[d] for d, how in node.deps if how != "kw"))
    kwargs = {f"k{d}": values[d] for d, how in node.deps if how != "pos"}
    return (f"n{i}", args, kwargs, "Dummy Output" if node.inner else None)


def expected_outcomes(i, node, observed, registered, cancels):
    """The outcomes the engine may give node i, from its fault and its dependencies.

    It follows the order in which the engine decides, so where a dependency failed and
    another was cancelled, the first decision wins.
    """
    deps = [d for d, _ in node.deps]
    accepted, running = cancels.get(i, (False, False))
    if accepted and not running:
        return [CANCELLED]
    # Executable and prompt bodies run first, before the engine looks at arguments.
    if node.kind in (EXECUTABLE, PROMPT) and node.fault in (
        "body_raises",
        "empty_command",
    ):
        return [own_error(i, node)]
    # A node registers after its dependencies; one that never registers fails it.
    unregistered = [d for d in deps if not registered[d]]
    if unregistered:
        if any(observed[d] == CANCELLED for d in unregistered):
            return [CANCELLED]
        return [("dependency", {f"n{d}" for d in unregistered})]
    if node.fault in ("step_raises", "empty_prompt"):
        return [own_error(i, node)]
    # The run loop, once every dependency is done
    failed = [d for d in deps if observed[d][0] == "error"]
    if any(observed[d] == CANCELLED for d in deps):
        expected = [CANCELLED]
    elif failed:
        expected = [("dependency", {f"n{d}" for d in failed})]
    elif node.fault in ("backend_fails", "body_raises"):
        expected = [own_error(i, node)]
    else:
        values = {d: observed[d][1] for d in deps}
        expected = [("ok", result_of(i, node, values))]
    return expected + [CANCELLED] if accepted else expected


def matches(observed, expected):
    if expected[0] in ("ok", "cancelled"):
        return observed == expected
    if observed[0] != "error":
        return False
    error = observed[1]
    if expected[0] == "raises":
        return type(error) is expected[1] and expected[2] in str(error)
    return (
        isinstance(error, DependencyFailureError)
        and set(error.failed_dependencies) == expected[1]
    )


def label_outcome(node, observed, expected):
    """Counts outcomes worth seeing in --hypothesis-show-statistics."""
    if expected[0][0] == "dependency" and len(expected[0][1]) > 1:
        event("DependencyFailureError naming several dependencies")
    if observed[0] == "ok" and node.kind == BLOCK and node.deps:
        if any(how != "pos" for _, how in node.deps):
            event("a block got the value of a keyword dependency")
    if len(expected) > 1 and observed != CANCELLED:
        event("a task finished before the backend cancelled it")


def records(flow, names):
    """The non-empty records among names, by name."""
    found = {name: list(getattr(flow, name, ())) for name in names}
    return {name: entries for name, entries in found.items() if entries}


async def settle(flow, timeout=3.0):
    """Waits until the run loop has processed every finished component."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while records(flow, TRANSIENT) and loop.time() < deadline:
        await asyncio.sleep(0.005)


async def check_workflow(wf, work_dir):
    failing = {f"n{i}" for i, n in enumerate(wf.nodes) if n.fault == "backend_fails"}
    flow = await WorkflowEngine.create(
        backend=FaultyNoopBackend(failing, wf.completion), work_dir=work_dir
    )
    steps = {f"n{i}": n.step for i, n in enumerate(wf.nodes) if n.step}
    inject_step_faults(flow, steps)

    @flow.function_task
    async def inner_task(i):
        return i

    inner, cancels, helpers, futures = {}, {}, [], []
    for i, node in enumerate(wf.nodes):
        component = make_component(flow, i, node, inner, inner_task)
        args = [i] + [futures[d] for d, how in node.deps if how != "kw"]
        kwargs = {f"k{d}": futures[d] for d, how in node.deps if how != "pos"}
        fut = component(*args, **kwargs)
        futures.append(fut)
        if node.fault == "cancel_before":
            cancels[i] = (fut.cancel(), False)
        elif node.fault == "cancel_after":
            helpers.append(
                asyncio.ensure_future(cancel_after(flow, i, node, fut, cancels))
            )

    done_before = None
    if wf.shutdown_ms is not None:
        await asyncio.sleep(wf.shutdown_ms / 1000)
        done_before = {i for i, fut in enumerate(futures) if fut.done()}
        if len(done_before) < len(futures):
            event("shutdown mid-flight")
        if getattr(flow, "_pending_registrations", None):
            event("shutdown with registrations pending")
        await flow.shutdown()
        running = [fut for fut in futures if not fut.done()]
        assert not running, f"not done when shutdown returned: {running}"

    _, pending = await asyncio.wait(futures + helpers, timeout=TIMEOUT)
    assert not pending, f"not done after {TIMEOUT} s: {pending}"
    if inner:
        _, pending = await asyncio.wait(list(inner.values()), timeout=TIMEOUT)
        assert not pending, f"inner tasks not done after {TIMEOUT} s: {pending}"

    observed = [outcome(fut) for fut in futures]
    registered = [hasattr(fut, "task") or hasattr(fut, "block") for fut in futures]
    for i, node in enumerate(wf.nodes):
        if done_before is not None and i not in done_before:
            assert observed[i] == CANCELLED, f"n{i} after shutdown: {observed[i]}"
            continue
        expected = expected_outcomes(i, node, observed, registered, cancels)
        assert any(matches(observed[i], e) for e in expected), (
            f"n{i} ({node.kind}, fault {node.fault}): {observed[i]!r}, "
            f"expected one of {expected!r}"
        )
        label_outcome(node, observed[i], expected)
    for i, fut in inner.items():
        if observed[i][0] == "ok":
            assert outcome(fut) == ("ok", "Dummy Output"), f"inner task of n{i}"
        else:
            outcome(fut)  # retrieves its exception

    if done_before is None:
        for fut, ok in zip(futures, registered):
            if not ok:
                assert fut.uid not in flow.components, f"{fut.uid} failed to register"
                assert fut.uid not in flow.dependencies, f"{fut.uid} failed to register"
        await settle(flow)
        assert not records(flow, TRANSIENT), "records of finished components"
        await flow.shutdown()
    assert not records(flow, RECORDS), "records after shutdown"
    await asyncio.sleep(0.02)  # a registration that outlived shutdown would show now
    assert not records(flow, RECORDS), "records after shutdown"


@pytest.fixture(scope="module")
def work_dir(tmp_path_factory):
    return str(tmp_path_factory.mktemp("engines"))


@given(wf=workflows())
# A block gets the values of positional and keyword dependencies
@example(
    wf=Workflow(
        [
            Node([], FUNCTION),
            Node([], EXECUTABLE, delay=1),
            Node([(0, "kw"), (1, "both")], BLOCK, inner=True),
        ],
        completion=0,
        shutdown_ms=None,
    )
)
# A DependencyFailureError names the failed dependency, not the one that succeeded
@example(
    wf=Workflow(
        [
            Node([], FUNCTION, fault="backend_fails"),
            Node([], PROMPT),
            Node([(0, "pos"), (1, "kw")], FUNCTION),
        ],
        completion=1,
        shutdown_ms=None,
    )
)
# Shutdown while an executable body runs and its dependent waits for it to register
@example(
    wf=Workflow(
        [Node([], EXECUTABLE, delay=20), Node([(0, "pos")], FUNCTION)],
        completion=0,
        shutdown_ms=5,
    )
)
def test_random_workflows(work_dir, wf):
    note(repr(wf))
    errors = ErrorRecords()
    root = logging.getLogger()
    root.addHandler(errors)
    try:
        asyncio.run(check_workflow(wf, work_dir))
        gc.collect()
    finally:
        root.removeHandler(errors)
    assert not errors.records, log_violations(errors.records)

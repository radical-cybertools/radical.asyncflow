"""Unit tests for capture_stdio decorator parameter and _work_dir authority."""

import asyncio
import os

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine

# ---------------------------------------------------------------------------
# comp_desc field tests (no real execution, dry_run / Noop)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capture_stdio_stored_in_comp_desc():
    """capture_stdio=True is stored at the top level of comp_desc."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task(capture_stdio=True)
    async def t():
        return "echo hello"

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc["capture_stdio"] is True
    await engine.shutdown()


@pytest.mark.asyncio
async def test_capture_stdio_default_false():
    """capture_stdio defaults to False when not specified."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return "echo hello"

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc["capture_stdio"] is False
    await engine.shutdown()


@pytest.mark.asyncio
async def test_capture_stdio_on_function_task_stored_false():
    """capture_stdio=True on a function_task is stored but ignored at execution."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.function_task(capture_stdio=True)
    async def t():
        return 42

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc["capture_stdio"] is True
    await engine.shutdown()


@pytest.mark.asyncio
async def test_capture_stdio_not_in_task_backend_specific_kwargs():
    """capture_stdio is top-level in comp_desc, NOT nested inside
    task_backend_specific_kwargs."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task(capture_stdio=True)
    async def t():
        return "echo hello"

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    # top-level presence
    assert "capture_stdio" in desc
    # NOT nested inside task_backend_specific_kwargs
    assert "capture_stdio" not in desc.get("task_backend_specific_kwargs", {})
    await engine.shutdown()


# ---------------------------------------------------------------------------
# _work_dir authority tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_workflow_engine_sets_backend_work_dir(tmp_path):
    """WorkflowEngine.create sets backend._work_dir to {work_dir}/{uid}."""
    backend = NoopExecutionBackend()
    engine = await WorkflowEngine.create(
        backend=backend,
        uid="wf-test",
        work_dir=str(tmp_path),
    )
    assert backend._work_dir == os.path.join(str(tmp_path), "wf-test")
    assert os.path.isdir(backend._work_dir)
    await engine.shutdown()


# ---------------------------------------------------------------------------
# End-to-end test with ConcurrentExecutionBackend (RHAPSODY)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capture_stdio_writes_files_concurrent_backend(tmp_path):
    """capture_stdio=True routes stdout/stderr to files; future resolves to path."""
    pytest.importorskip("rhapsody", reason="RHAPSODY not installed")
    from rhapsody.backends import ConcurrentExecutionBackend

    backend = await ConcurrentExecutionBackend()
    engine = await WorkflowEngine.create(
        backend=backend,
        uid="test-wf",
        work_dir=str(tmp_path),
    )

    @engine.executable_task(capture_stdio=True)
    async def say_hello():
        return "/bin/bash -c 'echo hello'"

    stdout_path = await say_hello()

    assert isinstance(stdout_path, str)
    assert stdout_path.endswith(".stdout")
    assert open(stdout_path).read() == "hello\n"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_capture_stdio_false_resolves_to_string(tmp_path):
    """Without capture_stdio, the future resolves to the decoded stdout string."""
    pytest.importorskip("rhapsody", reason="RHAPSODY not installed")
    from rhapsody.backends import ConcurrentExecutionBackend

    backend = await ConcurrentExecutionBackend()
    engine = await WorkflowEngine.create(backend=backend, work_dir=str(tmp_path))

    @engine.executable_task
    async def say_world():
        return "/bin/echo world"

    result = await say_world()

    assert result == "world\n"
    await engine.shutdown()

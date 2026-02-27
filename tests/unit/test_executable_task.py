"""Unit tests for executable_task decorator — shlex splitting behaviour."""

import asyncio

import pytest

from radical.asyncflow import WorkflowEngine
from radical.asyncflow.workflow_manager import EXECUTABLE, FUNCTION

# ---------------------------------------------------------------------------
# Split correctness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executable_task_simple_split():
    """'echo hello' → executable='echo', arguments=['hello']."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return "echo hello"

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc[EXECUTABLE] == "echo"
    assert desc["arguments"] == ["hello"]
    assert desc[FUNCTION] is None
    await engine.shutdown()


@pytest.mark.asyncio
async def test_executable_task_no_arguments():
    """'ls' → executable='ls', arguments=[]."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return "ls"

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc[EXECUTABLE] == "ls"
    assert desc["arguments"] == []
    await engine.shutdown()


@pytest.mark.asyncio
async def test_executable_task_multiple_arguments():
    """'echo hello world' → arguments=['hello', 'world']."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return "echo hello world"

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc[EXECUTABLE] == "echo"
    assert desc["arguments"] == ["hello", "world"]
    await engine.shutdown()


@pytest.mark.asyncio
async def test_executable_task_quoted_argument():
    """Shlex preserves quoted tokens as single arguments."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return 'echo "hello world"'

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc[EXECUTABLE] == "echo"
    assert desc["arguments"] == ["hello world"]
    await engine.shutdown()


@pytest.mark.asyncio
async def test_executable_task_path_with_spaces():
    """Executable paths with spaces quoted correctly."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return '"/usr/my tool" --verbose'

    await t()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc[EXECUTABLE] == "/usr/my tool"
    assert desc["arguments"] == ["--verbose"]
    await engine.shutdown()


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_executable_task_none_return_raises():
    """Returning None raises ValueError."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return None

    with pytest.raises(ValueError, match="non-empty command string"):
        await t()

    await engine.shutdown()


@pytest.mark.asyncio
async def test_executable_task_empty_string_raises():
    """Returning '' raises ValueError."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return ""

    with pytest.raises(ValueError, match="non-empty command string"):
        await t()

    await engine.shutdown()


@pytest.mark.asyncio
async def test_executable_task_whitespace_only_raises():
    """Returning whitespace-only string raises ValueError."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.executable_task
    async def t():
        return "   "

    with pytest.raises(ValueError, match="non-empty command string"):
        await t()

    await engine.shutdown()

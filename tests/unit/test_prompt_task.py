"""Unit tests for prompt_task and multi-backend registry."""

import asyncio

import pytest

from radical.asyncflow import NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.workflow_manager import EXECUTABLE, FUNCTION, PROMPT, TASK


# ---------------------------------------------------------------------------
# prompt_task registration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prompt_task_decorator_exists():
    """WorkflowEngine exposes a prompt_task decorator."""
    engine = await WorkflowEngine.create(dry_run=True)
    assert hasattr(engine, "prompt_task")
    assert callable(engine.prompt_task)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_prompt_task_registers_correctly():
    """prompt_task stores prompt string and clears function/executable fields."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.prompt_task
    async def ask(question):
        return f"Answer: {question}"

    await ask("What is 2+2?")
    await asyncio.sleep(0.05)

    assert len(engine.components) == 1
    desc = next(iter(engine.components.values()))["description"]

    assert desc["name"] == "ask"
    assert isinstance(desc[PROMPT], str)
    assert desc[PROMPT] == "Answer: What is 2+2?"
    assert desc[FUNCTION] is None
    assert desc[EXECUTABLE] is None
    await engine.shutdown()


@pytest.mark.asyncio
async def test_prompt_task_invalid_return_raises():
    """prompt_task must return a non-empty string — None raises ValueError."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.prompt_task
    async def bad_prompt():
        return None  # invalid

    with pytest.raises(ValueError, match="non-empty string prompt"):
        await bad_prompt()

    await engine.shutdown()


@pytest.mark.asyncio
async def test_prompt_task_empty_string_raises():
    """prompt_task must return a non-empty string — empty string raises ValueError."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.prompt_task
    async def empty_prompt():
        return ""

    with pytest.raises(ValueError, match="non-empty string prompt"):
        await empty_prompt()

    await engine.shutdown()


@pytest.mark.asyncio
async def test_prompt_task_result_via_noop():
    """NoopExecutionBackend returns 'Dummy Prompt Output' for prompt tasks."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.prompt_task
    async def summarize(text):
        return f"Summarize: {text}"

    result = await summarize("hello world")
    assert result == "Dummy Prompt Output"
    await engine.shutdown()


# ---------------------------------------------------------------------------
# Multi-backend registry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_single_backend_registered_by_default():
    """A single backend is stored under its name as the default."""
    backend = NoopExecutionBackend(name="primary")
    engine = await WorkflowEngine.create(backend=backend)

    assert "primary" in engine._backends
    assert engine._default_backend_name == "primary"
    assert engine.backend is backend
    await engine.shutdown()


@pytest.mark.asyncio
async def test_multi_backend_list_registers_all():
    """Passing a list registers every backend; first is the default."""
    b1 = NoopExecutionBackend(name="compute")
    b2 = NoopExecutionBackend(name="ai")
    engine = await WorkflowEngine.create(backend=[b1, b2])

    assert "compute" in engine._backends
    assert "ai" in engine._backends
    assert engine._default_backend_name == "compute"
    assert engine.backend is b1
    await engine.shutdown()


@pytest.mark.asyncio
async def test_backend_property_returns_default():
    """engine.backend property always returns the default backend."""
    b1 = NoopExecutionBackend(name="alpha")
    b2 = NoopExecutionBackend(name="beta")
    engine = await WorkflowEngine.create(backend=[b1, b2])

    assert engine.backend is b1
    await engine.shutdown()


@pytest.mark.asyncio
async def test_all_backends_receive_callback():
    """Every registered backend has the engine callback registered."""
    b1 = NoopExecutionBackend(name="compute")
    b2 = NoopExecutionBackend(name="ai")
    engine = await WorkflowEngine.create(backend=[b1, b2])

    # Bound methods compare equal when wrapping the same function on the same instance
    assert b1._callback_func == engine.task_callbacks
    assert b2._callback_func == engine.task_callbacks
    await engine.shutdown()


# ---------------------------------------------------------------------------
# Per-task backend routing (target_backend stored in comp_desc)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_target_backend_stored_in_comp_desc():
    """backend= decorator param is stored as target_backend in comp_desc."""
    b1 = NoopExecutionBackend(name="compute")
    b2 = NoopExecutionBackend(name="ai")
    engine = await WorkflowEngine.create(backend=[b1, b2])

    @engine.function_task(backend="compute")
    async def compute_task():
        return 42

    await compute_task()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc["target_backend"] == "compute"
    await engine.shutdown()


@pytest.mark.asyncio
async def test_no_target_backend_is_none():
    """Tasks without backend= have target_backend=None in comp_desc."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.function_task
    async def unrouted_task():
        return "ok"

    await unrouted_task()
    await asyncio.sleep(0.05)

    desc = next(iter(engine.components.values()))["description"]
    assert desc["target_backend"] is None
    await engine.shutdown()


@pytest.mark.asyncio
async def test_routed_task_resolves_on_correct_backend():
    """A task routed to 'ai' backend resolves via that backend's callback."""
    b1 = NoopExecutionBackend(name="compute")
    b2 = NoopExecutionBackend(name="ai")
    engine = await WorkflowEngine.create(backend=[b1, b2])

    @engine.prompt_task(backend="ai")
    async def ask_ai(q):
        return f"Q: {q}"

    result = await ask_ai("hello?")
    assert result == "Dummy Prompt Output"
    await engine.shutdown()


@pytest.mark.asyncio
async def test_unregistered_backend_name_fails_task():
    """Routing to an unknown backend name causes the task future to fail."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.function_task(backend="nonexistent")
    async def bad_route():
        return "oops"

    t = bad_route()
    await asyncio.sleep(0.2)

    # The KeyError triggers engine shutdown; the future ends up done (failed/cancelled)
    assert t.done()

"""Integration tests for prompt_task and multi-backend routing."""

import asyncio

import pytest

from radical.asyncflow import (
    LocalExecutionBackend,
    NoopExecutionBackend,
    WorkflowEngine,
)

# ---------------------------------------------------------------------------
# prompt_task end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prompt_task_end_to_end_dry_run():
    """prompt_task runs through the full engine pipeline and returns Noop output."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.prompt_task
    async def summarize(text):
        return f"Please summarize: {text}"

    result = await summarize("AsyncFlow is a workflow engine.")
    assert result == "Dummy Prompt Output"
    await engine.shutdown()


@pytest.mark.asyncio
async def test_prompt_task_in_dag():
    """prompt_task participates in a DAG — it waits for its dependency."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.function_task
    async def preprocess(raw):
        return raw.strip().lower()

    @engine.prompt_task
    async def summarize(clean_text):
        return f"Summarize: {clean_text}"

    preprocessed = preprocess("  Hello World  ")
    result = await summarize(preprocessed)

    assert result == "Dummy Prompt Output"
    await engine.shutdown()


@pytest.mark.asyncio
async def test_prompt_task_concurrent_workflows():
    """Multiple concurrent workflows each containing a prompt_task all complete."""
    engine = await WorkflowEngine.create(dry_run=True)

    @engine.function_task
    async def preprocess(text):
        return text.strip()

    @engine.prompt_task
    async def summarize(text):
        return f"Summarize: {text}"

    @engine.function_task
    async def postprocess(summary):
        return f"[done] {summary}"

    async def run_pipeline(doc):
        cleaned = preprocess(doc)
        summary = summarize(cleaned)
        return await postprocess(summary)

    docs = [f"Document {i}" for i in range(8)]
    results = await asyncio.gather(*[run_pipeline(d) for d in docs])

    assert len(results) == 8
    # NoopExecutionBackend returns "Dummy Output"
    # for function_task (function not executed)
    assert all(r == "Dummy Output" for r in results)
    await engine.shutdown()


# ---------------------------------------------------------------------------
# Multi-backend routing end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_function_task_and_prompt_task_route_to_separate_backends():
    """function_task and prompt_task route to different backends and both complete."""
    compute = NoopExecutionBackend(name="compute")
    ai = NoopExecutionBackend(name="ai")
    engine = await WorkflowEngine.create(backend=[compute, ai])

    @engine.function_task(backend="compute")
    async def preprocess(text):
        return text.strip()

    @engine.prompt_task(backend="ai")
    async def summarize(text):
        return f"Summarize: {text}"

    preprocessed = preprocess("  hello  ")
    result = await summarize(preprocessed)

    assert result == "Dummy Prompt Output"
    await engine.shutdown()


@pytest.mark.asyncio
async def test_mixed_routing_concurrent_pipelines():
    """Many pipelines with mixed routing (compute + ai backends) all complete."""
    compute = NoopExecutionBackend(name="compute")
    ai = NoopExecutionBackend(name="ai")
    engine = await WorkflowEngine.create(backend=[compute, ai])

    @engine.function_task(backend="compute")
    async def preprocess(text):
        return text.strip()

    @engine.prompt_task(backend="ai")
    async def summarize(clean_text):
        return f"Q: {clean_text}"

    @engine.function_task(backend="compute")
    async def postprocess(summary):
        return f"[result] {summary}"

    async def run(doc):
        cleaned = preprocess(doc)
        summary = summarize(cleaned)
        return await postprocess(summary)

    docs = [f"doc-{i}" for i in range(16)]
    results = await asyncio.gather(*[run(d) for d in docs])

    assert len(results) == 16
    # NoopExecutionBackend returns "Dummy Output"
    # for function_task (function not executed)
    assert all(r == "Dummy Output" for r in results)
    await engine.shutdown()


@pytest.mark.asyncio
async def test_default_backend_handles_unrouted_tasks():
    """Tasks without backend= are handled by the default backend in a multi-backend
    setup."""
    compute = NoopExecutionBackend(name="compute")
    ai = NoopExecutionBackend(name="ai")
    engine = await WorkflowEngine.create(backend=[compute, ai])

    @engine.function_task
    async def unrouted():
        return "unrouted result"

    result = await unrouted()
    assert result == "Dummy Output"
    await engine.shutdown()


@pytest.mark.asyncio
async def test_shutdown_drains_all_backends():
    """Shutdown() completes even with multiple backends registered."""
    b1 = NoopExecutionBackend(name="b1")
    b2 = NoopExecutionBackend(name="b2")
    b3 = NoopExecutionBackend(name="b3")
    engine = await WorkflowEngine.create(backend=[b1, b2, b3])

    assert len(engine._backends) == 3
    # Should not raise and should complete
    await engine.shutdown()


@pytest.mark.asyncio
async def test_local_backend_prompt_task_raises_not_implemented():
    """LocalExecutionBackend raises NotImplementedError for prompt_task."""
    local = await LocalExecutionBackend()
    engine = await WorkflowEngine.create(backend=local)

    @engine.prompt_task
    async def ask(q):
        return f"Q: {q}"

    with pytest.raises(NotImplementedError, match="LocalExecutionBackend"):
        await ask("test?")

    await engine.shutdown()

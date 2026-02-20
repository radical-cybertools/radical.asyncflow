"""Concurrent Backends Example.

Demonstrates routing tasks to multiple named backends registered on a single
WorkflowEngine.  Two backends are used in the same workflow:

- ``compute`` — a ``LocalExecutionBackend`` that runs Python function tasks
  (preprocessing, postprocessing).
- ``ai`` — a ``NoopExecutionBackend`` that simulates an AI inference backend
  handling ``prompt_task`` calls.

Replace ``NoopExecutionBackend`` with a RHAPSODY AI backend (e.g.
``DragonVllmInferenceBackend``) in production:

    Requires: pip install rhapsody-py
    RHAPSODY docs: https://radical-cybertools.github.io/rhapsody/
    AsyncFlow integration: https://radical-cybertools.github.io/rhapsody/integrations/#radical-asyncflow-integration

Workflow DAG
------------
    preprocess(raw_text)
         |
    summarize(preprocessed)   ← routed to "ai" backend
         |
    postprocess(summary)
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from radical.asyncflow import LocalExecutionBackend, NoopExecutionBackend, WorkflowEngine
from radical.asyncflow.logging import init_default_logger

logger = logging.getLogger(__name__)


async def main():
    init_default_logger(logging.DEBUG)

    # Initialize two named backends independently
    ai_backend = NoopExecutionBackend(name="ai")
    compute_backend = await LocalExecutionBackend(ThreadPoolExecutor(), name="compute")
    

    # Register both on the same engine — first backend ("compute") is the default
    flow = await WorkflowEngine.create(backend=[compute_backend, ai_backend])

    # --- Task definitions ---

    @flow.function_task(backend="compute")
    async def preprocess(raw_text):
        """Clean and tokenize raw text on the compute backend."""
        logger.info("preprocess: cleaning input")
        cleaned = raw_text.strip().lower()
        await asyncio.sleep(0.05)  # simulate CPU work
        return cleaned

    @flow.prompt_task(backend="ai")
    async def summarize(preprocessed):
        """Build a prompt and send it to the AI inference backend."""
        logger.info("summarize: building prompt")
        return f"Summarize the following text in one sentence:\n\n{preprocessed}"

    @flow.function_task(backend="compute")
    async def postprocess(summary):
        """Format the AI response on the compute backend."""
        logger.info("postprocess: formatting result")
        await asyncio.sleep(0.02)  # simulate formatting work
        return f"[Result] {summary}"

    # --- Run multiple document workflows concurrently ---

    documents = [
        "AsyncFlow is an async-first workflow engine built on Python asyncio.",
        "RHAPSODY provides HPC and AI execution backends for AsyncFlow.",
        "Tasks can be routed to named backends using the backend= decorator parameter.",
        "prompt_task bridges the gap between workflow orchestration and AI inference.",
        "Multi-backend routing lets compute and AI tasks run side by side.",
    ]

    async def run_pipeline(doc_id: int, text: str):
        start = time.time()
        preprocessed = preprocess(text)
        summary = summarize(preprocessed)
        result = await postprocess(summary)
        logger.info(f"doc {doc_id} finished in {time.time() - start:.2f}s: {result}")
        return result

    results = await asyncio.gather(
        *[run_pipeline(i, doc) for i, doc in enumerate(documents)]
    )

    logger.info(f"All {len(results)} documents processed.")
    await flow.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

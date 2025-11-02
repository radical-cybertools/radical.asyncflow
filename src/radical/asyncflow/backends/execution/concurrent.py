"""Concurrent execution backend using Python's concurrent.futures.

This module provides a backend that executes tasks using ThreadPoolExecutor or
ProcessPoolExecutor from the concurrent.futures module.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import subprocess

from concurrent.futures import Executor, ProcessPoolExecutor
from typing import Any, Callable, Optional

from ...constants import StateMapper
from .base import BaseExecutionBackend, Session

try:
    import cloudpickle
except ImportError:
    cloudpickle = None

logger = logging.getLogger(__name__)


class ConcurrentExecutionBackend(BaseExecutionBackend):
    """Simple async-only concurrent execution backend."""

    def __init__(self, executor: Executor):
        if not isinstance(executor, Executor):
            err = "Executor must be ThreadPoolExecutor or ProcessPoolExecutor"
            raise TypeError(err)

        if isinstance(executor, ProcessPoolExecutor) and cloudpickle is None:
            raise ImportError(
                "ProcessPoolExecutor requires 'cloudpickle'. "
                "Install it with: pip install cloudpickle"
            )

        self.executor = executor
        self.tasks: dict[str, dict] = {}
        self.session = Session()
        self._callback_func: Optional[Callable] = None
        self._initialized = False

    def __await__(self):
        """Make backend awaitable."""
        return self._async_init().__await__()

    async def _async_init(self):
        """Async initialization."""
        if not self._initialized:
            StateMapper.register_backend_states_with_defaults(backend=self)
            self._initialized = True
            executor_name = type(self.executor).__name__
            logger.info(f"{executor_name} execution backend started successfully")
        return self

    def get_task_states_map(self):
        return StateMapper(backend=self)

    def register_callback(self, func: Callable):
        self._callback_func = func

    async def _execute_task(self, task: dict) -> tuple[dict, str]:
        """Execute a single task."""
        try:
            if "function" in task and task["function"]:
                return await self._execute_function(task)
            else:
                return await self._execute_command(task)
        except Exception as e:
            task.update(
                {
                    "stderr": str(e),
                    "stdout": None,
                    "exit_code": 1,
                    "exception": e,
                    "return_value": None,
                }
            )
            return task, "FAILED"

    @staticmethod
    def _run_in_process(func, args, kwargs):
        """Execute async function in isolated executor process."""
        func = cloudpickle.loads(func)
        return asyncio.run(func(*args, **kwargs))

    @staticmethod
    def _run_in_thread(func, args, kwargs):
        """Execute async function in isolated executor process."""
        return asyncio.run(func(*args, **kwargs))

    async def _execute_function(self, task: dict) -> tuple[dict, str]:
        """Execute async function task in Process/Thread PoolExecutor."""
        func = task["function"]
        args = task.get("args", [])
        kwargs = task.get("kwargs", {})

        # Serialize the async function
        if isinstance(self.executor, ProcessPoolExecutor):
            func = cloudpickle.dumps(func)
            exec_wrapper = self._run_in_process
        else:
            exec_wrapper = self._run_in_thread

        loop = asyncio.get_running_loop()

        # Submit to the executor
        result = await loop.run_in_executor(
            self.executor,
            exec_wrapper,
            func,
            args,
            kwargs,
        )

        task.update({"return_value": result, "stdout": str(result), "exit_code": 0})
        return task, "DONE"

    async def _execute_command(self, task: dict) -> tuple[dict, str]:
        """Execute command task."""
        cmd = " ".join([task["executable"]] + task.get("arguments", []))
        process = None
        try:
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                close_fds=True,
            )

            # Communicate and get results
            stdout, stderr = await process.communicate()
            exit_code = process.returncode

            # Force cleanup of the process to prevent ResourceWarnings
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()

            task.update(
                {
                    "stdout": stdout.decode(),
                    "stderr": stderr.decode(),
                    "exit_code": exit_code,
                }
            )

        except Exception:
            # Fallback to thread executor with proper subprocess configuration
            def run_subprocess():
                result = subprocess.run(  # noqa: S602
                    cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    close_fds=True,
                )
                return result

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(self.executor, run_subprocess)

            task.update(
                {
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "exit_code": result.returncode,
                }
            )

        finally:
            # Ensure process cleanup to prevent ResourceWarnings
            if process is not None and process.returncode is None:
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
                except Exception as e:
                    # Log cleanup errors but don't fail the task
                    logger.debug(f"Process cleanup error: {e}")

        state = "DONE" if task["exit_code"] == 0 else "FAILED"
        return task, state

    async def _handle_task(self, task: dict) -> None:
        """Handle task execution with callback."""
        result_task, state = await self._execute_task(task)
        if self._callback_func:
            self._callback_func(result_task, state)

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        """Submit tasks for execution."""
        for task in tasks:
            future = asyncio.create_task(self._handle_task(task))

            self.tasks[task["uid"]] = task
            self.tasks[task["uid"]]["future"] = future

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task by its UID.

        Args:
            uid (str): The UID of the task to cancel.

        Returns:
            bool: True if the task was found and cancellation was attempted,
                  False otherwise.
        """
        if uid in self.tasks:
            task = self.tasks[uid]
            future = task["future"]
            if future and future.cancel():
                if self._callback_func:
                    self._callback_func(task, "CANCELED")
                return True
        return False

    async def cancel_all_tasks(self) -> int:
        """Cancel all running tasks."""
        cancelled_count = 0
        for task in self.tasks.values():
            future = task["future"]
            future.cancel()
            cancelled_count += 1
        self.tasks.clear()
        return cancelled_count

    async def shutdown(self) -> None:
        """Shutdown the executor with proper resource cleanup."""
        await self.cancel_all_tasks()

        # Give time for tasks to complete cleanup
        await asyncio.sleep(0.1)

        # Shutdown executor
        self.executor.shutdown(wait=True)

        # Force garbage collection to clean up any remaining resources
        gc.collect()

        logger.info("Concurrent execution backend shutdown complete")

    def build_task(self, uid, task_desc, task_specific_kwargs):
        """Build or prepare a task for execution.

        Note:
            This is a no-op implementation for the concurrent backend.
        """

    def link_explicit_data_deps(
        self, src_task=None, dst_task=None, file_name=None, file_path=None
    ):
        """Link explicit data dependencies between tasks.

        Note:
            This is a no-op implementation for the concurrent backend.
        """

    def link_implicit_data_deps(self, src_task, dst_task):
        """Link implicit data dependencies between tasks.

        Note:
            This is a no-op implementation for the concurrent backend.
        """

    def state(self):
        """Get the current state of the execution backend.

        Returns:
            str: Always returns 'RUNNING' for active backend.
        """
        return "RUNNING"

    def task_state_cb(self, task: dict, state: str) -> None:
        """Callback function invoked when a task's state changes.

        Note:
            This is a no-op implementation for the concurrent backend.
        """

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()

    @classmethod
    async def create(cls, executor: Executor):
        """Alternative factory method for creating initialized backend.

        Args:
            executor: A concurrent.Executor instance (ThreadPoolExecutor
                      or ProcessPoolExecutor).

        Returns:
            Fully initialized ConcurrentExecutionBackend instance.
        """
        backend = cls(executor)
        return await backend

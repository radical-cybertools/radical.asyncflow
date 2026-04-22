from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Callable

import cloudpickle

logger = logging.getLogger(__name__)


class StateMapper:
    """Simple state mapper for noop backend."""

    DONE = "DONE"
    RUNNING = "RUNNING"
    CANCELED = "CANCELED"
    FAILED = "FAILED"

    terminal_states = {DONE, CANCELED, FAILED}


class NoopExecutionBackend:
    """A no-operation execution backend for testing and development purposes.

    This backend simulates task execution without actually running any tasks. All
    submitted tasks immediately return dummy output and transition to DONE state. Useful
    for testing workflow logic without computational overhead.
    """

    def __init__(self, name: str = "default"):
        """Initialize the no-op execution backend.

        Sets up dummy task storage, and default callback function.

        Args:
            name: Name used to identify this backend in the multi-backend registry.
        """
        self.name = name
        self.tasks = {}
        self._callback_func: Callable = lambda task, state: None  # default no-op
        self._work_dir: str = os.getcwd()
        self.is_attached: bool = False
        self.attached_to: list[str] = []

    def state(self) -> str:
        """Get the current state of the no-op execution backend.

        Returns:
            str: Always returns 'IDLE' as this backend performs no actual work.
        """
        return "IDLE"

    def task_state_cb(self, task: dict, state: str) -> None:
        """Callback function invoked when a task's state changes.

        Args:
            task: Dictionary containing task information and metadata.
            state: The new state of the task.

        Note:
            This is a no-op implementation that performs no actions.
        """
        pass

    def get_task_states_map(self) -> StateMapper:
        """Retrieve a mapping of task IDs to their current states.

        Returns:
            StateMapper: Object containing the mapping of task states for this backend.
        """
        return StateMapper()

    def register_callback(self, func: Callable) -> None:
        """Register a callback for task state changes.

        Args:
            func: Function to be called when task states change. Should accept
                task and state parameters.
        """
        self._callback_func = func

    def build_task(self, uid: str, task_desc: dict, task_specific_kwargs: dict) -> None:
        """Build or prepare a task for execution.

        Args:
            uid: Unique identifier for the task.
            task_desc: Dictionary containing task description and metadata.
            task_specific_kwargs: Backend-specific keyword arguments for the task.

        Note:
            This is a no-op implementation that performs no actual task building.
        """
        pass

    async def cancel_task(self, uid: str) -> None:
        pass

    async def submit_tasks(self, tasks: list) -> None:
        """Submit tasks for mock execution.

        Immediately marks all tasks as completed with dummy output without
        performing any actual computation.

        Args:
            tasks: List of task dictionaries to be processed. Each task will
                receive dummy stdout and return_value before being marked as DONE.
        """
        for task in tasks:
            if task.get("prompt"):
                task["stdout"] = "Dummy Prompt Output"
                task["return_value"] = "Dummy Prompt Output"
            else:
                task["stdout"] = "Dummy Output"
                task["return_value"] = "Dummy Output"
            self._callback_func(task, "DONE")

    def link_explicit_data_deps(
        self,
        src_task: dict | None = None,
        dst_task: dict | None = None,
        file_name: str | None = None,
        file_path: str | None = None,
    ) -> None:
        """Handle explicit data dependencies between tasks.

        Args:
            src_task: The source task that produces the dependency.
            dst_task: The destination task that depends on the source.
            file_name: Name of the file that represents the dependency.
            file_path: Full path to the file that represents the dependency.

        Note:
            This is a no-op implementation as this backend doesn't handle dependencies.
        """
        pass

    def link_implicit_data_deps(self, src_task: dict, dst_task: dict) -> None:
        """Handle implicit data dependencies for a task.

        Args:
            src_task: The source task that produces data.
            dst_task: The destination task that depends on the source task's output.

        Note:
            This is a no-op implementation as this backend doesn't handle dependencies.
        """
        pass

    async def shutdown(self) -> None:
        """Shutdown the no-op execution backend.

        Performs cleanup operations. Since this is a no-op backend, no actual resources
        need to be cleaned up.
        """
        pass


class LocalExecutionBackend:
    """Simple async-only concurrent execution backend."""

    def __init__(self, executor: Executor = None, name: str = "default"):
        if not executor:
            executor = ThreadPoolExecutor()
            logger.info(
                "No executor was provided. Falling back to default ThreadPoolExecutor"
            )

        if not isinstance(executor, Executor):
            err = "Executor must be ThreadPoolExecutor or ProcessPoolExecutor"
            raise TypeError(err)

        if isinstance(executor, ProcessPoolExecutor) and cloudpickle is None:
            raise ImportError(
                "ProcessPoolExecutor requires 'cloudpickle'. "
                "Install it with: pip install cloudpickle"
            )

        self.name = name
        self.executor = executor
        self.tasks: dict[str, asyncio.Task] = {}
        self._callback_func: Callable = lambda t, s: None
        self._initialized = False
        self._work_dir: str = os.getcwd()
        self.is_attached: bool = False
        self.attached_to: list[str] = []

    def __await__(self):
        """Make backend awaitable."""
        return self._async_init().__await__()

    async def _async_init(self):
        """Unified async initialization with backend and task state registration.

        Pattern:
        1. Register backend states first
        2. Register task states
        3. Set backend state to INITIALIZED
        4. Initialize backend components (if needed)
        """
        if not self._initialized:
            try:
                # Register task states
                logger.debug("Registering task states...")

                # Initialize backend components (already done in __init__)
                self._initialized = True

                executor_name = type(self.executor).__name__
                logger.info(f"{executor_name} execution backend started successfully")

            except Exception as e:
                logger.exception(f"Concurrent backend initialization failed: {e}")
                self._initialized = False
                raise

        return self

    def get_task_states_map(self) -> StateMapper:
        return StateMapper()

    def register_callback(self, func: Callable) -> None:
        """Register a callback for task state changes.

        Args:
            func: Function to be called when task states change. Should accept
                task and state parameters.
        """
        self._callback_func = func

    async def _execute_task(self, task: dict) -> tuple[dict, str]:
        """Execute a single task."""
        try:
            if task.get("prompt"):
                raise NotImplementedError(
                    "LocalExecutionBackend does not support prompt_task. "
                    "Register an AI execution backend and route with "
                    "@flow.prompt_task(backend='<name>')."
                )
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
        executable = task["executable"]
        arguments = task.get("arguments", [])
        backend_kwargs = task.get("task_backend_specific_kwargs", {})
        execute_in_shell = backend_kwargs.get("shell", True)

        if execute_in_shell:
            # Shell mode: join executable and arguments into single command string
            cmd = " ".join([executable] + arguments)
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        else:
            # Exec mode: pass executable and arguments separately (no shell)
            process = await asyncio.create_subprocess_exec(
                executable,
                *arguments,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        stdout, stderr = await process.communicate()

        task.update(
            {
                "stdout": stdout.decode(),
                "stderr": stderr.decode(),
                "exit_code": process.returncode,
            }
        )

        state = "DONE" if task["exit_code"] == 0 else "FAILED"
        return task, state

    async def _handle_task(self, task: dict) -> None:
        """Handle task execution with callback."""
        try:
            result_task, state = await self._execute_task(task)
            # Set state on the task object itself before callback
            self._callback_func(result_task, state)
        except Exception as e:
            logger.exception(f"Error handling task {task.get('uid')}: {e}")
            raise

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> list[asyncio.Task]:
        """Submit tasks for execution."""

        submitted_tasks = []

        for task in tasks:
            future = asyncio.create_task(self._handle_task(task))
            submitted_tasks.append(future)

            self.tasks[task["uid"]] = task
            self.tasks[task["uid"]]["future"] = future

        return submitted_tasks

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
                # Set state on the task object itself before callback
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
        """Shutdown the executor."""
        # Set backend state to SHUTDOWN
        await self.cancel_all_tasks()
        self.executor.shutdown(wait=True)
        logger.info("Concurrent execution backend shutdown complete")

    def build_task(self, uid: str, task_desc: dict, task_specific_kwargs: dict) -> None:
        pass

    def link_explicit_data_deps(
        self,
        src_task: dict | None = None,
        dst_task: dict | None = None,
        file_name: str | None = None,
        file_path: str | None = None,
    ) -> None:
        pass

    def link_implicit_data_deps(self, src_task: dict, dst_task: dict) -> None:
        pass

    def state(self) -> str:
        """Get backend state.

        Returns:
            str: Current backend state (INITIALIZED, RUNNING, SHUTDOWN)
        """
        return "INITIALIZED" if self._initialized else "IDLE"

    def task_state_cb(self):
        pass

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()

    @classmethod
    async def create(cls, executor: Executor) -> LocalExecutionBackend:
        """Alternative factory method for creating initialized backend.

        Args:
            executor: A concurrent.Executor instance (ThreadPoolExecutor
                      or ProcessPoolExecutor).

        Returns:
            Fully initialized LocalExecutionBackend instance.
        """
        backend = cls(executor)
        return await backend

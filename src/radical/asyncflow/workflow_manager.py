# flake8: noqa
from __future__ import annotations

import asyncio
import inspect
import logging
import os
import signal
from collections import defaultdict, deque
from functools import wraps
from itertools import count
from pathlib import Path
from typing import Any, Callable, Optional, Union

import typeguard

from .backends.execution.base import BaseExecutionBackend
from .backends.execution.noop import NoopExecutionBackend
from .data import InputFile, OutputFile
from .errors import DependencyFailureError
from .utils import get_next_uid
from .utils import reset_uid_counter
from .utils import get_event_loop_or_raise

TASK = "task"
BLOCK = "block"
FUNCTION = "function"
EXECUTABLE = "executable"

logger = logging.getLogger(__name__)


class WorkflowEngine:
    """
    An asynchronous workflow manager that uses asyncio event loops and coroutines
    to manage and execute workflow components (blocks and/or tasks) within
    Directed Acyclic Graph (DAG) or Chain Graph (CG) structures.

    This class provides async/await operations and handles task dependencies,
    input/output data staging, and execution.

    Attributes:
        loop (asyncio.AbstractEventLoop): The asyncio event loop (current running loop).
        backend (BaseExecutionBackend): The execution backend used for task execution.
        dry_run (bool): Indicates whether the engine is in dry-run mode.
        work_dir (str): The working directory for the workflow session.
        log (ru.Logger): Logger instance for logging workflow events.
        prof (ru.Profiler): Profiler instance for profiling workflow execution.
    """

    @typeguard.typechecked
    def __init__(
        self,
        backend: BaseExecutionBackend,
        dry_run: bool = False,
        implicit_data: bool = True,
    ) -> None:
        """
        Initialize the WorkflowEngine (sync part only).

        Note: This is a private constructor. Use WorkflowEngine.create() instead.

        Args:
            backend: Execution backend (required, pre-validated)
            dry_run: Whether to run in dry-run mode
            implicit_data: Whether to enable implicit data dependency linking
        """
        # Get the current running loop - assume it exists
        self.loop = get_event_loop_or_raise("WorkflowEngine")

        # Store backend (already validated by create method)
        self.backend = backend

        # Initialize core attributes
        self.running = []
        self.components = {}
        self.resolved = set()
        self.dependencies = {}
        self.dry_run = dry_run
        self.queue = asyncio.Queue()
        self.implicit_data_mode = implicit_data

        # Optimization: Track component state changes
        self._ready_queue = deque()
        self._dependents_map = defaultdict(set)
        self._dependency_count = {}
        self._component_change_event = asyncio.Event()

        self.task_states_map = self.backend.get_task_states_map()

        # Setup working directory
        self.work_dir = self.backend.session.path or os.getcwd()

        # Register callback with backend
        self.backend.register_callback(self.task_callbacks)

        # Define decorators
        self.block = self._register_decorator(comp_type=BLOCK)
        self.function_task = self._register_decorator(
            comp_type=TASK, task_type=FUNCTION
        )
        self.executable_task = self._register_decorator(
            comp_type=TASK, task_type=EXECUTABLE
        )

        # Initialize async task references (will be set in _start_async_components)
        self._run_task = None
        self._shutdown_event = asyncio.Event()  # Added shutdown signal

        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """
        Register signal handlers for graceful shutdown on SIGHUP, SIGTERM, and SIGINT.
        """
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for sig in signals:
            try:
                self.loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(
                        self._handle_shutdown_signal(s), name=f"{sig.name}-task"
                    ),
                )
                logger.debug(f"Registered signal handler for {sig.name}")
            except NotImplementedError:
                logger.exception(f"Signal {sig.name} not supported on this platform")

    async def _handle_shutdown_signal(
        self, sig: signal.Signals, source: str = "external"
    ):
        """
        Handle received signals by initiating a graceful shutdown.

        Args:
            sig: The signal received (e.g., SIGHUP, SIGTERM, SIGINT)
        """
        if self._shutdown_event.is_set():
            logger.info(f"Shutdown process already started")
            return

        logger.info(
            f"Received {source} shutdown signal ({sig.name}), "
            "initiating graceful shutdown"
        )

        await self.shutdown()

    @classmethod
    async def create(
        cls,
        backend: Optional[BaseExecutionBackend] = None,
        dry_run: bool = False,
        implicit_data: bool = True,
    ) -> "WorkflowEngine":
        """
        Factory method to create and initialize a WorkflowEngine.

        Args:
            backend: Execution backend. If None and dry_run=True,
                     uses NoopExecutionBackend
            dry_run: Whether to run in dry-run mode
            implicit_data: Whether to enable implicit data dependency linking

        Returns:
            WorkflowEngine: Fully initialized workflow engine

        Example:
            engine = await WorkflowEngine.create(dry_run=True)
        """
        # Setup and validate backend first
        validated_backend = cls._setup_execution_backend(backend, dry_run)

        # Create instance with validated backend
        instance = cls(
            backend=validated_backend, dry_run=dry_run, implicit_data=implicit_data
        )

        # Initialize async components
        await instance._start_async_components()

        return instance

    @staticmethod
    def _setup_execution_backend(
        backend: Optional[BaseExecutionBackend], dry_run: bool
    ) -> BaseExecutionBackend:
        """Setup and validate the execution backend."""
        if backend is None:
            if dry_run:
                return NoopExecutionBackend()
            else:
                raise RuntimeError(
                    'An execution backend must be provided when not in "dry_run" mode.'
                )
        else:
            if dry_run and not isinstance(backend, NoopExecutionBackend):
                raise RuntimeError('Dry-run only supports the "NoopExecutionBackend".')
            return backend

    async def _start_async_components(self):
        """Start internal async components (run and submit tasks)."""
        self._run_task = asyncio.create_task(self.run(), name="run-component")

        comps = [self._run_task.get_coro().__name__]

        for comp in comps:
            logger.debug(f"Started {comp} component")

    def _update_dependency_tracking(self, comp_uid: str):
        """Update dependency tracking structures for a component."""
        dependencies = self.dependencies[comp_uid]

        # Count unresolved dependencies
        unresolved_count = 0
        for dep in dependencies:
            dep_uid = dep["uid"]
            if (
                dep_uid not in self.resolved
                or not self.components[dep_uid]["future"].done()
            ):
                unresolved_count += 1
                # Track reverse dependencies
                self._dependents_map[dep_uid].add(comp_uid)

        self._dependency_count[comp_uid] = unresolved_count

        # If no dependencies, add to ready queue
        if unresolved_count == 0:
            self._ready_queue.append(comp_uid)

    def _register_decorator(self, comp_type: str, task_type: Optional[str] = None):
        """Create a decorator factory for registering workflow components.

        This method creates a decorator that can be used to register functions
        as workflow components (tasks or blocks). The decorator handles task
        description extraction, merging of definition-time and call-time parameters,
        and component registration.

        Args:
            comp_type (str): The type of component to register (e.g., "task", "block").
            task_type (Optional[str]): The specific task type for task components
                (e.g., "executable", "function"). Not used for block components.
                Defaults to None.

        Returns:
            Callable: A decorator factory function that accepts optional parameters:
                - possible_func (Union[Callable, None]): The function to decorate,
                  or None if decorator is called with parameters.
                - service (bool): Whether the component should be treated as a service.
                    Defaults to False.

        Raises:
            TypeError: If service parameter is not a boolean, if the decorated object
                is not callable, or if task_description default value is not a dict.

        Example:
            >>> # Create task decorator
            >>> task_decorator = engine._register_decorator("task", "executable")
            >>>
            >>> # Use without parameters
            >>> @task_decorator
            >>> async def my_task():
            >>>     return "result"
            >>>
            >>> # Use with service parameter
            >>> @task_decorator(service=True)
            >>> async def my_service():
            >>>     return "service_result"
            >>>
            >>> # Function with task_description default
            >>> @task_decorator
            >>> async def configured_task(task_description={'cpu': 4}):
            >>>     return "configured_result"

        Note:
            The decorator extracts and merges task_description parameters from both
            function definition defaults and runtime kwargs. Call-time parameters
            take precedence over definition-time defaults.
        """

        def outer(possible_func: Union[Callable, None] = None, service: bool = False):
            if not isinstance(service, bool):
                raise TypeError(
                    f"'service' must be a boolean, got {type(service).__name__}"
                )

            def actual_decorator(func: Callable) -> Callable:
                if not callable(func):
                    raise TypeError(
                        f"Expected a callable function, got {type(func).__name__}"
                    )

                # Extract definition-time task_description (if any)
                sig = inspect.signature(func)
                task_description_def = {}

                if "task_description" in sig.parameters:
                    param = sig.parameters["task_description"]
                    if param.default is not inspect.Parameter.empty:
                        if not isinstance(param.default, dict):
                            raise TypeError(
                                f"Default value for 'task_description' in function "
                                f"'{func.__name__}' must be a dict, got "
                                f"{type(param.default).__name__}"
                            )
                        task_description_def = param.default

                setattr(func, "__task_description__", task_description_def)

                @wraps(func)
                def wrapped(*args, **kwargs):
                    try:
                        # Extract task_description from call-time kwargs
                        task_description_call = kwargs.pop("task_description", {}) or {}

                        if not isinstance(task_description_call, dict):
                            raise TypeError(
                                f"Expected 'task_description' to be a dict, "
                                f"got {type(task_description_call)}"
                            )

                        # Merge call-time overrides over def-time defaults
                        task_description_final = {
                            **task_description_def,
                            **task_description_call,
                        }

                        registered_func = self._handle_flow_component_registration(
                            func,
                            is_service=service,
                            comp_type=comp_type,
                            task_type=task_type,
                            task_backend_specific_kwargs=task_description_final,
                        )

                        return registered_func(*args, **kwargs)

                    except Exception as e:
                        # Add context to any errors that occur
                        # during registration/execution
                        raise type(e)(
                            f"Error in decorated function '{func.__name__}': {str(e)}"
                        ) from e

                return wrapped

            # Handle both @decorator and @decorator(...)
            if callable(possible_func):
                return actual_decorator(possible_func)
            return actual_decorator

        return outer

    def _handle_flow_component_registration(
        self,
        func: Callable,
        is_service: bool,
        comp_type: str,
        task_type: Optional[str],
        task_backend_specific_kwargs: Optional[dict] = None,
    ):
        """Handles registration of async tasks and blocks as workflow components.

        Creates a decorator that manages component registration for async functions.
        The decorator creates appropriate asyncio.Future to track execution state.

        Args:
            func (Callable): Async function to be registered as a workflow component
            is_service (bool): Whether the component is a service
            comp_type (str): Component type (e.g., "task", "block")
            task_type (str): Task type, determines result handling
            task_backend_specific_kwargs (dict, optional): Backend-specific kwargs.
            Defaults to None.

        Returns:
            Callable: A decorator that:
                - Wraps the original async function
                - Registers it as a workflow component
                - Returns a future tracking the component's execution

        Note:
            Only async functions are supported. The decorator handles awaiting
            async functions
            and collecting their return values appropriately.
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create async future - we only support async
            comp_fut = asyncio.Future()

            comp_desc = {
                "args": args,
                "function": func,
                "kwargs": kwargs,
                "is_service": is_service,
                "task_backend_specific_kwargs": task_backend_specific_kwargs or {},
            }

            # Only handle async functions
            if asyncio.iscoroutinefunction(func):

                async def async_wrapper():
                    # Get executable from async function call
                    comp_desc[EXECUTABLE] = (
                        await func(*args, **kwargs) if task_type == EXECUTABLE else None
                    )
                    return self._register_component(
                        comp_fut, comp_type, comp_desc, task_type
                    )

                # FIXME: assign name for this comp (comp uid)
                asyncio.create_task(async_wrapper())
            else:
                # Raise error for non-async functions since we only support async
                raise TypeError(
                    f"Function {func.__name__} must be async. "
                    "For sync functions, wrap them in async def."
                )

            return comp_fut

        return wrapper

    def _register_component(
        self,
        comp_fut: asyncio.Future,
        comp_type: str,
        comp_desc: dict,
        task_type: Optional[str] = None,
    ):
        """Registers a workflow component with shared registration logic.

        Handles the core registration process for both tasks and blocks.
        Assigns identifiers, sets up metadata, detects dependencies, and manages
        component storage.

        Args:
            comp_fut (asyncio.Future): Future object for tracking component execution
            comp_type (str): Type of component ('task' or 'block')
            comp_desc (dict): Component description containing:
                - function: The callable to execute
                - args: Function arguments
                - kwargs: Function keyword arguments
                - task_backend_specific_kwargs: Backend-specific parameters
            task_type (str, optional): Specific task type for execution handling.
                                       Defaults to None.

        Returns:
            asyncio.Future: The future object with:
                - Assigned component ID
                - Associated component description
                - Registration in workflow tracking structures

        Raises:
            ValueError: If an executable task returns non-string output

        Side Effects:
            - Updates self.components with new component
            - Updates self.dependencies with component dependencies
            - Logs component registration
        """

        # make sure not to specify both func and executable at the same time
        comp_desc["name"] = comp_desc["function"].__name__
        comp_desc["uid"] = self._assign_uid(prefix=comp_type)

        if task_type == EXECUTABLE:
            # For executable tasks, validate the executable value
            executable_value = comp_desc.get(EXECUTABLE)
            if executable_value is None:
                raise ValueError(
                    f"Executable task '{comp_desc['name']}' returned None — "
                    "must return a string command"
                )
            if not isinstance(executable_value, str):
                raise ValueError(
                    "Executable task must return a string, got "
                    f"{type(executable_value)}"
                )
            comp_desc[FUNCTION] = None  # Clear function since we're using executable
        else:
            # For regular tasks, clear executable and keep function
            comp_desc[EXECUTABLE] = None

        # Detect dependencies
        comp_deps, input_files_deps, output_files_deps = self._detect_dependencies(
            comp_desc["args"]
        )

        comp_desc["metadata"] = {
            "dependencies": comp_deps,
            "input_files": input_files_deps,
            "output_files": output_files_deps,
        }

        # Setup future
        comp_fut.id = comp_desc["uid"].split(f"{comp_type}.")[1]
        setattr(comp_fut, comp_type, comp_desc)

        # Store component that will be sent to the backend
        self.components[comp_desc["uid"]] = {
            "type": comp_type,
            "future": comp_fut,
            "description": comp_desc,
        }

        self.dependencies[comp_desc["uid"]] = comp_deps

        logger.debug(
            f"Registered {comp_type}: '{comp_desc['name']}' "
            f"with id of {comp_desc['uid']}"
        )

        # Update dependency tracking
        self._update_dependency_tracking(comp_desc["uid"])
        self._component_change_event.set()

        # Setup cancel hook
        comp_fut.cancel = self._setup_future_cancel_hook(comp_fut, comp_desc["uid"])

        return comp_fut

    def _setup_future_cancel_hook(
        self, fut: asyncio.Future, uid: str
    ) -> Callable[..., Any]:
        """Sets up a custom cancel hook for a future to enable backend cancellation.

        Stores the original `cancel` method and overrides it with a patched version
        that triggers task cancellation via the execution backend.

        Args:
            fut (Union[SyncFuture, AsyncFuture]): The future object to modify.
            uid (str): The unique identifier for the task.

        Returns:
            Callable[..., Any]: The patched cancel function.
        """
        orig_cancel = fut.cancel
        # to be used for tasks that are not submitted to the
        # execution backend yet.
        fut.original_cancel = orig_cancel

        def patched_cancel(*args, **kwargs):
            if not fut.done() and uid in self.running:
                # NOTE: Returning True means cancellation was requested/scheduled,
                # but not guaranteed.
                # This follows asyncio.Future.cancel() behavior, which is best-effort.
                # Actual cancellation is handled asynchronously by the backend and
                # delivered to the WorkflowEngine via callbacks only.
                logger.info(
                    f"Cancellation requested for {uid} "
                    "('running') from the execution backend"
                )
                asyncio.create_task(
                    self.backend.cancel_task(uid)
                )  # fire and forget (non-blocking)
                return True
            else:
                # Task is pending -> cancel locally
                logger.info(f"Cancellation requested for {uid} (pending) locally")
                return fut.original_cancel

        return patched_cancel

    def _assign_uid(self, prefix: str) -> str:
        """
        Generates a unique identifier (UID) for a flow component using a counter.

        Args:
            prefix (str): The prefix to use for the UID.

        Returns:
            str: A unique identifier like 'task.000001'.
        """
        uid = get_next_uid()
        return f"{prefix}.{uid}"

    def _detect_dependencies(self, possible_dependencies):
        """
        Detects and categorizes possible dependencies into blocks/tasks, input files,
        and output files.

        This method iterates over a list of possible dependencies and classifies
        them into three categories:
        - Blocks/Tasks that are instances of `Future` with a `task` or `block`
          attribute (task dependencies).
        - Input files that are instances of `InputFile`
          (files required by the task only).
        - Output files that are instances of `OutputFile`
          (files produced by the task only).

        Args:
            possible_dependencies (list): A list of possible dependencies,
            which can include blocks, tasks, input files, and output files.

        Returns:
            tuple: A tuple containing three lists:
                - `dependencies`: A list of flow components that need to be completed.
                - `input_files`: A list of input file names that need to be fetched.
                - `output_files`: A list of output file names that need to be retrieved
                   from the task folder.
        """
        dependencies = []
        input_files = []
        output_files = []

        for possible_dep in possible_dependencies:
            # Flow component dependency
            if isinstance(possible_dep, asyncio.Future):
                if hasattr(possible_dep, TASK):
                    possible_dep = possible_dep.task
                elif hasattr(possible_dep, BLOCK):
                    possible_dep = possible_dep.block
                dependencies.append(possible_dep)
            # Input file dependency
            elif isinstance(possible_dep, InputFile):
                input_files.append(possible_dep.filename)
            # Output file dependency
            elif isinstance(possible_dep, OutputFile):
                output_files.append(possible_dep.filename)

        return dependencies, input_files, output_files

    async def _extract_dependency_values(self, comp_desc: dict):
        """
        Resolve Future objects in args and kwargs to their actual values.
        This is called right before submission when all dependencies are
        guaranteed to be done.

        Args:
            comp_desc: Component description dict

        Returns:
            tuple: (resolved_args, resolved_kwargs)
        """
        resolved_args = []
        resolved_kwargs = {}

        # Resolve args
        for arg in comp_desc["args"]:
            if isinstance(arg, asyncio.Future):
                resolved_args.append(arg.result())  # Safe call, non-blocking
            else:
                resolved_args.append(arg)

        # Resolve kwargs
        for key, value in comp_desc["kwargs"].items():
            if isinstance(value, asyncio.Future):
                resolved_kwargs[key] = (
                    value.result()
                )  # Safe call are done, non-blocking
            else:
                resolved_kwargs[key] = value

        return tuple(resolved_args), resolved_kwargs

    def _clear_internal_records(self):
        """Clear workflow components and their dependencies."""
        self.components.clear()
        self.dependencies.clear()
        self._ready_queue.clear()
        self._dependents_map.clear()
        self._dependency_count.clear()

        reset_uid_counter()

    def _notify_dependents(self, comp_uid: str):
        """Notify dependents that a component has completed and update ready queue."""
        for dependent_uid in self._dependents_map[comp_uid]:
            if dependent_uid in self._dependency_count:
                self._dependency_count[dependent_uid] -= 1
                if self._dependency_count[dependent_uid] == 0:
                    self._ready_queue.append(dependent_uid)

        # Clean up
        del self._dependents_map[comp_uid]
        if comp_uid in self._dependency_count:
            del self._dependency_count[comp_uid]

    def _create_dependency_failure_exception(self, comp_desc: dict, failed_deps: list):
        """
        Create a DependencyFailureError exception that shows both the immediate failure
        and the root cause from failed dependencies.

        Args:
            comp_desc (dict): Description of the component that cannot execute
            failed_deps (list): List of exceptions from failed dependencies

        Returns:
            DependencyFailureError: Exception with detailed failure information
        """
        # Get the first failed dependency's exception as root cause
        root_exception = failed_deps[0]
        # Create a descriptive error message
        error_message = (
            f"Cannot execute '{comp_desc['name']}' due to dependency failure"
        )

        # Get names of failed dependencies
        failed_dep_names = []
        dependencies = self.dependencies[comp_desc["uid"]]
        dep_futures = [self.components[dep["uid"]]["future"] for dep in dependencies]

        for dep, dep_future in zip(dependencies, dep_futures):
            if dep_future.exception() is not None:
                failed_dep_names.append(dep["name"])

        return DependencyFailureError(
            message=error_message,
            failed_dependencies=failed_dep_names,
            root_cause=root_exception,
        )

    def _get_dependency_output_files(self, dependencies):
        """
        Helper method to get all output files from dependencies.

        Args:
            dependencies: List of dependency descriptions

        Returns:
            set: Set of output file names from all dependencies
        """
        dependency_output_files = set()
        for dep in dependencies:
            dep_desc = self.components[dep["uid"]]["description"]
            for output_file in dep_desc["metadata"]["output_files"]:
                dependency_output_files.add(Path(output_file).name)
        return dependency_output_files

    async def run(self):
        """Manages asynchronous execution of workflow components.

        Continuously monitors and manages workflow components, handling their
        dependencies and execution states. Performs dependency resolution and
        prepares components for execution when their dependencies are satisfied.

        Workflow Process:
            1. Monitors unresolved components
            2. Checks dependency resolution status
            3. Prepares resolved components for execution
            4. Handles data staging between components
            5. Submits ready components to execution

        Args:
            None

        Returns:
            None

        Raises:
            asyncio.CancelledError: If the coroutine is cancelled during execution

        State Management:
            - unresolved (set): Component UIDs with pending dependencies
            - resolved (set): Component UIDs with satisfied dependencies
            - running (list): Currently executing component UIDs
            - dependencies (dict): Maps component UIDs to dependency info
            - components (dict): Maps UIDs to component descriptions and futures
            - queue (asyncio.Queue): Execution queue for ready components

        Note:
            - Runs indefinitely until cancelled or shutdown is signaled
            - Uses sleep intervals to prevent busy-waiting
            - Handles both implicit and explicit data dependencies
            - Trigger internal shutdown on loop failure
        """
        while not self._shutdown_event.is_set():
            try:
                to_submit = []

                # Process ready components
                while self._ready_queue and not self._shutdown_event.is_set():
                    comp_uid = self._ready_queue.popleft()

                    # Skip if already processed
                    if comp_uid in self.resolved or comp_uid in self.running:
                        continue

                    # Check if future is already done (could be cancelled/failed)
                    if self.components[comp_uid]["future"].done():
                        self.resolved.add(comp_uid)
                        self._notify_dependents(comp_uid)
                        continue

                    # Verify dependencies are still met
                    dependencies = self.dependencies[comp_uid]
                    dep_futures = [
                        self.components[dep["uid"]]["future"] for dep in dependencies
                    ]

                    failed_deps = []
                    cancelled_deps = []

                    for fut in dep_futures:
                        if fut.cancelled():
                            cancelled_deps.append(fut)
                        elif fut.exception() is not None:
                            failed_deps.append(fut.exception())

                    # Handle dependency issues
                    if cancelled_deps or failed_deps:
                        comp_desc = self.components[comp_uid]["description"]

                        if cancelled_deps:
                            logger.info(
                                f"Cancelling {comp_desc['name']} "
                                "due to cancelled dependencies"
                            )
                            self.handle_task_cancellation(
                                comp_desc, self.components[comp_uid]["future"]
                            )
                        else:  # failed_deps
                            chained_exception = (
                                self._create_dependency_failure_exception(
                                    comp_desc, failed_deps
                                )
                            )
                            logger.error(
                                f"Dependency failure for {comp_desc['name']}: "
                                f"{chained_exception}"
                            )
                            self.handle_task_failure(
                                comp_desc,
                                self.components[comp_uid]["future"],
                                chained_exception,
                            )

                        # Common cleanup
                        self.resolved.add(comp_uid)
                        self._notify_dependents(comp_uid)
                        continue

                    # Handle data dependencies for tasks
                    comp_desc = self.components[comp_uid]["description"]
                    if self.components[comp_uid]["type"] == TASK:
                        explicit_files_to_stage = []

                        for dep in dependencies:
                            dep_desc = self.components[dep["uid"]]["description"]

                            # Link implicit data dependencies
                            if self.implicit_data_mode and not dep_desc["metadata"].get(
                                "output_files"
                            ):
                                logger.debug(
                                    f"Linking implicit file(s): from {dep_desc['name']} "
                                    f"to {comp_desc['name']}"
                                )
                                self.backend.link_implicit_data_deps(
                                    dep_desc, comp_desc
                                )

                            # Link explicit data dependencies
                            for output_file in dep_desc["metadata"]["output_files"]:
                                if output_file in comp_desc["metadata"]["input_files"]:
                                    logger.debug(
                                        f"Linking explicit file ({output_file}) "
                                        f"from {dep_desc['name']} "
                                        f"to {comp_desc['name']}"
                                    )
                                    data_dep = self.backend.link_explicit_data_deps(
                                        src_task=dep_desc,
                                        dst_task=comp_desc,
                                        file_name=output_file,
                                    )
                                    explicit_files_to_stage.append(data_dep)

                        # Input staging data dependencies
                        dependency_output_files = self._get_dependency_output_files(
                            dependencies
                        )
                        staged_targets = {
                            Path(item["target"]).name
                            for item in explicit_files_to_stage
                        }

                        for input_file in comp_desc["metadata"]["input_files"]:
                            input_basename = Path(input_file).name
                            if (
                                input_basename not in staged_targets
                                and input_basename not in dependency_output_files
                            ):
                                logger.debug(
                                    f"Staging {input_file} "
                                    f"to {comp_desc['name']} work dir"
                                )
                                data_dep = self.backend.link_explicit_data_deps(
                                    src_task=None,
                                    dst_task=comp_desc,
                                    file_name=input_basename,
                                    file_path=input_file,
                                )
                                explicit_files_to_stage.append(data_dep)

                    try:
                        # Update the component description with resolved values
                        (
                            comp_desc["args"],
                            comp_desc["kwargs"],
                        ) = await self._extract_dependency_values(comp_desc)
                    except Exception as e:
                        logger.error(
                            f"Failed to resolve future for {comp_desc['name']}: {e}"
                        )
                        self.handle_task_failure(
                            comp_desc, self.components[comp_uid]["future"], e
                        )
                        self.resolved.add(comp_uid)
                        self._notify_dependents(comp_uid)
                        continue

                    to_submit.append(comp_desc)
                    res_deps = [dep["name"] for dep in dependencies]
                    msg = f"Ready to submit: {comp_desc['name']}"
                    msg += f" with resolved dependencies: {res_deps}"
                    logger.debug(msg)

                # Submit ready components
                if to_submit:
                    await self.submit(to_submit)
                    for comp_desc in to_submit:
                        comp_uid = comp_desc["uid"]
                        self.running.append(comp_uid)
                        self.resolved.add(comp_uid)

                # Check for completed components and update dependency tracking
                completed_components = []
                for comp_uid in list(self.running):
                    if self.components[comp_uid]["future"].done():
                        completed_components.append(comp_uid)
                        self.running.remove(comp_uid)

                # Notify dependents of completed components
                for comp_uid in completed_components:
                    self._notify_dependents(comp_uid)

                # Signal changes
                if completed_components:
                    self._component_change_event.set()

                # If nothing is ready/running, wait for changes or shutdown
                if not self._ready_queue and not to_submit and not completed_components:
                    try:
                        # Create tasks for event waiting
                        event_task = asyncio.create_task(
                            self._component_change_event.wait(),
                            name="component-event-task",
                        )
                        shutdown_task = asyncio.create_task(
                            self._shutdown_event.wait(), name="shutdown-event-task"
                        )

                        done, pending = await asyncio.wait(
                            [event_task, shutdown_task],
                            return_when=asyncio.FIRST_COMPLETED,
                            timeout=1.0,
                        )
                        # Cancel any pending tasks to clean up
                        for task in pending:
                            task.cancel()
                        # Clear component change event if it was set
                        if event_task in done:
                            self._component_change_event.clear()
                    except asyncio.CancelledError:
                        # If we get cancelled, make sure to clean up our tasks
                        for task in [event_task, shutdown_task]:
                            if not task.done():
                                task.cancel()
                        raise
                else:
                    await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                logger.debug("Run component stopped")
                break
            except Exception as e:
                logger.exception(f"Error in run loop: {e}")
                await self._handle_shutdown_signal(signal.SIGUSR1, source="internal")
                break

    async def submit(self, objects):
        """Manages asynchronous submission of tasks/blocks to the execution backend.

        Retrieves and submit ready tasks and blocks for execution. Separates incoming
        objects into tasks and blocks based on their UID pattern, and dispatches each
        to the appropriate backend method.

        Submission Process:
            1. Receive batch of objects
            2. Filters objects into tasks and blocks
            3. Submits tasks via `backend.submit_tasks`
            4. Submits blocks via `_submit_blocks` asynchronously

        Args:
            objects: Tasks and blocks are identified by `uid` field content

        Returns:
            None

        Raises:
            Exception: If an unexpected error occurs during submission
        """
        try:
            # Separate tasks and blocks
            tasks = [t for t in objects if t and BLOCK not in t["uid"]]
            blocks = [b for b in objects if b and TASK not in b["uid"]]

            logger.info(f"Submitting {len(objects)} tasks/blocks for execution")
            logger.debug(f"Submitting: {[b['name'] for b in objects]}")

            if tasks:
                await self.backend.submit_tasks(tasks)
            if blocks:
                await self._submit_blocks(blocks)
        except Exception as e:
            logger.exception(f"Error in submit component: {e}")
            raise

    async def _submit_blocks(self, blocks: list):
        """Submits workflow blocks for asynchronous execution.

        Iterates over a list of resolved workflow blocks and schedules each for
        execution as a coroutine. Each block's function is invoked with its
        specified arguments, and its future is updated to reflect the execution
        result.

        Submission Process:
            1. Iterates through provided blocks
            2. Extracts `function`, `args`, and `kwargs` for each block
            3. Retrieves the associated `future` for tracking execution result
            4. Schedules execution using `asyncio.create_task`

        Args:
            blocks (list): A list of block descriptors (dicts) containing
                - 'uid' (str): Unique identifier of the block
                - 'function' (Callable): The coroutine function to execute
                - 'args' (list): Positional arguments for the function
                - 'kwargs' (dict): Keyword arguments for the function

        Returns:
            None

        Raises:
            None

        State Management:
            - components (dict): Maps block UIDs to their metadata and future objects
            - Each block execution is tracked via its associated future

        Note:
            - Does not block; schedules all executions asynchronously
            - Relies on `execute_block` to handle the actual function call and future
        """
        for block in blocks:
            args = block["args"]
            kwargs = block["kwargs"]
            func = block["function"]
            block_fut = self.components[block["uid"]]["future"]

            # Execute the block function as a coroutine
            asyncio.create_task(
                self.execute_block(block_fut, func, *args, **kwargs), name=block["uid"]
            )

    async def execute_block(
        self, block_fut: asyncio.Future, func: Callable, *args, **kwargs
    ):
        """Executes a block function and sets its result on the associated future.

        Calls the given function with provided args, awaiting it if it's a coroutine,
        or running it in the executor otherwise. On completion, updates the `block_fut`
        with the result or exception.

        Args:
            block_fut (asyncio.Future): Future to store the result or exception.
            func (Callable): Function or coroutine function to execute.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            None
        """

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                # Run sync function in executor
                result = await self.loop.run_in_executor(None, func, *args, **kwargs)

            if not block_fut.done():
                block_fut.set_result(result)
        except Exception as e:
            if not block_fut.done():
                block_fut.set_exception(e)

    def handle_task_success(self, task: dict, task_fut: asyncio.Future):
        """Handles successful task completion and updates the associated future.

        Sets the result of the task's future based on whether the task was a function
        or a shell command. Raises an error if the future is already resolved.

        Args:
            task (dict): Completed task descriptor containing
                - 'uid' (str): Unique task identifier
                - 'return_value' / 'stdout': Result of the task execution
            task_fut (asyncio.Future): Future to set the result on.

        Returns:
            None

        Raises:
            None
        """
        internal_task = self.components[task["uid"]]["description"]

        if not task_fut.done():
            if internal_task[FUNCTION]:
                task_fut.set_result(task["return_value"])
            else:
                task_fut.set_result(task["stdout"])
        else:
            logger.warning(
                f'Attempted to handle an already finished task "{task["uid"]}"'
            )

    def handle_task_failure(
        self,
        task: dict,
        task_fut: asyncio.Future,
        override_error_message: Union[str, Exception] = None,
    ) -> None:
        """Handles task failure and sets the appropriate exception on the future.

        Marks the given task's future as failed by setting an exception derived from
        either a provided override error or the task's own recorded error/stderr. Logs
        a warning if the future is already resolved.

        Args:
            task (dict): Dictionary with task details, including:
                - 'uid' (str): Unique task identifier
                - 'exception' or 'stderr': Error information from execution
            task_fut (Union[SyncFuture, AsyncFuture]): Future to mark as failed.
            override_error_message (Union[str, Exception], optional): Custom
            error message or exception to set instead of the task's recorded error.

        Returns:
            None
        """
        if task_fut.done():
            logger.warning(
                f'Attempted to handle an already failed task "{task["uid"]}"'
            )
            return

        # Determine the appropriate exception to set
        if override_error_message is not None:
            # If it's already an exception (like DependencyFailureError),
            # use it directly
            if isinstance(override_error_message, Exception):
                exception = override_error_message
            else:
                # If it's a string, wrap it in RuntimeError
                exception = RuntimeError(str(override_error_message))
        else:
            # Use the task's original exception or stderr
            original_error = (
                task.get("exception")
                or task.get("stderr")
                or "failed with unknown error"
            )

            # Ensure we have an Exception object
            if isinstance(original_error, Exception):
                exception = original_error
            else:
                # If it's a string (stderr) or any other type, wrap it in RuntimeError
                exception = RuntimeError(str(original_error))

        task_fut.set_exception(exception)

    def handle_task_cancellation(self, task: dict, task_fut: asyncio.Future):
        """Handle task cancellation."""
        if task_fut.done():
            logger.warning(
                f'Attempted to handle an already cancelled task "{task["uid"]}"'
            )
            return

        # Restore original cancel method
        task_fut.cancel = task_fut.original_cancel
        return task_fut.cancel()

    @typeguard.typechecked
    def task_callbacks(
        self, task, state: str, service_callback: Optional[Callable] = None
    ):
        """Processes task state changes and invokes appropriate handlers.

        Handles state transitions for tasks, updates their futures, and triggers
        relevant state-specific handlers. Supports optional service-specific
        callbacks for extended functionality.

        Args:
            task (Union[dict, object]): Task object or dictionary containing task
            state information.
            state (str): New state of the task.
            service_callback (Optional[Callable], optional): Callback function
            for service tasks. Must be daemon-threaded to avoid blocking.
            Defaults to None.

        Returns:
            None

        State Transitions:
            - DONE: Calls handle_task_success
            - RUNNING: Marks future as running
            - CANCELED: Cancels the future
            - FAILED: Calls handle_task_failure

        Logging:
            - Debug: Non-relevant state received
            - Info: Task state changes
            - Warning: Unknown task received

        Example:
            ::

                def service_ready_callback(future, task, state):
                    def wait_and_set():
                        try:
                            # Synchronous operation
                            future.set_result(info)
                        except Exception as e:
                            future.set_exception(e)
                    threading.Thread(target=wait_and_set, daemon=True).start()
        """
        if (
            state not in self.task_states_map.terminal_states
            and state != self.task_states_map.RUNNING
        ):
            logger.debug(f"Non-relevant task state received: {state}. Skipping state.")
            return

        task_obj = task
        if isinstance(task, dict):
            task_dct = task
        else:
            task_dct = task.as_dict()

        if task_dct["uid"] not in self.components:
            logger.warning(
                f"Received an unknown task and will skip it: {task_dct['uid']}"
            )
            return

        task_fut = self.components[task_dct["uid"]]["future"]
        logger.info(f"{task_dct['uid']} is in {state} state")

        if service_callback:
            # service tasks are marked done by a backend specific
            # mechanism that are provided during the callbacks only
            service_callback(task_fut, task_obj, state)

        if state == self.task_states_map.DONE:
            self.handle_task_success(task_dct, task_fut)
        elif state == self.task_states_map.RUNNING:
            # NOTE: with asyncio future the running state is
            # implicit: when a coroutine that awaits the future
            # is scheduled and started by the event loop, that’s
            # when the “work” is running.
            pass
        elif state == self.task_states_map.CANCELED:
            self.handle_task_cancellation(task_dct, task_fut)
        elif state == self.task_states_map.FAILED:
            self.handle_task_failure(task_dct, task_fut)

    async def shutdown(self, skip_execution_backend: bool = False):
        """
        Internal implementation of asynchronous shutdown for the workflow manager.

        This method performs the following steps:
            1. Sets the shutdown event to signal components to exit
            2. Cancels background tasks responsible for running and
            submitting workflows.
            3. Waits for the cancellation and completion of these tasks,
            with a timeout of 5 seconds.
            4. Cancel workflow tasks.
            5. Logs a warning if the tasks do not complete within the timeout
            period.
            6. Shuts down the backend using an executor to avoid blocking the
            event loop.

        Args:
            skip_execution_backend (bool): If True, skips the shutdown of the
                execution backend.

        Returns:
            None

        Raises:
            asyncio.TimeoutError: If the background tasks do not complete
                within the timeout period.
            asyncio.CancelledError: If the shutdown is cancelled before
                completion.
        """
        logger.info("Initiating shutdown")
        # Signal components to exit
        self._shutdown_event.set()

        # cancel workflow futures (tasks and blocks)
        for comp in self.components.values():
            future = comp["future"]
            comp_desc = comp["description"]
            if not future.done():
                self.handle_task_cancellation(comp_desc, future)

        # Cancel internal components task
        if not self._run_task.done():
            logger.debug(f"Shutting down run component")
            self._run_task.cancel()

        # Wait for internal components shutdown to complete
        try:
            await asyncio.wait_for(self._run_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for internal components to shutdown")
        except asyncio.CancelledError:
            logger.warning("Internal components shutdown cancelled")

        # Shutdown execution backend
        if not skip_execution_backend and self.backend:
            await self.backend.shutdown()
            self._clear_internal_records()
            logger.debug("Shutting down execution backend completed")
        else:
            logger.warning("Skipping execution backend shutdown as requested")

        logger.info("Shutdown completed for all components.")

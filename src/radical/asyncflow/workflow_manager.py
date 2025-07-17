# flake8: noqa
import os
import asyncio
import threading
from contextlib import contextmanager
from collections import defaultdict, deque

from pathlib import Path
from typing import Callable, Optional, Union

import radical.utils as ru

from functools import wraps
from asyncio import Future as AsyncFuture
from concurrent.futures import Future as SyncFuture

import typeguard
from .data import InputFile, OutputFile

from .errors import DependencyFailure
from .backends.execution.noop import NoopExecutionBackend
from .backends.execution.base import BaseExecutionBackend

TASK = 'task'
BLOCK = 'block'
FUNCTION = 'function'
EXECUTABLE = 'executable'

class WorkflowEngine:
    """
    An asynchronous workflow manager that uses asyncio event loops
    and coroutines to manage and execute workflow components (blocks and/or
    tasks) within Directed Acyclic Graph (DAG) or Chain Graph (CG) structures.

    This class provides support for async/await operations and handles task
    dependencies, input/output data staging, and execution.

    Attributes:
        loop (asyncio.AbstractEventLoop): The asyncio event loop used for managing asynchronous tasks.
        backend (BaseExecutionBackend): The execution backend used for task execution.
        dry_run (bool): Indicates whether the engine is in dry-run mode.
        work_dir (str): The working directory for the workflow session.
        log (ru.Logger): Logger instance for logging workflow events.
        prof (ru.Profiler): Profiler instance for profiling workflow execution.
        jupyter_async (bool): Indicates whether the engine is running in Jupyter async mode.
    """

    @typeguard.typechecked
    def __init__(self, backend: Optional[BaseExecutionBackend] = None,
                 dry_run: bool = False, jupyter_async=None, implicit_data=True) -> None:

        self.loop = None
        self.running = []
        self.components = {}
        self.resolved = set()
        self.dependencies = {}
        self.backend = backend
        self.dry_run = dry_run
        self.unresolved = set()
        self.queue = asyncio.Queue()
        self.implicit_data_mode = implicit_data

        # Optimization: Track component state changes
        self._ready_queue = deque()
        self._dependents_map = defaultdict(set)  # Maps component -> components that depend on it
        self._dependency_count = {}  # Maps component -> number of unresolved dependencies
        self._component_change_event = asyncio.Event()

        self._setup_execution_backend()

        self.task_states_map = self.backend.get_task_states_map()

        # FIXME: session should always have a valid path
        self.work_dir = self.backend.session.path or os.getcwd()

        # always set the logger and profiler **before** setting the async loop
        self.log = ru.Logger(name='workflow_manager',
                             ns='radical.asyncflow', path=self.work_dir)
        self.prof = ru.Profiler(name='workflow_manager',
                                ns='radical.asyncflow', path=self.work_dir)

        self.backend.register_callback(self.task_callbacks)

        self.jupyter_async = jupyter_async if jupyter_async is not None else \
                             os.environ.get('FLOW_JUPYTER_ASYNC', None)

        self._set_loop() # detect and set the event-loop 
        self._start_async_internal_comps() # start the solver and submitter

        # Define specific decorators
        self.block = self._register_decorator(comp_type=BLOCK)
        self.function_task = self._register_decorator(comp_type=TASK, task_type=FUNCTION)
        self.executable_task = self._register_decorator(comp_type=TASK, task_type=EXECUTABLE)

    def _update_dependency_tracking(self, comp_uid):
        """Update dependency tracking structures for a component."""
        dependencies = self.dependencies[comp_uid]
        
        # Count unresolved dependencies
        unresolved_count = 0
        for dep in dependencies:
            dep_uid = dep['uid']
            if dep_uid not in self.resolved or not self.components[dep_uid]['future'].done():
                unresolved_count += 1
                # Track reverse dependencies
                self._dependents_map[dep_uid].add(comp_uid)
        
        self._dependency_count[comp_uid] = unresolved_count
        
        # If no dependencies, add to ready queue
        if unresolved_count == 0:
            self._ready_queue.append(comp_uid)

    def _setup_execution_backend(self):
        """Sets up and validates the execution backend for the workflow manager.

        This method configures the execution backend based on the dry run mode
        setting. If no backend is specified and dry run is enabled, it creates
        a NoopExecutionBackend. Otherwise, it validates that the provided
        backend is compatible with the current mode.

        Args:
            None

        Returns:
            None

        Raises:
            RuntimeError: If no execution backend is specified in normal mode
            RuntimeError: If an incompatible backend is specified in dry run mode
        """

        if self.backend is None:
            if self.dry_run:
                self.backend = NoopExecutionBackend()
            else:
                raise RuntimeError('An execution backend must be specified'
                                   ' when not in "dry_run" mode.')
        else:
            if self.dry_run and not isinstance(self.backend, NoopExecutionBackend):
                raise RuntimeError('Dry-run only supports the "NoopExecutionBackend".')

    def _is_in_jupyter(self):
        """Determines if the code is running within a Jupyter environment.

        This method checks for the presence of the 'JPY_PARENT_PID' environment
        variable, which is set when code is executed within a Jupyter notebook
        or Jupyter lab environment.

        Returns:
            bool: True if running in a Jupyter environment, False otherwise.
        """

        return "JPY_PARENT_PID" in os.environ

    def _set_loop(self):
        """Configures and sets the asyncio event loop for the current context.

        Determines the appropriate asyncio event loop based on the execution
        environment and handles both synchronous and asynchronous execution
        modes. The behavior varies depending on whether code is running in
        Jupyter, IPython, or standard Python environments.

        The event loop configuration follows these rules:
            - In Jupyter with jupyter_async=True: Reuses existing loop
            - In Jupyter with jupyter_async=False: Creates new loop
            - In IPython: Reuses existing loop
            - In standard Python: Creates new loop if none exists

        Args:
            None

        Returns:
            None

        Raises:
            ValueError: If in Jupyter and jupyter_async setting is not specified
            RuntimeError: If no event loop can be obtained or created

        Notes:
            The jupyter_async setting can be specified either through the
            constructor parameter or via the FLOW_JUPYTER_ASYNC environment
            variable.
        """

        try:
            # get current loop if running
            loop = asyncio.get_running_loop()

            if loop and self._is_in_jupyter():
                # We can not detect if the user wants to execute
                # **sync/async** function unless we are instructed to, so we fail.
                if self.jupyter_async is None:
                    exception_msg = ('Jupyter requires async/sync mode to be '
                                     ' set via the "jupyter_async" parameter or '
                                     'the "FLOW_JUPYTER_ASYNC" environment variable.')
                    raise ValueError(exception_msg)

                elif isinstance(self.jupyter_async, str):
                    self.jupyter_async = True if self.jupyter_async == 'TRUE' else False

                if self.jupyter_async:
                    # Jupyter async context and runs **async** functions
                    self.loop = loop
                    self.log.debug('Running within Async Jupyter and loop is found/re-used')
                else:
                    # Jupyter async context and runs **sync** functions
                    self.loop = asyncio.new_event_loop()
                    self.log.debug('Running within Sync Jupyter and new loop is created')
            else:
                # IPython async context and runs **async/sync** functions
                self.loop = loop
                self.log.debug('Running within IPython loop is found/re-used')

        except RuntimeError:
            # Python sync context and runs **async/sync** functions
            self.loop = asyncio.new_event_loop()    # create a new loop if none exists
            self.log.debug('No loop was found, new loop is created/set')

        if not self.loop:
            raise RuntimeError('Failed to obtain or create a new event-loop for unknown reason')

        asyncio.set_event_loop(self.loop)
        self.log.debug('Event-Loop is set successfully')

    def _start_async_internal_comps(self):
        """Starts asynchronous internal components of the workflow manager.

        Initializes and starts the workflow manager's internal coroutine tasks
        (submit and run) in both synchronous and asynchronous execution
        contexts.

        The method handles two scenarios:
            - Async context: Creates tasks directly using asyncio.create_task
            - Sync context: Creates a background thread to run the event loop
                            and schedule tasks

        Args:
            None

        Returns:
            None

        Attributes Modified:
            _submit_task: Created coroutine task for handling submissions
            _run_task: Created coroutine task for managing workflow execution

        Notes:
            In synchronous contexts, a daemon thread is created to run the event
            loop in the background. This ensures the workflow can execute
            without blocking the main thread.
        """

        def _start():
            # Sync context: run loop in background thread
            self._run_task = self.loop.create_task(self.run())
            self._submit_task = self.loop.create_task(self.submit())

            if not self.loop.is_running():
                self.loop.run_forever()

        if self.loop.is_running():
            # Async context
            self._run_task = asyncio.create_task(self.run())
            self._submit_task = asyncio.create_task(self.submit())
        else:
            # Sync context
            thread = threading.Thread(target=_start, daemon=True)
            thread.start()

    def _register_decorator(self, comp_type: str, task_type: Optional[str] = None):
        """Creates a decorator factory for registering workflow components.

        This method generates decorators that handle registration of tasks and
        blocks with optional task-specific descriptions. The generated
        decorators support both definition-time and invocation-time task
        descriptions.

        Args:
            comp_type (str): Type of workflow component (e.g., 'task', 'stage')
            task_type (Optional[str], optional): Specific task type. Defaults to None

        Returns:
            Callable: A decorator factory that produces decorators for registering
            workflow components

        The decorator handles:
            - Capturing task descriptions from default arguments at definition time
            - Processing task descriptions from keyword arguments at invocation time
            - Merging descriptions with invocation-time values taking precedence
            - Registering components using internal registration methods

        Example:
            >>> @engine.function_task(task_description={'cores': 4})
            ... def my_task():
            ...     pass

            >>> my_task(task_description={'memory': '2GB'})  # Merges descriptions
        """

        def outer(possible_func: Union[Callable, None] = None, service: bool = False):
            def actual_decorator(func: Callable) -> Callable:
                # Capture definition-time task_description from default args
                task_description_def = func.__defaults__[0] if func.__defaults__ else {}
                setattr(func, '__task_description__', task_description_def)

                @wraps(func)
                def wrapped(*args, **kwargs):
                    task_description_call = kwargs.pop("task_description", {}) or {}

                    task_description_final = {
                        **getattr(func, '__task_description__', {}),
                        **task_description_call}

                    registered_func = self._handle_flow_component_registration(
                        func,
                        is_service=service,
                        comp_type=comp_type,
                        task_type=task_type,
                        task_backend_specific_kwargs=task_description_final)

                    return registered_func(*args, **kwargs)

                return wrapped

            # If used as @decorator
            if callable(possible_func):
                return actual_decorator(possible_func)

            # If used as @decorator(...)
            return actual_decorator

        return outer

    def _handle_flow_component_registration(self,
                                            func: Callable,
                                            is_service:bool,
                                            comp_type: str,
                                            task_type: str,
                                            task_backend_specific_kwargs: dict = None):
        """Handles registration of tasks and blocks as workflow components.

        Creates a decorator that manages component registration, handling both
        synchronous and asynchronous functions. The decorator creates
        appropriate futures (SyncFuture or AsyncFuture) to track execution
        state.

        Args:
            func (Callable): Function to be registered as a workflow component
            is_service (bool): Whether the component is a service
            comp_type (str): Component type (e.g., "task", "block")
            task_type (str): Task type, determines result handling
            task_backend_specific_kwargs (dict, optional): Backend-specific kwargs. Defaults to None.

        Returns:
            Callable: A decorator that:
                - Wraps the original function
                - Registers it as a workflow component
                - Returns a future tracking the component's execution

        Note:
            For executable tasks, the decorator handles awaiting async functions
            and collecting their return values appropriately.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            is_async = asyncio.iscoroutinefunction(func)

            comp_desc = {}
            comp_desc['args'] = args
            comp_desc['function'] = func
            comp_desc['kwargs'] = kwargs
            comp_desc['is_service'] = is_service
            comp_desc['task_backend_specific_kwargs'] = task_backend_specific_kwargs

            if is_async:
                comp_fut = AsyncFuture()
                async def async_wrapper():
                    # get the executable from the function call using await
                    comp_desc[EXECUTABLE] = await func(*args, **kwargs) if task_type == EXECUTABLE else None
                    return self._register_component(comp_fut, comp_type, comp_desc, task_type)
                asyncio.create_task(async_wrapper())
                return comp_fut
            else:
                comp_fut = SyncFuture()
                # get the executable from the function call
                comp_desc[EXECUTABLE] = func(*args, **kwargs) if task_type == EXECUTABLE else None
                self._register_component(comp_fut, comp_type, comp_desc, task_type)
                return comp_fut

        return wrapper

    def _register_component(self, comp_fut, comp_type: str,
                            comp_desc: dict, task_type: str = None):
        """Registers a workflow component with shared registration logic.

        Handles the core registration process for both tasks and blocks.
        Assigns identifiers, sets up metadata, detects dependencies, and manages
        component storage.

        Args:
            comp_fut (Union[AsyncFuture, SyncFuture]): Future object for tracking component execution
            comp_type (str): Type of component ('task' or 'block')
            comp_desc (dict): Component description containing:
                - function: The callable to execute
                - args: Function arguments
                - kwargs: Function keyword arguments
                - task_backend_specific_kwargs: Backend-specific parameters
            task_type (str, optional): Specific task type for execution handling. Defaults to None.

        Returns:
            Union[AsyncFuture, SyncFuture]: The future object with:
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
        comp_desc['name'] = comp_desc['function'].__name__
        comp_desc['uid'] = self._assign_uid(prefix=comp_type)

        comp_desc[FUNCTION] = None if task_type == EXECUTABLE else comp_desc[FUNCTION]
        
        if comp_desc[EXECUTABLE] and not isinstance(comp_desc[EXECUTABLE], str):
            error_msg = f"Executable task must return a string, got {type(comp_desc[EXECUTABLE])}"
            raise ValueError(error_msg)

        comp_deps, input_files_deps, output_files_deps = self._detect_dependencies(comp_desc['args'])

        comp_desc['metadata'] = {'dependencies': comp_deps,
                                 'input_files' : input_files_deps,
                                 'output_files': output_files_deps}

        comp_fut.id = comp_desc['uid'].split(f'{comp_type}.')[1]

        setattr(comp_fut, comp_type, comp_desc)

        # prepare the task package that will be sent to the backend
        self.components[comp_desc['uid']] = {'type': comp_type,
                                             'future': comp_fut,
                                             'description': comp_desc}

        self.dependencies[comp_desc['uid']] = comp_deps

        self.log.debug(f"Registered {comp_type}: '{comp_desc['name']}' with id of {comp_desc['uid']}")

        self._update_dependency_tracking(comp_desc['uid'])
        self._component_change_event.set()

        return comp_fut

    @staticmethod
    def shutdown_on_failure(func: Callable):
        """Decorator that ensures backend shutdown on function failure.

        Wraps a function to catch any exceptions, shut down the execution
        backend, and re-raise the original exception. This ensures cleanup of
        backend resources even when errors occur.

        Args:
            func (Callable): Function to be decorated

        Returns:
            Callable: Wrapped function that handles exceptions by shutting
                down the backend before re-raising

        Raises:
            Exception: Re-raises any exception caught from the wrapped function
                after shutting down the backend

        Note:
            The wrapped function must be an instance method with access to
            self.backend and self.log
        """

        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                self.log.exception('Internal failure is detected, shutting down the execution backend')
                self.backend.shutdown()  # Call shutdown on exception
                raise e
        return wrapper

    def _assign_uid(self, prefix):
        """
        Generates a unique identifier (UID) for a flow component.

        This method generates a custom flow component UID based on the format
        `task.%(item_counter)06d` and assigns it a session-specific namespace
        using the backend session UID.

        Returns:
            str: The generated unique identifier for the flow component.
        """

        uid = ru.generate_id(prefix, ru.ID_SIMPLE)

        return uid


    def _detect_dependencies(self, possible_dependencies):
        """
        Detects and categorizes possible dependencies into blocks/tasks, input files,
        and output files.

        This method iterates over a list of possible dependencies and classifies
        them into three categories:
        - Blocks/Tasks that are instances of `Future` with a `task` or `block` attribute
          (task dependencies).
        - Input files that are instances of `InputFile` (files required by the task only).
        - Output files that are instances of `OutputFile` (files produced by the task only).

        Args:
            possible_dependencies (list): A list of possible dependencies, which can include
            blocks, tasks, input files, and output files.

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
            # it is a flow component deps
            if isinstance(possible_dep, SyncFuture) or \
                isinstance(possible_dep, AsyncFuture):
                if hasattr(possible_dep, TASK):
                    possible_dep = possible_dep.task
                elif hasattr(possible_dep, BLOCK):
                    possible_dep = possible_dep.block
                dependencies.append(possible_dep)
            # it is input file needs to be obtained from somewhere
            elif isinstance(possible_dep, InputFile):
                input_files.append(possible_dep.filename)
            # it is output file needs to be obtained from the task folder
            elif isinstance(possible_dep, OutputFile):
                output_files.append(possible_dep.filename)

        return dependencies, input_files, output_files

    def _clear(self):
        """
        clear workflow component and their deps
        """
        self.components.clear()
        self.dependencies.clear()
        self._ready_queue.clear()
        self._dependents_map.clear()
        self._dependency_count.clear()

    def _notify_dependents(self, comp_uid):
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

    def _create_dependency_failure_exception(self, comp_desc, failed_deps):
        """
        Create a DependencyFailure exception that shows both the immediate failure
        and the root cause from failed dependencies.
        
        Args:
            comp_desc (dict): Description of the component that cannot execute
            failed_deps (list): List of exceptions from failed dependencies
            
        Returns:
            DependencyFailure: Exception with detailed failure information
        """
        # Get the first failed dependency's exception as root cause
        root_exception = failed_deps[0]

        # Create a descriptive error message
        error_message = f"Cannot execute '{comp_desc['name']}' due to dependency failure"

        # Get the names of the failed dependencies for better context
        failed_dep_names = []
        dependencies = self.dependencies[comp_desc['uid']]
        dep_futures = [self.components[dep['uid']]['future'] for dep in dependencies]

        for dep, dep_future in zip(dependencies, dep_futures):
            if dep_future.exception() is not None:
                failed_dep_names.append(dep['name'])

        # Create the DependencyFailure exception with all context
        return DependencyFailure(
            message=error_message,
            failed_dependencies=failed_dep_names,
            root_cause=root_exception
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
            dep_desc = self.components[dep['uid']]['description']
            for output_file in dep_desc['metadata']['output_files']:
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
            5. Submits ready components to execution queue

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
            - Runs indefinitely until cancelled
            - Uses sleep intervals to prevent busy-waiting
            - Handles both implicit and explicit data dependencies
        """

        while True:
            try:
                # Process ready components first
                to_submit = []

                while self._ready_queue:
                    comp_uid = self._ready_queue.popleft()

                    # Skip if already processed
                    if comp_uid in self.resolved or comp_uid in self.running:
                        continue

                    # Check if future is already done (could be cancelled/failed)
                    if self.components[comp_uid]['future'].done():
                        self.resolved.add(comp_uid)
                        self._notify_dependents(comp_uid)
                        continue

                    # Verify dependencies are still met
                    dependencies = self.dependencies[comp_uid]
                    dep_futures = [self.components[dep['uid']]['future'] for dep in dependencies]
                    failed_deps = [fut.exception() for fut in dep_futures if fut.exception() is not None]

                    if failed_deps:
                        comp_desc = self.components[comp_uid]['description']
                        
                        # Create a comprehensive chained exception
                        chained_exception = self._create_dependency_failure_exception(comp_desc, failed_deps)
                        
                        self.log.error(f"Dependency failure for {comp_desc['name']}: {chained_exception}")

                        # Fail this component with the chained exception
                        self.handle_task_failure(comp_desc, self.components[comp_uid]['future'], chained_exception)

                        self.resolved.add(comp_uid)
                        self._notify_dependents(comp_uid)
                        continue

                    # Prepare component for submission
                    comp_desc = self.components[comp_uid]['description']

                    # Handle data dependencies for tasks
                    if self.components[comp_uid]['type'] == TASK:
                        explicit_files_to_stage = []

                        for dep in dependencies:
                            dep_desc = self.components[dep['uid']]['description']

                            # Link implicit data dependencies
                            if self.implicit_data_mode and not dep_desc['metadata'].get('output_files'):
                                self.log.debug(f'Linking implicit file(s): from {dep_desc["name"]} to {comp_desc["name"]}')
                                self.backend.link_implicit_data_deps(dep_desc, comp_desc)

                            # Link explicit data dependencies
                            for output_file in dep_desc['metadata']['output_files']:
                                if output_file in comp_desc['metadata']['input_files']:
                                    self.log.debug(f'Linking explicit file ({output_file}) from {dep_desc["name"]} to {comp_desc["name"]}')
                                    data_dep = self.backend.link_explicit_data_deps(
                                        src_task=dep_desc,
                                        dst_task=comp_desc,
                                        file_name=output_file
                                    )
                                    explicit_files_to_stage.append(data_dep)

                        # Input staging data dependencies
                        # Get all output files from dependencies to avoid staging files that are already linked
                        dependency_output_files = self._get_dependency_output_files(dependencies)
                        staged_targets = {Path(item['target']).name for item in explicit_files_to_stage}
                        
                        for input_file in comp_desc['metadata']['input_files']:
                            input_basename = Path(input_file).name
                            # Only stage if the file is not already staged AND not an output from a dependency
                            if input_basename not in staged_targets and input_basename not in dependency_output_files:
                                self.log.debug(f'Staging {input_file} to {comp_desc["name"]} work dir')
                                data_dep = self.backend.link_explicit_data_deps(
                                    src_task=None,
                                    dst_task=comp_desc,
                                    file_name=input_basename,
                                    file_path=input_file
                                )
                                explicit_files_to_stage.append(data_dep)

                    to_submit.append(comp_desc)
                    msg = f"Ready to submit: {comp_desc['name']}"
                    msg += f" with resolved dependencies: {[dep['name'] for dep in dependencies]}"
                    self.log.debug(msg)

                # Submit ready components
                if to_submit:
                    await self.queue.put(to_submit)
                    for comp_desc in to_submit:
                        comp_uid = comp_desc['uid']
                        self.running.append(comp_uid)
                        self.resolved.add(comp_uid)

                # Check for completed components and update dependency tracking
                completed_components = []
                for comp_uid in list(self.running):
                    if self.components[comp_uid]['future'].done():
                        completed_components.append(comp_uid)
                        self.running.remove(comp_uid)

                # Notify dependents of completed components
                for comp_uid in completed_components:
                    self._notify_dependents(comp_uid)

                # Signal that something changed
                if completed_components:
                    self._component_change_event.set()

                # If nothing is ready and nothing is running, wait for changes
                if not self._ready_queue and not to_submit and not completed_components:
                    # Wait for new components or state changes, with a timeout
                    try:
                        await asyncio.wait_for(self._component_change_event.wait(), timeout=1.0)
                        self._component_change_event.clear()
                    except asyncio.TimeoutError:
                        # Timeout is fine, just continue the loop
                        pass
                else:
                    # Small delay to prevent tight loop when actively processing
                    await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.exception(f"Error in run loop: {e}")
                await asyncio.sleep(0.1)

    async def submit(self):
        """Manages asynchronous submission of tasks and blocks to the execution backend.

        Continuously monitors the internal queue, retrieves ready tasks and blocks,
        and submits them for execution. Separates incoming objects into tasks and
        blocks based on their UID pattern, and dispatches each to the appropriate
        backend method.

        Submission Process:
            1. Waits for a batch of objects from the queue
            2. Filters objects into tasks and blocks
            3. Submits tasks via `backend.submit_tasks`
            4. Submits blocks via `_submit_blocks` asynchronously
            5. Retries on timeout with a short delay

        Args:
            None

        Returns:
            None

        Raises:
            Exception: If an unexpected error occurs during submission

        Queue Management:
            - queue (asyncio.Queue): Holds lists of objects ready for execution
            - Each queue item is a list of dicts representing tasks and/or blocks
            - Tasks and blocks are identified by `uid` field content

        Note:
            - Runs indefinitely until cancelled or stopped
            - Uses a 1-second timeout to avoid blocking indefinitely
            - Handles `asyncio.TimeoutError` gracefully with a sleep interval
        """

        while True:
            try:
                objects = await asyncio.wait_for(self.queue.get(), timeout=1)

                # pass the resolved tasks to the backend
                tasks = [t for t in objects if t and BLOCK not in t['uid']]
                blocks = [b for b in objects if b and TASK not in b['uid']]

                self.log.debug(f'Submitting {[b["name"] for b in objects]} for execution')

                if tasks:
                    self.backend.submit_tasks(tasks)
                if blocks:
                    await self._submit_blocks(blocks)

            except asyncio.TimeoutError:
                await asyncio.sleep(0.5)
            except Exception as e:
                self.log.exception(f"Error in submit: {e}")
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
            args = block['args']
            kwargs = block['kwargs']
            func = block['function']
            block_fut = self.components[block['uid']]['future']

            # Execute the block function as a coroutine
            asyncio.create_task(self.execute_block(block_fut, func, *args, **kwargs))

    async def execute_block(self, block_fut, func, *args, **kwargs):
        """Executes a block function and sets its result on the associated future.

        Calls the given function with provided arguments, awaiting it if it's a coroutine,
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
                # If function is not async, run it in executor
                result = await self.loop.run_in_executor(None, func, *args, **kwargs)

            if not block_fut.done():
                block_fut.set_result(result)
        except Exception as e:
            if not block_fut.done():
                block_fut.set_exception(e)

    def handle_task_success(self, task, task_fut):
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
        internal_task = self.components[task['uid']]['description']

        if not task_fut.done():
            if internal_task[FUNCTION]:
                task_fut.set_result(task['return_value'])
            else:
                task_fut.set_result(task['stdout'])
        else:
            self.log.warning(f'Attempted to handle an already resolved task "{task["uid"]}"')

    def handle_task_failure(self, task: dict, task_fut: Union[SyncFuture, AsyncFuture], 
                            override_error_message: Union[str, Exception] = None) -> None:
        """Handles task failure and sets the appropriate exception on the future.

        Marks the given task's future as failed by setting an exception derived from
        either a provided override error or the task's own recorded error/stderr. Logs
        a warning if the future is already resolved.

        Args:
            task (dict): Dictionary with task details, including:
                - 'uid' (str): Unique task identifier
                - 'exception' or 'stderr': Error information from execution
            task_fut (Union[SyncFuture, AsyncFuture]): Future to mark as failed.
            override_error_message (Union[str, Exception], optional): Custom error message
                or exception to set instead of the task's recorded error.

        Returns:
            None
        """
        if task_fut.done():
            self.log.warning(f'Attempted to handle an already resolved task "{task["uid"]}"')

        internal_task = self.components[task['uid']]['description']

        # Determine the appropriate exception to set
        if override_error_message is not None:
            # If it's already an exception (like DependencyFailure), use it directly
            if isinstance(override_error_message, Exception):
                exception = override_error_message
            else:
                # If it's a string, wrap it in RuntimeError
                exception = RuntimeError(str(override_error_message))
        else:
            # Use the task's original exception or stderr
            original_error = task['exception'] if internal_task.get(FUNCTION) else task['stderr']

            # Ensure we have an Exception object
            if isinstance(original_error, Exception):
                exception = original_error
            else:
                # If it's a string (stderr) or any other type, wrap it in RuntimeError
                exception = RuntimeError(str(original_error))

        task_fut.set_exception(exception)

    @typeguard.typechecked
    def task_callbacks(self, task, state: str,
                    service_callback: Optional[Callable] = None):
        """Processes task state changes and invokes appropriate handlers.

        Handles state transitions for tasks, updates their futures, and triggers
        relevant state-specific handlers. Supports optional service-specific
        callbacks for extended functionality.

        Args:
            task (Union[dict, object]): Task object or dictionary containing task state information.
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
        if state not in self.task_states_map.terminal_states and \
            state != self.task_states_map.RUNNING:
            self.log.debug(f"Non-relevant task state received: {state}. Skipping state.")
            return

        task_obj = task

        if isinstance(task, dict):
            task_dct = task
        else:
            task_dct = task.as_dict()


        if task_dct['uid'] not in self.components:
            self.log.warning(f'Received an unknown task and will skip it: {task_dct["uid"]}')
            return

        task_fut = self.components[task_dct['uid']]['future']

        self.log.info(f'{task_dct["uid"]} is in {state} state')

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
            if isinstance(task_fut, SyncFuture):
                task_fut.set_running_or_notify_cancel()

        elif state == self.task_states_map.CANCELED:
            task_fut.cancel()

        elif state == self.task_states_map.FAILED:
            self.handle_task_failure(task_dct, task_fut)

    async def _async_shutdown_internal(self, skip_execution_backend):
        """
        Internal implementation of asynchronous shutdown for the workflow manager.

        This method performs the following steps:
            1. Cancels background tasks responsible for running and
            submitting workflows.
            2. Waits for the cancellation and completion of these tasks,
            with a timeout of 5 seconds.
            3. Logs a warning if the tasks do not complete within the timeout
            period.
            4. Shuts down the backend using an executor to avoid blocking the
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
        internal_component_to_shutdown = [t for t in (self._run_task, self._submit_task) if t]

        # Cancel background tasks
        for internal_component in internal_component_to_shutdown:
            if internal_component and not internal_component.done():
                internal_comp_name = internal_component.get_coro().__name__
                self.log.debug(f"Shutting down {internal_comp_name} component")
                internal_component.cancel()

        # Wait for tasks to complete
        try:
            await asyncio.wait_for(asyncio.gather(*internal_component_to_shutdown,
                                                  return_exceptions=True), timeout=5.0)

        except asyncio.TimeoutError:
            self.log.warning("Timeout waiting for tasks to shutdown")
        except asyncio.CancelledError:
            self.log.warning("Shutdown cancelled")

        # Shutdown the execution backend
        if not skip_execution_backend and self.backend:
            await self.loop.run_in_executor(None, self.backend.shutdown)
            self.log.debug(f"Shutting down execution backend")
        else:
            self.log.warning("Skipping execution backend shutdown as requested")

    def shutdown(self, skip_execution_backend: bool = False):
        """
        Shuts down the workflow manager in a universal way, handling different
        execution environments:

        - In Jupyter Notebook (sync or async mode), it either returns the
        coroutine for async mode or runs it in a thread for sync mode.
        - Outside Jupyter, it detects if running in an async context and 
        returns the coroutine, or runs it synchronously if not.
        - Ensures proper shutdown regardless of whether the environment is
        synchronous or asynchronous, and whether it's running in Jupyter
        or standard Python.

        Args:
            skip_execution_backend (bool): If True, skips the shutdown of the
                execution backend. This is useful for cases where the backend
                should not be shut down, such as in testing or when the backend
                is managed externally.

        Returns:
            Union[Coroutine, Any]: The result of the asynchronous shutdown operation, 
                either as a coroutine (for async contexts) or the actual result 
                (for sync contexts).

        Modes:
            - Regular sync Python
            - Jupyter sync mode
            - Jupyter async mode
            - Regular async Python
        """
        # Case 1: We're in Jupyter
        if self._is_in_jupyter():
            if self.jupyter_async:
                # Jupyter async mode - return the coroutine
                return self._async_shutdown_internal(skip_execution_backend)
            else:
                # Jupyter sync mode - run in thread
                future = asyncio.run_coroutine_threadsafe(
                    self._async_shutdown_internal(skip_execution_backend),
                    self.loop
                )
                return future.result()


        # Case 2: Not in Jupyter - detect async context
        try:
            asyncio.get_running_loop()
            return self._async_shutdown_internal(skip_execution_backend)
        except RuntimeError:
            future = asyncio.run_coroutine_threadsafe(
                self._async_shutdown_internal(skip_execution_backend),
                self.loop
            )
            return future.result()

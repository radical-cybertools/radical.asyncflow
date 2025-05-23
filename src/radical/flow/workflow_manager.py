# flake8: noqa
import os
import asyncio
import logging
import threading
from typing import Callable, Dict, Optional

import radical.utils as ru

from functools import wraps
from asyncio import Future as AsyncFuture
from concurrent.futures import Future as SyncFuture

import typeguard
from .data import InputFile, OutputFile

from radical.flow.backends.execution.noop import NoopExecutionBackend
from radical.flow.backends.execution.base import BaseExecutionBackend

TASK = 'task'
BLOCK = 'block'
FUNCTION = 'function'
EXECUTABLE = 'executable'

class WorkflowEngine:
    """
    WorkflowEngine is an asynchronous workflow manager that uses asyncio event loops 
    and coroutines to manage and execute workflow components (blocks and/or tasks) 
    within Directed Acyclic Graph (DAG) or Chain Graph (CG) structures. It provides 
    support for async/await operations and handles task dependencies, input/output 
    data staging, and execution.

        loop (asyncio.AbstractEventLoop): The asyncio event loop used for managing asynchronous tasks.
        backend (BaseExecutionBackend): The execution backend used for task execution.
        dry_run (bool): Indicates whether the engine is in dry-run mode.
        work_dir (str): The working directory for the workflow session.
        log (ru.Logger): Logger instance for logging workflow events.
        prof (ru.Profiler): Profiler instance for profiling workflow execution.
        jupyter_async (bool): Indicates whether the engine is running in Jupyter async mode.
    Methods:
        __init__(backend, dry_run, jupyter_async):
            Initializes the WorkflowEngine with the specified backend, dry-run mode, and Jupyter async mode.
        _setup_execution_backend():
            Configures the execution backend based on the provided backend and dry-run mode.
        _is_in_jupyter():
            Checks if the engine is running in a Jupyter environment.
        _set_loop():
            Configures and sets the asyncio event loop for the current context.
        _start_async_tasks():
            Starts asynchronous tasks for the workflow engine.
        _register_decorator(comp_type, task_type=None):
            Creates a decorator for registering tasks or blocks.
        _handle_flow_component_registration(func, comp_type, task_type, task_backend_specific_kwargs):
            Handles the registration of tasks or blocks as flow components.
        _register_component(comp_fut, comp_type, comp_desc, task_type=None, task_backend_specific_kwargs=None):
            Registers a task or block as a flow component.
        shutdown_on_failure(func):
            Decorator that shuts down the execution backend if an exception occurs in the decorated function.
        _assign_uid(prefix):
        _detect_dependencies(possible_dependencies):
            Detects and categorizes dependencies into tasks, input files, and output files.
        _clear():
            Clears workflow components and their dependencies.
        run():
            Async method to manage the execution of workflow components by resolving dependencies and submitting them for execution.
        submit():
            Async method to submit blocks or tasks from the queue for execution.
        _submit_blocks(blocks):
            Async method to submit blocks for execution.
        execute_block(block_fut, func, *args, **kwargs):
            Async method to execute a block function and update its asyncio future.
        task_callbacks(task, state):
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
        self.implicit_data = implicit_data

        self._setup_execution_backend()

        self.task_states_map = self.backend.get_task_states_map()

        # FIXME: session should always have a valid path
        self.work_dir = self.backend.session.path or os.getcwd()

        # always set the logger and profiler **before** setting the async loop
        self.log = ru.Logger(name='workflow_manager',
                             ns='radical.flow', path=self.work_dir)
        self.prof = ru.Profiler(name='workflow_manager',
                                ns='radical.flow', path=self.work_dir)

        self.backend.register_callback(self.task_callbacks)

        self.jupyter_async = jupyter_async if jupyter_async is not None else \
                             os.environ.get('FLOW_JUPYTER_ASYNC', None)

        self._set_loop() # detect and set the event-loop 
        self._start_async_tasks() # start the solver and submitter

    def _setup_execution_backend(self):
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
        return "JPY_PARENT_PID" in os.environ

    def _set_loop(self):
        """
        Configure and set the asyncio event loop for the current context.

        This method determines the appropriate asyncio event loop to use based on
        the execution environment (e.g., Jupyter, IPython, or standard Python).
        It handles both synchronous and asynchronous execution modes and ensures
        that a valid event loop is set.

        Raises:
            ValueError: If running in a Jupyter environment and the `jupyter_async`
                        parameter or the `FLOW_JUPYTER_ASYNC` environment variable
                        is not set.
            RuntimeError: If no event loop could be obtained or created.

        Notes:
            - In Jupyter, the behavior depends on the `jupyter_async` parameter:
              - If `True`, the existing loop is reused for asynchronous execution.
              - If `False`, a new loop is created for synchronous execution.
            - In IPython, the existing loop is reused.
            - In standard Python, a new loop is created if none exists.
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

    def _start_async_tasks(self):
        """Starts async tasks in both sync and async contexts."""
        def _start():
            self.loop.create_task(self.submit())  # let's schedule submit() in loop
            self.loop.create_task(self.run())     # let's schedule run() in loop
            if not self.loop.is_running():
                self.loop.run_forever()  # keep the event loop alive

        # case1: called from an async namespace
        if self.loop.is_running():
            asyncio.create_task(self.submit())
            asyncio.create_task(self.run())
        else:
            # case2: called from a sync namespace
            thread = threading.Thread(target=_start, daemon=True)
            thread.start()

    @staticmethod
    def _register_decorator(comp_type: str, task_type: str = None):
        """
        A decorator factory for registering workflow components with optional task descriptions.

        Args:
            comp_type (str): The type of the workflow component (e.g., 'task', 'stage', etc.).
            task_type (str, optional): The specific type of task, if applicable. Defaults to None.

        Returns:
            Callable: A decorator that wraps the target function, capturing and merging
            task descriptions provided at definition and invocation time, and registers
            the function as a workflow component using the manager's registration logic.

        The decorator:
            - Captures a `task_description` from the function's default arguments at
              definition time.
            - Allows overriding or extending the `task_description` at invocation time
              via keyword arguments.
            - Merges both descriptions, with invocation-time values taking precedence.
            - Registers the function as a workflow component using the manager's internal
              registration method.
        """
        def decorator(self, func: Callable) -> Callable:
            # Capture and store definition-time `task_description` if available
            task_description_def = func.__defaults__[0] if func.__defaults__ else {}
            setattr(func, '__task_description__', task_description_def)

            @wraps(func)
            def wrapped(*args, **kwargs):
                # Invocation-time `task_description` (if provided)
                task_description_call = kwargs.pop("task_description", {}) or {}

                # Merge with definition-time `task_description`
                task_description_final = {
                    **getattr(func, '__task_description__', {}),
                    **task_description_call
                }

                registered_func = self._handle_flow_component_registration(
                    func,
                    comp_type=comp_type,
                    task_type=task_type,
                    task_backend_specific_kwargs=task_description_final
                )
                return registered_func(*args, **kwargs)
            return wrapped
        return decorator

    # Define specific decorators
    block = _register_decorator(comp_type=BLOCK)
    function_task = _register_decorator(comp_type=TASK, task_type=FUNCTION)
    executable_task = _register_decorator(comp_type=TASK, task_type=EXECUTABLE)

    def _handle_flow_component_registration(self,
                                            func: Callable,
                                            comp_type: str,
                                            task_type: str,
                                            task_backend_specific_kwargs: dict = None):
        """
        Universal decorator logic for registering both tasks and blocks as flow components.

        This method returns a decorator that wraps the given function (`func`) and handles
        its registration as a flow component. It supports both synchronous and asynchronous
        functions, and manages the creation of appropriate future objects (`SyncFuture` or
        `AsyncFuture`) for tracking the component's execution and registration.

        Args:
            func (Callable): The function to be registered as a flow component.
            comp_type (str): The type of the component (e.g., "task", "block").
            task_type (str): The type of the task, used to determine how to handle the
            function's result.
            task_backend_specific_kwargs (dict, optional): Additional backend-specific
            keyword arguments for the task.

        Returns:
            Callable: A decorator that wraps the original function, registers it as a
            flow component, and returns a future object representing the component's
            execution.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            is_async = asyncio.iscoroutinefunction(func)

            comp_desc = {}
            comp_desc['args'] = args
            comp_desc['function'] = func
            comp_desc['kwargs'] = kwargs
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
        """
        Register a workflow component (task or block) with shared logic.
        This method assigns a unique identifier to the component, sets up its metadata,
        detects dependencies, and stores the component's future and description for later use.
        Args:
            comp_fut: The future object associated with the component.
            comp_type (str): The type of the component (e.g., 'task', 'block').
            comp_desc (dict): The component's description, including function, arguments, etc.
            task_type (str, optional): The type of task, used to distinguish between function and executable tasks.
        Raises:
            ValueError: If an executable task does not return a string.
        Returns:
            The updated future object with assigned id and component description.
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

        return comp_fut

    @staticmethod
    def shutdown_on_failure(func: Callable):
        """
        Decorator that calls `shutdown` if an exception occurs in the decorated function.
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

    async def run(self):
        """
        Async method to manage the execution of workflow components by resolving
        dependencies and submitting then for execution once they are resolved.

        This method continuously checks for unresolved components, evaluates their
        dependencies, and prepares them for submission if all dependencies are resolved.
        It also handles input and output data staging for the components.

        Workflow:
        - Identifies unresolved components.
        - Checks if their dependencies are resolved and their associated tasks are completed.
        - Prepares components for execution by setting up pre-execution commands and
          input staging based on dependencies.
        - Submits components that are ready for execution to the queue.

        Attributes:
            unresolved (set): A set of component UIDs that have unresolved dependencies.
            resolved (set): A set of component UIDs whose dependencies are resolved.
            running (list): A list of component UIDs that are currently running.
            dependencies (dict): A mapping of component UIDs to their dependency information.
            components (dict): A mapping of component UIDs to their descriptions and futures.
            queue (asyncio.Queue): A queue to submit components ready for execution.

        Raises:
            asyncio.CancelledError: If the coroutine is cancelled during execution.

        Notes:
            - This method runs indefinitely until cancelled.
            - It uses a sleep interval to avoid busy-waiting.
        """

        while True:
            self.unresolved = set(self.dependencies.keys())

            if not self.unresolved:
                await asyncio.sleep(0.1)
                continue

            to_submit = []

            for comp_uid in list(self.unresolved):
                if self.components[comp_uid]['future'].done():
                    self.resolved.add(comp_uid)
                    self.unresolved.remove(comp_uid)
                    continue

                if comp_uid in self.running:
                    if self.components[comp_uid]['future'].done():
                        self.running.remove(comp_uid)
                    else:
                        continue

                dependencies = self.dependencies[comp_uid]
                if all(dep['uid'] in self.resolved and self.components[dep['uid']]['future'].done() for dep in dependencies):
                    comp_desc = self.components[comp_uid]['description']

                    explicit_files_to_stage = []

                    # NOTE: We do not link or manage implicit or explicit
                    # data dependencies if the component is a block.
                    # TODO: We should consider this in the future.
                    if self.components[comp_uid]['type'] == TASK:

                        for dep in dependencies:
                            dep_desc = self.components[dep['uid']]['description']

                            # link implicit data dependencies
                            if self.implicit_data and not dep_desc['metadata'].get('output_files'):
                                self.log.debug(f'Linking implicit file(s): from {dep_desc['name']} to {comp_desc["name"]}')
                                self.backend.link_implicit_data_deps(dep_desc, comp_desc)

                            # link explicit data dependencies
                            for output_file in dep_desc['metadata']['output_files']:
                                if output_file in comp_desc['metadata']['input_files']:
                                    self.log.debug(f'Linking explicit file ({output_file}) from {dep_desc['name']} to {comp_desc["name"]}')
                                    data_dep = self.backend.link_explicit_data_deps(src_task=dep_desc,
                                                                                    dst_task=comp_desc,
                                                                                    file_name=output_file)
                                    explicit_files_to_stage.append(data_dep)

                        # input staging data dependencies
                        staged_targets = [item['target'].split('/')[-1] for item in explicit_files_to_stage]
                        for input_file in comp_desc['metadata']['input_files']:
                            input_basename = input_file.split('/')[-1]
                            if input_basename not in staged_targets:
                                self.log.debug(f'Staging input file: {input_file} to {comp_desc["name"]}')
                                data_dep = self.backend.link_explicit_data_deps(src_task=None,
                                                                                dst_task=comp_desc,
                                                                                file_name=input_basename,
                                                                                file_path=input_file)
                                explicit_files_to_stage.append(data_dep)

                    to_submit.append(comp_desc)

                    msg = f"Ready to submit: {comp_desc['name']}"
                    msg += f" with resolved dependencies: {[dep['name'] for dep in dependencies]}"
                    self.log.debug(msg)

            if to_submit:
                await self.queue.put(to_submit)
                for t in to_submit:
                    self.running.append(t['uid'])
                    self.resolved.add(t['uid'])
                    self.unresolved.remove(t['uid'])

            await asyncio.sleep(0.5)

    async def submit(self):
        """Async method to submit blocks or tasks from the queue for execution."""
        while True:
            try:
                objects = await asyncio.wait_for(self.queue.get(), timeout=1)

                # pass only the id of the resolved tasks to the backend
                tasks = [t for t in objects if t and BLOCK not in t['uid']]
                blocks = [b for b in objects if b and TASK not in b['uid']]

                self.log.debug(f'Submitting {[b['name'] for b in objects]} for execution')

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
        """Async method to submit blocks for execution."""
        for block in blocks:
            args = block['args']
            kwargs = block['kwargs']
            func = block['function']
            block_fut = self.components[block['uid']]['future']

            # Execute the block function as a coroutine
            asyncio.create_task(self.execute_block(block_fut, func, *args, **kwargs))

    async def execute_block(self, block_fut, func, *args, **kwargs):
        """Async method to execute block function and update asyncio future."""
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
        """
        Handle task success by setting the result in the future.
        """
        internal_task = self.components[task['uid']]['description']

        if internal_task[FUNCTION]:
            task_fut.set_result(task['return_value'])
        else:
            task_fut.set_result(task['stdout'])

    def handle_task_failure(self, task, task_fut):
        """
        Handle task failure by setting the exception in the future.
        """
        internal_task = self.components[task['uid']]['description']

        if internal_task[FUNCTION]:
            task_fut.set_exception(Exception(task['exception']))
        else:
            task_fut.set_exception(Exception(task['stderr']))

    def task_callbacks(self, task, state):
        """
        Callback function to handle task state changes using asyncio.Future.
        """
        # we assume that the task object is a dictionary or a dict-like object
        if not isinstance(task, dict):
            task = task.as_dict()

        if task['uid'] not in self.components:
            self.log.warning(f'Received an unknown task and will skip it: {task["uid"]}')
            return


        if state in self.task_states_map.terminal_states:
            self.log.info(f'{task["uid"]} is in {state} state')
            task_fut = self.components[task['uid']]['future']
            if state == self.task_states_map.DONE:
                self.handle_task_success(task, task_fut)
            elif state == self.task_states_map.CANCELED:
                task_fut.cancel()
            elif state == self.task_states_map.RUNNING:
                task_fut.set_running_or_notify_cancel()
            else:
                self.handle_task_failure(task, task_fut)

import os
import asyncio
import typeguard
import subprocess
import radical.utils as ru
from typing import Dict, Callable
from concurrent.futures import ThreadPoolExecutor, Future

from ...constants import StateMapper
from .base import Session, BaseExecutionBackend


class ThreadExecutionBackend(BaseExecutionBackend):
    """Thread-based execution backend for running tasks concurrently.
    
    This backend uses a ThreadPoolExecutor to execute tasks in separate threads,
    supporting both synchronous and asynchronous functions as well as subprocess
    execution. It manages task states, handles callbacks, and provides graceful
    shutdown capabilities.
    
    Attributes:
        tasks (Dict): Dictionary storing task descriptions indexed by UID.
        session (Session): Session object for managing execution context.
        executor (ThreadPoolExecutor): Thread pool for concurrent task execution.
        _callback_func (Callable[[Future], None]): Callback function for task completion.
    
    Example:
        ::
            resources = {'max_workers': 4}
            backend = ThreadExecutionBackend(resources)
            backend.register_callback(my_callback_func)
            backend.build_task('task_1', task_description, {})
            backend.submit_tasks([task_list])
    """
    
    @typeguard.typechecked
    def __init__(self, resources: Dict):
        """Initialize the ThreadExecutionBackend with specified resources.
        
        Args:
            resources (Dict): Configuration dictionary for ThreadPoolExecutor.
                Typically contains 'max_workers' to specify thread pool size.
        
        Note:
            Automatically registers backend states with defaults and prints
            startup confirmation message.
        """
        self.tasks = {}
        self.session = Session()
        self.executor = ThreadPoolExecutor(**resources)
        self._callback_func: Callable[[Future], None] = lambda f: None
        StateMapper.register_backend_states_with_defaults(backend=self)
        print('ThreadPool execution backend started successfully')

    def get_task_states_map(self):
        """Get the state mapper for this backend.
        
        Returns:
            StateMapper: State mapper instance configured for this backend.
        """
        return StateMapper(backend=self)

    def state(self):
        """Get the current state of the backend.
        
        Note:
            This method is currently not implemented and serves as a placeholder.
        """
        pass

    def task_state_cb(self):
        """Task state callback handler.
        
        Note:
            This method is currently not implemented and serves as a placeholder.
        """
        pass

    def register_callback(self, func: Callable):
        """Register a callback function for task completion events.
        
        Args:
            func (Callable): Callback function to be invoked when tasks complete.
                Should accept the result of Future.result() as parameters.
        """
        self._callback_func = func

    def build_task(self, uid, task_desc, task_specific_kwargs):
        """Build and register a task with the backend.
        
        Args:
            uid: Unique identifier for the task.
            task_desc: Task description containing execution details.
            task_specific_kwargs: Task-specific keyword arguments.
        """
        self.tasks[uid] = task_desc

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        """Link explicit data dependencies between tasks.
        
        Args:
            src_task (optional): Source task for the dependency.
            dst_task (optional): Destination task for the dependency.
            file_name (optional): Name of the file involved in the dependency.
            file_path (optional): Path to the file involved in the dependency.
        
        Note:
            This method is currently not implemented and serves as a placeholder.
        """
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        """Link implicit data dependencies between tasks.
        
        Args:
            src_task: Source task for the dependency.
            dst_task: Destination task for the dependency.
        
        Note:
            This method is currently not implemented and serves as a placeholder.
        """
        pass

    def run_async_func(self, coroutine_func):
        """Execute an async function in the appropriate event loop context.
        
        Handles different event loop scenarios:
        - Creates new event loop if current loop is running
        - Uses existing loop if available but not running
        - Creates new event loop if none exists
        
        Args:
            coroutine_func: Function that returns a coroutine to be executed.
        
        Returns:
            The result of the coroutine execution.
        
        Raises:
            RuntimeError: If event loop handling fails.
        """
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # Run in a new event loop
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                return new_loop.run_until_complete(coroutine_func())
            else:
                return loop.run_until_complete(coroutine_func())
        except RuntimeError:
            # No event loop at all
            return asyncio.run(coroutine_func())

    def _task_wrapper(self, task):
        """Wrap and execute a task, handling both functions and subprocess calls.
        
        Executes tasks in one of two modes:
        1. Function execution: Calls Python functions (sync or async)
        2. Subprocess execution: Runs external commands via subprocess
        
        Args:
            task (Dict): Task dictionary containing execution parameters.
                For functions: 'function', 'args', 'kwargs'
                For subprocesses: 'executable', 'arguments'
        
        Returns:
            Tuple[Dict, str]: Updated task dictionary and final state.
                State can be 'DONE', 'FAILED'.
        
        Task Updates:
            - return_value: Result of function execution (function mode only)
            - stdout: Standard output or string representation of return value
            - stderr: Standard error output (subprocess mode only)
            - exit_code: Exit code (0 for success, 1 for failure)
            - exception: Exception object if execution failed
        
        Example:
            ::
                # Function task
                task = {
                    'function': my_function,
                    'args': [arg1, arg2],
                    'kwargs': {'key': 'value'}
                }
                
                # Subprocess task
                task = {
                    'executable': 'python',
                    'arguments': ['-c', 'print("hello")']
                }
        """
        try:
            if 'function' in task and task['function']:
                func = task['function']
                args = task.get('args', [])
                kwargs = task.get('kwargs', {})

                if asyncio.iscoroutinefunction(task['function']):
                    return_value = self.run_async_func(lambda: func(*args, **kwargs))
                else:
                    return_value = func(*args, **kwargs)

                task['return_value'] = return_value
                task['stdout'] = str(return_value)
                task['exit_code'] = 0
                state = 'DONE'
            else:
                exec_list = [task['executable']]
                exec_list.extend(task.get('arguments', []))

                result = subprocess.run(exec_list, text=True,
                                        capture_output=True, shell=True)

                task['stdout'] = result.stdout
                task['stderr'] = result.stderr
                task['exit_code'] = result.returncode
                state = 'DONE' if result.returncode == 0 else 'FAILED'
        except Exception as e:
            state = 'FAILED'
            task['stderr'] = None
            task['stdout'] = None
            task['exit_code'] = 1
            task['exception'] = e
            task['return_value'] = None

        return task, state

    def submit_tasks(self, tasks: list):
        """Submit multiple tasks for concurrent execution.
        
        Submits each task to the thread pool executor and registers completion
        callbacks. Tasks are executed concurrently based on thread pool capacity.
        
        Args:
            tasks (list): List of task dictionaries to be executed.
                Each task must contain 'task_backend_specific_kwargs' key.
        
        Note:
            Completion callbacks are automatically invoked when tasks finish,
            using the callback function registered via register_callback().
        
        Example:
            ::
                tasks = [
                    {
                        'function': my_func,
                        'args': [],
                        'task_backend_specific_kwargs': {}
                    },
                    {
                        'executable': 'echo',
                        'arguments': ['hello'],
                        'task_backend_specific_kwargs': {}
                    }
                ]
                backend.submit_tasks(tasks)
        """
        for task in tasks:
            # Submit task to thread pool
            fut = self.executor.submit(self._task_wrapper, task,
                                       **task['task_backend_specific_kwargs'])
            fut.add_done_callback(lambda f, task=task: self._callback_func(*f.result()))

    def shutdown(self) -> None:
        """Shutdown the backend and clean up resources.
        
        Gracefully terminates the thread pool executor, cancelling any pending
        futures and waiting for running tasks to complete.
        
        Note:
            Prints confirmation message when shutdown is triggered.
        """
        self.executor.shutdown(cancel_futures=True)
        print('Shutdown is triggered, terminating the resources gracefully')

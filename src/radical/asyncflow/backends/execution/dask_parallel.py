import asyncio
from typing import List, Dict, Any, Union, Optional, Callable
import dask
import typeguard
from functools import wraps
from dask.distributed import Client, Future as DaskFuture
from concurrent.futures import Future as ConcurrentFuture

from ...constants import StateMapper
from .base import BaseExecutionBackend, Session


class DaskExecutionBackend(BaseExecutionBackend):
    """A robust Dask execution backend supporting both synchronous and asynchronous functions.
    
    Handles task submission, cancellation, and proper event loop handling
    for distributed task execution using Dask.
    """

    @typeguard.typechecked
    def __init__(self, resources: Optional[Dict]):
        """Initialize the Dask execution backend.

        Args:
            resources: Dictionary of resource requirements for tasks. Contains
                configuration parameters for the Dask client initialization.
        """
        self.tasks = {}
        self._client = None
        self.session = Session()
        self._callback_func = None
        self.initialize(resources)
        StateMapper.register_backend_states_with_defaults(backend=self)

    def initialize(self, resources) -> None:
        """Initialize the Dask client and set up worker environments.

        Args:
            resources: Configuration parameters for Dask client initialization.

        Raises:
            Exception: If Dask client initialization fails.
        """
        try:
            self._client = Client(**resources)
            # Ensure workers can handle async functions
            print(f"Dask backend initialized with dashboard at {self._client.dashboard_link}")
        except Exception as e:
            print(f"Failed to initialize Dask client: {str(e)}")
            raise

    def register_callback(self, callback: Callable) -> None:
        """Register a callback for task state changes.
        
        Args:
            callback: Function to be called when task states change. Should accept
                task and state parameters.
        """
        self._callback_func = callback

    def get_task_states_map(self):
        """Retrieve a mapping of task IDs to their current states.
        
        Returns:
            StateMapper: Object containing the mapping of task states for this backend.
        """
        return StateMapper(backend=self)

    def cancel_task(self, uid: str) -> bool:
        """Cancel a task by its UID.

        Args:
            uid (str): The UID of the task to cancel.

        Returns:
            bool: True if the task was found and cancellation was attempted, False otherwise.
        """
        if uid in self.tasks:
            task = self.tasks[uid]
            future = task.get('future')
            if future:
                return future.cancel()
        return False

    def submit_tasks(self, tasks: List[Dict[str, Any]]) -> None:
        """Submit tasks to Dask cluster, handling both sync and async functions.

        Processes a list of tasks and submits them to the Dask cluster for execution.
        Filters out future objects from arguments and handles both synchronous and
        asynchronous functions appropriately.

        Args:
            tasks: List of task dictionaries containing:
                - uid: Unique task identifier
                - function: Callable to execute
                - args: Positional arguments
                - kwargs: Keyword arguments
                - async: Boolean indicating if function is async
                - executable: Optional executable path (not supported)
                - task_backend_specific_kwargs: Backend-specific parameters
                
        Note:
            Executable tasks are not supported and will result in FAILED state.
            Future objects are filtered out from arguments as they are not picklable.
        """
        for task in tasks:
            is_func_task = bool(task.get('function'))
            is_exec_task = bool(task.get('executable'))

            if not is_func_task and is_exec_task:
                error_msg = 'DaskExecutionBackend does not support executable tasks'
                task['stderr'] = ValueError(error_msg)
                self._callback_func(task, 'FAILED')
                continue

            self.tasks[task['uid']] = task

            # make sure we do not pass future object to Dask as it is not picklable
            task['args'] = tuple(arg for arg in task['args'] if not isinstance(arg,
                                               (ConcurrentFuture, asyncio.Future)))

            try:
                if asyncio.iscoroutinefunction(task['function']):
                    self._submit_async_function(task)
                    print(f"Successfully submitted async task {task['uid']}")
                else:
                    self._submit_sync_function(task)
                    print(f"Successfully submitted sync task {task['uid']}")
            except Exception as e:
                print(f"Failed to submit task {task['uid']}: {str(e)}")
                raise

    def _submit_to_dask(self, task: Dict[str, Any], fn: Callable, *args) -> None:
        """Submit function to Dask and register completion callback.
        
        Submits the wrapped function to Dask client and registers a callback
        to handle task completion or failure.
        
        Args:
            task: Task dictionary containing task metadata and configuration.
            fn: The function to submit to Dask.
            *args: Arguments to pass to the function.
        """
        def on_done(f: DaskFuture):
            task_uid = task['uid']
            try:
                result = f.result()
                task['return_value'] = result
                self._callback_func(task, 'DONE')
            except dask.distributed.client.FutureCancelledError:
                self._callback_func(task, 'CANCELED')
            except Exception as e:
                task['exception'] = e
                self._callback_func(task, 'FAILED')
            finally:
                # Clean up the future reference once task is complete
                if task_uid in self.tasks:
                    del self.tasks[task_uid]

        dask_future = self._client.submit(fn, *args,
                                          **task['task_backend_specific_kwargs'])

        # Store the future for potential cancellation
        self.tasks[task['uid']]['future'] = dask_future

        dask_future.add_done_callback(on_done)

    def _submit_async_function(self, task: Dict[str, Any]) -> None:
        """Submit async function to Dask.
        
        Creates an async wrapper that preserves the original function name
        for better visibility in the Dask dashboard.
        
        Args:
            task: Task dictionary containing the async function and its parameters.
        """
        
        # in dask dashboard we want the real task name not "async_wrapper"
        @wraps(task['function'])
        async def async_wrapper():
            return await task['function'](*task['args'], **task['kwargs'])

        self._submit_to_dask(task, async_wrapper)

    def _submit_sync_function(self, task: Dict[str, Any]) -> None:
        """Submit sync function to Dask.
        
        Creates a sync wrapper that preserves the original function name
        for better visibility in the Dask dashboard.
        
        Args:
            task: Task dictionary containing the sync function and its parameters.
        """

        # in dask dashboard we want the real task name not "sync_wrapper"
        @wraps(task['function'])
        def sync_wrapper(fn, args, kwargs):
            return fn(*args, **kwargs)

        self._submit_to_dask(task, sync_wrapper, task['function'], task['args'], task['kwargs'])

    def cancel_all_tasks(self) -> int:
        """Cancel all currently running/pending tasks.
        
        Returns:
            Number of tasks that were successfully cancelled
        """
        cancelled_count = 0
        task_uids = list(self.tasks.keys())
        
        for task_uid in task_uids:
            if self.cancel_task(task_uid):
                cancelled_count += 1
        
        return cancelled_count

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        """Handle explicit data dependencies between tasks.
        
        Args:
            src_task: The source task that produces the dependency. Defaults to None.
            dst_task: The destination task that depends on the source. Defaults to None.
            file_name: Name of the file that represents the dependency. Defaults to None.
            file_path: Full path to the file that represents the dependency. Defaults to None.
        """
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        """Handle implicit data dependencies for a task.
        
        Args:
            src_task: The source task that produces data.
            dst_task: The destination task that depends on the source task's output.
        """
        pass

    def state(self) -> str:
        """Get the current state of the Dask execution backend.
        
        Returns:
            Current state of the backend as a string.
        """
        pass

    def task_state_cb(self, task: dict, state: str) -> None:
        """Callback function invoked when a task's state changes.
        
        Args:
            task: Dictionary containing task information and metadata.
            state: The new state of the task.
        """
        pass

    def build_task(self, task: dict) -> None:
        """Build or prepare a task for execution.
        
        Args:
            task: Dictionary containing task definition, parameters, and metadata
                required for task construction.
        """
        pass

    def shutdown(self) -> None:
        """Shutdown the Dask client and clean up resources.
        
        Closes the Dask client connection, clears task storage, and handles
        any cleanup exceptions gracefully.
        """
        if self._client is not None:
            try:
                self._client.close()
                print("Dask client shutdown complete")
            except Exception as e:
                print(f"Error during shutdown: {str(e)}")
            finally:
                self._client = None
                self.tasks.clear()

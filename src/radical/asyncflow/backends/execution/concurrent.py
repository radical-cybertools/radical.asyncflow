import asyncio
import typeguard
import subprocess
from typing import Dict, Callable, Optional, Any, List, Union
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Executor

from ...constants import StateMapper
from .base import Session, BaseExecutionBackend


class ConcurrentExecutionBackend(BaseExecutionBackend):
    @typeguard.typechecked
    def __init__(self, executor: Union[ThreadPoolExecutor, ProcessPoolExecutor]):
        if not isinstance(executor, (ThreadPoolExecutor, ProcessPoolExecutor)):
            raise TypeError("Executor must be an instance of ThreadPoolExecutor or ProcessPoolExecutor.")
        
        self.tasks: Dict[str, asyncio.Future] = {}
        self.session: Session = Session()
        self.executor: Union[Executor, ThreadPoolExecutor, ProcessPoolExecutor] = executor
        self._callback_func: Optional[Callable[[Any, str], None]] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        StateMapper.register_backend_states_with_defaults(backend=self)
        print(f'{type(executor).__name__} execution backend started successfully')

    def get_task_states_map(self):
        return StateMapper(backend=self)

    def state(self):
        pass

    def task_state_cb(self):
        pass

    def register_callback(self, func: Callable):
        self._callback_func = func

    def build_task(self, uid, task_desc, task_specific_kwargs):
        self.tasks[uid] = task_desc
    
    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        """Get the current event loop or create one if none exists."""
        try:
            loop = asyncio.get_running_loop()
            return loop
        except RuntimeError:
            # No event loop running, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    async def _run_coroutine_in_thread(self, coro_func, *args, **kwargs):
        """Run a coroutine function in the current event loop."""
        return await coro_func(*args, **kwargs)

    def _run_sync_in_thread(self, sync_func, *args, **kwargs):
        """Run a synchronous function (for use in thread pool)."""
        return sync_func(*args, **kwargs)

    async def _task_wrapper_async(self, task):
        """Async wrapper for task execution."""
        try:
            if 'function' in task and task['function']:
                func = task['function']
                args = task.get('args', [])
                kwargs = task.get('kwargs', {})

                if asyncio.iscoroutinefunction(func):
                    # Run coroutine directly in current event loop
                    return_value = await func(*args, **kwargs)
                else:
                    # Run sync function in thread pool to avoid blocking
                    loop = asyncio.get_running_loop()
                    return_value = await loop.run_in_executor(
                        self.executor, self._run_sync_in_thread, func, *args, **kwargs
                    )

                task['return_value'] = return_value
                task['stdout'] = str(return_value)
                task['exit_code'] = 0
                state = 'DONE'
            else:
                # Execute subprocess asynchronously
                exec_list = [task['executable']]
                exec_list.extend(task.get('arguments', []))
                
                # Build command string for shell execution
                cmd = ' '.join(exec_list)

                # Use asyncio.create_subprocess_shell for better async handling
                try:
                    process = await asyncio.create_subprocess_shell(
                        cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        text=True
                    )
                    stdout, stderr = await process.communicate()
                    
                    task['stdout'] = stdout
                    task['stderr'] = stderr
                    task['exit_code'] = process.returncode
                    state = 'DONE' if process.returncode == 0 else 'FAILED'
                except Exception as e:
                    # Fallback to subprocess.run in thread pool
                    loop = asyncio.get_running_loop()
                    result = await loop.run_in_executor(
                        self.executor,
                        lambda: subprocess.run(cmd, text=True, capture_output=True, shell=True)
                    )
                    
                    task['stdout'] = result.stdout
                    task['stderr'] = result.stderr
                    task['exit_code'] = result.returncode
                    state = 'DONE' if result.returncode == 0 else 'FAILED'
                    
        except Exception as e:
            state = 'FAILED'
            task['stderr'] = str(e)
            task['stdout'] = None
            task['exit_code'] = 1
            task['exception'] = e            
            task['return_value'] = None

        return task, state

    def _task_wrapper_sync(self, task):
        """Synchronous wrapper for task execution (original implementation)."""
        try:
            if 'function' in task and task['function']:
                func = task['function']
                args = task.get('args', [])
                kwargs = task.get('kwargs', {})

                if asyncio.iscoroutinefunction(task['function']):
                    # This is problematic - we're in a thread pool, can't run coroutines directly
                    # We'll need to handle this case differently
                    raise RuntimeError("Cannot run coroutine function in sync context. Use async submission.")
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
            task['stderr'] = str(e)
            task['stdout'] = None
            task['exit_code'] = 1
            task['exception'] = e            
            task['return_value'] = None

        return task, state

    async def submit_tasks_async(self, tasks: List[Dict]) -> List[asyncio.Task]:
        """Submit tasks asynchronously and return task objects."""
        async_tasks = []
        
        for task in tasks:
            # Create async task for each submission
            async_task = asyncio.create_task(self._task_wrapper_async(task))
            
            # Add callback if provided
            if self._callback_func:
                async_task.add_done_callback(
                    lambda t: self._callback_func(*t.result()) if not t.cancelled() else None
                )
            
            async_tasks.append(async_task)
        
        return async_tasks

    def submit_tasks(self, tasks: List[Dict]):
        """Submit tasks (maintains backward compatibility)."""
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, schedule the async version
            for task in tasks:
                asyncio.create_task(self._handle_task_async(task))
        except RuntimeError:
            # No event loop running, use sync version
            for task in tasks:
                fut = self.executor.submit(self._task_wrapper_sync, task)
                if self._callback_func:
                    fut.add_done_callback(lambda f: self._callback_func(*f.result()))

    async def _handle_task_async(self, task):
        """Handle individual task asynchronously."""
        try:
            result = await self._task_wrapper_async(task)
            if self._callback_func:
                self._callback_func(*result)
        except Exception as e:
            if self._callback_func:
                failed_task = task.copy()
                failed_task.update({
                    'stderr': str(e),
                    'stdout': None,
                    'exit_code': 1,
                    'exception': e,
                    'return_value': None
                })
                self._callback_func(failed_task, 'FAILED')

    async def wait_for_tasks(self, tasks: List[asyncio.Task], timeout: Optional[float] = None):
        """Wait for all tasks to complete with optional timeout."""
        try:
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            
            # Handle any pending tasks if timeout occurred
            if pending:
                print(f"Warning: {len(pending)} tasks did not complete within timeout")
                for task in pending:
                    task.cancel()
            
            return [task.result() for task in done if not task.cancelled()]
        except Exception as e:
            print(f"Error waiting for tasks: {e}")
            return []

    def shutdown(self) -> None:
        """Shutdown the executor gracefully."""
        self.executor.shutdown(wait=True)
        print('Shutdown is triggered, terminating the resources gracefully')

    async def aclose(self):
        """Async cleanup method."""
        self.executor.shutdown(wait=False)
        print('Async shutdown completed')

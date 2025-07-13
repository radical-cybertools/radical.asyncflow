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

    async def _execute_function_task(self, task: Dict) -> tuple[Dict, str]:
        """Execute a function-based task."""
        func = task['function']
        args = task.get('args', [])
        kwargs = task.get('kwargs', [])

        try:
            if asyncio.iscoroutinefunction(func):
                # Run coroutine directly
                return_value = await func(*args, **kwargs)
            else:
                # Run sync in thread pool
                loop = asyncio.get_running_loop()
                return_value = await loop.run_in_executor(
                    self.executor, func, *args, **kwargs
                )

            task.update({
                'return_value': return_value,
                'stdout': str(return_value),
                'exit_code': 0
            })
            state = 'DONE'

        except Exception as e:
            task.update({
                'stderr': str(e),
                'stdout': None,
                'exit_code': 1,
                'exception': e,
                'return_value': None
            })
            state = 'FAILED'

        return task, state

    async def _execute_executable_task(self, task: Dict) -> tuple[Dict, str]:
        """Execute a executable-based task."""
        exec_list = [task['executable']]
        exec_list.extend(task.get('arguments', []))
        cmd = ' '.join(exec_list)

        try:
            # Try async executable first
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            task.update({
                'stdout': stderr.decode(),
                'stderr': stdout.decode(),
                'exit_code': process.returncode})

            state = 'DONE' if process.returncode == 0 else 'FAILED'
            return task, state

        except (NotImplementedError, RuntimeError) as e:
            # Fallback to sync executable in thread pool
            # This handles cases where asyncio executables aren't supported
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self.executor,
                lambda: subprocess.run(cmd, text=True, capture_output=True, shell=True)
            )

            task.update({
                'stdout': result.stdout,
                'stderr': result.stderr,
                'exit_code': process.returncode})

            state = 'DONE' if result.returncode == 0 else 'FAILED'
            return task, state

        except Exception as e:            
            task.update({
                'exception': e,
                'return_value': None,
                'stdout': result.stdout,
                'stderr': str(e),
                'exit_code': process.returncode})

            state = 'FAILED'
            return task, state

    async def _execute_task(self, task: Dict) -> tuple[Dict, str]:
        """Execute a single task (function or executable)."""
        if 'function' in task and task['function']:
            return await self._execute_function_task(task)
        else:
            return await self._execute_executable_task(task)

    async def _handle_task_with_callback(self, task: Dict) -> None:
        """Handle task execution and invoke callback if registered."""
        try:
            result_task, state = await self._execute_task(task)
            if self._callback_func:
                # Run callback in thread pool to avoid blocking if it's sync
                if asyncio.iscoroutinefunction(self._callback_func):
                    await self._callback_func(result_task, state)
                else:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        self.executor, self._callback_func, result_task, state
                    )
        except Exception as e:
            # Handle unexpected errors in task execution
            failed_task = task.copy()
            failed_task.update({
                'stderr': str(e),
                'stdout': None,
                'exit_code': 1,
                'exception': e,
                'return_value': None
            })
            if self._callback_func:
                if asyncio.iscoroutinefunction(self._callback_func):
                    await self._callback_func(failed_task, 'FAILED')
                else:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        self.executor, self._callback_func, failed_task, 'FAILED'
                    )

    def submit_tasks(self, tasks: List[Dict]) -> None:
        """Submit tasks (maintains backward compatibility)."""
        # Check if we're in an async context
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, schedule the tasks
            for task in tasks:
                asyncio.create_task(self._handle_task_with_callback(task))
        except RuntimeError:
            # No event loop running, we need to handle this differently
            # In this case, we'll use the sync fallback approach
            self._submit_tasks_sync(tasks)

    async def submit_tasks_async(self, tasks: List[Dict]) -> List[asyncio.Task]:
        """Submit tasks for async execution and return Task objects."""
        async_tasks = []
        for task in tasks:
            async_task = asyncio.create_task(self._handle_task_with_callback(task))
            async_tasks.append(async_task)
        return async_tasks

    def _submit_tasks_sync(self, tasks: List[Dict]) -> None:
        """Fallback sync task submission for when no event loop is running."""
        for task in tasks:
            if 'function' in task and task['function'] and asyncio.iscoroutinefunction(task['function']):
                # Cannot execute coroutines in sync context
                if self._callback_func:
                    failed_task = task.copy()
                    failed_task.update({
                        'stderr': 'Cannot execute coroutine function without event loop',
                        'stdout': None,
                        'exit_code': 1,
                        'exception': RuntimeError('Cannot execute coroutine function without event loop'),
                        'return_value': None
                    })
                    self._callback_func(failed_task, 'FAILED')
                continue

            # Submit sync task to executor
            fut = self.executor.submit(self._execute_task_sync, task)
            if self._callback_func:
                fut.add_done_callback(
                    lambda f: self._callback_func(*f.result()) if not f.exception() else None
                )

    def _execute_task_sync(self, task: Dict) -> tuple[Dict, str]:
        """Synchronous task execution for fallback cases."""
        try:
            if 'function' in task and task['function']:
                func = task['function']
                args = task.get('args', [])
                kwargs = task.get('kwargs', {})

                if asyncio.iscoroutinefunction(func):
                    raise RuntimeError("Cannot run coroutine function in sync context")

                return_value = func(*args, **kwargs)

                task.update({
                    'return_value': return_value,
                    'stdout': str(return_value),
                    'exit_code': 0
                })
                state = 'DONE'

            else:
                exec_list = [task['executable']] + task.get('arguments', [])
                cmd = ' '.join(exec_list)

                result = subprocess.run(cmd, text=True, capture_output=True, shell=True)

                task.update({
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'exit_code': result.returncode
                })
                state = 'DONE' if result.returncode == 0 else 'FAILED'

        except Exception as e:
            task.update({
                'stderr': str(e),
                'stdout': None,
                'exit_code': 1,
                'exception': e,
                'return_value': None
            })
            state = 'FAILED'

        return task, state

    def shutdown(self) -> None:
        """Shutdown the executor gracefully."""
        self.executor.shutdown(wait=True)
        print('Shutdown is triggered, terminating the resources gracefully')

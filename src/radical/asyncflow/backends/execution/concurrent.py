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

    async def _execute_task(self, task: Dict) -> tuple[Dict, str]:
        """Execute a single task (function or executable)."""
        try:
            if 'function' in task and task['function']:
                return await self._execute_function(task)
            else:
                return await self._execute_command(task)
        except Exception as e:
            task.update({
                'stderr': str(e),
                'stdout': None,
                'exit_code': 1,
                'exception': e,
                'return_value': None
            })
            return task, 'FAILED'

    async def _execute_function(self, task: Dict) -> tuple[Dict, str]:
        """Execute a function-based task."""
        func = task['function']
        args = task.get('args', [])
        kwargs = task.get('kwargs', {})

        if asyncio.iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        else:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(self.executor, func, *args, **kwargs)

        task.update({
            'return_value': result,
            'stdout': str(result),
            'exit_code': 0
        })
        return task, 'DONE'

    async def _execute_command(self, task: Dict) -> tuple[Dict, str]:
        """Execute a command-based task."""
        cmd = ' '.join([task['executable']] + task.get('arguments', []))

        try:
            # Try async subprocess first
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            
            task.update({
                'stdout': stdout.decode(),
                'stderr': stderr.decode(),
                'exit_code': process.returncode
            })
            
        except (NotImplementedError, RuntimeError):
            # Fallback to sync subprocess in thread pool
            # This handles cases where asyncio executables aren't supported
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self.executor,
                lambda: subprocess.run(cmd, text=True, capture_output=True, shell=True)
            )
            
            task.update({
                'stdout': result.stdout,
                'stderr': result.stderr,
                'exit_code': result.returncode
            })

        state = 'DONE' if task['exit_code'] == 0 else 'FAILED'
        return task, state

    async def _handle_task_with_callback(self, task: Dict) -> None:
        """Handle task execution and invoke callback if registered."""
        result_task, state = await self._execute_task(task)
        
        if self._callback_func:
            if asyncio.iscoroutinefunction(self._callback_func):
                await self._callback_func(result_task, state)
            else:
                self._callback_func(result_task, state)

    def submit_tasks(self, tasks: List[Dict]) -> None:
        """Submit tasks for execution."""

        try:
            # If we're in an async context, create tasks directly
            loop = asyncio.get_running_loop()
            for task in tasks:
                asyncio.create_task(self._handle_task_with_callback(task))
        except RuntimeError:
            # No event loop running, create one
            asyncio.run(self._run_tasks_in_new_loop(tasks))

    async def submit_tasks_async(self, tasks: List[Dict]) -> List[asyncio.Task]:
        """Submit tasks for async execution and return Task objects."""
        return [asyncio.create_task(self._handle_task_with_callback(task)) for task in tasks]

    async def _run_tasks_in_new_loop(self, tasks: List[Dict]) -> None:
        """Run tasks in a new event loop."""
        await asyncio.gather(*[self._handle_task_with_callback(task) for task in tasks])

    def shutdown(self) -> None:
        """Shutdown the executor gracefully."""
        self.executor.shutdown(wait=True)
        print('Shutdown is triggered, terminating the resources gracefully')

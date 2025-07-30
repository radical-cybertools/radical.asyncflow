import asyncio
import subprocess
from typing import Dict, Callable, Optional, Any, List, Union
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from ...constants import StateMapper
from .base import Session, BaseExecutionBackend


class ConcurrentExecutionBackend(BaseExecutionBackend):
    """Simple async-only concurrent execution backend."""

    def __init__(self, executor: Union[ThreadPoolExecutor, ProcessPoolExecutor]):
        if not isinstance(executor, (ThreadPoolExecutor, ProcessPoolExecutor)):
            raise TypeError("Executor must be ThreadPoolExecutor or ProcessPoolExecutor")

        self.executor = executor
        self.tasks: Dict[str, asyncio.Task] = {}
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
            print(f'{type(self.executor).__name__} execution backend started successfully')
        return self

    def get_task_states_map(self):
        return StateMapper(backend=self)

    def register_callback(self, func: Callable):
        self._callback_func = func

    async def _execute_task(self, task: Dict) -> tuple[Dict, str]:
        """Execute a single task."""
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
        """Execute function task."""
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
        """Execute command task."""
        cmd = ' '.join([task['executable']] + task.get('arguments', []))

        try:
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            task.update({
                'stdout': stdout.decode(),
                'stderr': stderr.decode(),
                'exit_code': process.returncode
            })

        except Exception:
            # Fallback to thread executor
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self.executor,
                subprocess.run, cmd, True, True
            )

            task.update({
                'stdout': result.stdout,
                'stderr': result.stderr,
                'exit_code': result.returncode
            })

        state = 'DONE' if task['exit_code'] == 0 else 'FAILED'
        return task, state

    async def _handle_task(self, task: Dict) -> None:
        """Handle task execution with callback."""
        result_task, state = await self._execute_task(task)

        self._callback_func(result_task, state)

    async def submit_tasks(self, tasks: List[Dict]) -> List[asyncio.Task]:
        """Submit tasks for execution."""
        submitted_tasks = []

        for task in tasks:
            task_uid = task.get('uid', f'task_{id(task)}')
            async_task = asyncio.create_task(self._handle_task(task))
            self.tasks[task_uid] = async_task
            submitted_tasks.append(async_task)

        return submitted_tasks

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task by UID."""
        if uid in self.tasks:
            self.tasks[uid].cancel()
            self._callback_func(task, 'CANCELED')
            return True
        return False

    async def cancel_all_tasks(self) -> int:
        """Cancel all running tasks."""
        cancelled_count = 0
        for task in self.tasks.values():
            task.cancel()
            cancelled_count += 1
        self.tasks.clear()
        return cancelled_count

    async def shutdown(self) -> None:
        """Shutdown the executor."""
        await self.cancel_all_tasks()
        self.executor.shutdown(wait=True)
        print('Shutdown complete')

    def build_task(self, uid, task_desc, task_specific_kwargs):
        self.tasks[uid] = task_desc
    
    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    def state(self):
        pass

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
    async def create(cls, executor: Union[ThreadPoolExecutor, ProcessPoolExecutor]):
        """Factory method for creating initialized backend."""
        backend = cls(executor)
        return await backend

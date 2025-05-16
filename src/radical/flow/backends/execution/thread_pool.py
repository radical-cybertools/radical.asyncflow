import os
import asyncio
import typeguard
import subprocess
import radical.utils as ru
from typing import Dict, Callable
from concurrent.futures import ThreadPoolExecutor, Future

from .base import Session, BaseExecutionBackend


class ThreadExecutionBackend(BaseExecutionBackend):
    @typeguard.typechecked
    def __init__(self, resources: Dict):
        self.tasks = {}
        self.session = Session()
        self.executor = ThreadPoolExecutor()
        self._callback_func: Callable[[Future], None] = lambda f: None
        print('ThreadPool execution backend started successfully')

    def state(self):
        pass

    def task_state_cb(self):
        pass

    def register_callback(self, func: Callable):
        self._callback_func = func

    def build_task(self, uid, task_desc, task_specific_kwargs):
        self.tasks[uid] = task_desc
    
    def link_explicit_data_deps(self, task_id, file_name=None):
        pass

    def link_implicit_data_deps(self, src_task):
        pass

    def run_async_func(self, coroutine_func):
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
            task['return_value'] = None
            task['stdout'] = ''
            task['stderr'] = str(e)
            task['exit_code'] = 1
            task['exception'] = str(e)
            state = 'FAILED'

        return task, state

    def submit_tasks(self, tasks: list):
        for task in tasks:
            # Submit task to thread pool
            fut = self.executor.submit(self._task_wrapper, task,
                                       **task['task_backend_specific_kwargs'])
            fut.add_done_callback(lambda f, task=task: self._callback_func(*f.result()))

    def shutdown(self) -> None:
        self.executor.shutdown(cancel_futures=True)
        print('Shutdown is triggered, terminating the resources gracefully')

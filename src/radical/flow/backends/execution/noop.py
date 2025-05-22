import os
from typing import Dict, Callable

from .base import Session, BaseExecutionBackend


class NoopExecutionBackend(BaseExecutionBackend):
    def __init__(self):
        self.tasks = {}
        self.session = Session()
        self._callback_func: Callable = lambda task, state: None  # default no-op
        print('Noop execution backend started successfully')

    def state(self):
        return 'IDLE'

    def task_state_cb(self):
        pass

    def register_callback(self, func: Callable):
        self._callback_func = func

    def build_task(self, uid, task_desc, task_specific_kwargs):
        self.tasks[uid] = task_desc

    def submit_tasks(self, tasks):
        for task in tasks:
            task['stdout'] = 'Dummy Output'
            task['return_value'] = 'Dummy Output'
            self._callback_func(task, 'DONE')

    def link_explicit_data_deps(self, task_id, file_name=None, file_path=None):
        # No-op: No explicit data dependencies in the Noop backend
        pass

    def link_implicit_data_deps(self, task):
        # No-op: No implicit data dependencies in the Noop backend
        pass

    def build_task(self, uid, task_desc, task_specific_kwargs):
        # No-op: No task registration in the Noop backend
        pass

    def shutdown(self) -> None:
        print('Dummy shutdown: Nothing to cleanup.')

from unittest.mock import MagicMock

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ThreadExecutionBackend
from radical.asyncflow import InputFile, OutputFile


def test_implicit_data_dependencies_trigger():

    backend=ThreadExecutionBackend({})
    flow = WorkflowEngine(backend)
    flow.backend.link_implicit_data_deps = MagicMock()

    @flow.function_task
    def task1(*args):
        return "task result"

    @flow.function_task
    def task2(*args):
        return "task result"
    
    t1 = task1()
    t2 = task2(t1)
    print(t2.result())

    flow.backend.link_implicit_data_deps.assert_called_once()

def test_explicit_data_dependencies_trigger():
    pass
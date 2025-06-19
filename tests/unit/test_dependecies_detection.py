from radical.flow import WorkflowEngine
from radical.flow import NoopExecutionBackend
from radical.flow.data import InputFile, OutputFile


def test_detect_data_dependencies():
    engine = WorkflowEngine(backend=NoopExecutionBackend())
    a = InputFile("a.txt")
    b = OutputFile("b.txt")

    _, input_deps, output_deps = engine._detect_dependencies([a, b, 42, "string"])

    assert a.filename in input_deps
    assert b.filename in output_deps
    assert 42 not in input_deps
    assert "string" not in input_deps


def test_detect_task_dependencies():
    engine = WorkflowEngine(backend=NoopExecutionBackend())

    @engine.function_task
    def task1():
        return 1
    
    @engine.function_task
    def task2():
        return 2

    task = task2(task1)

    task_deps, _, _ = engine._detect_dependencies([task])

    assert len(task_deps) == 1
    assert task1 in task_deps[0]['args']

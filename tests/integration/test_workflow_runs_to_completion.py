import pytest
import asyncio
import time
from pathlib import Path

from radical.asyncflow import WorkflowEngine, ConcurrentExecutionBackend

from concurrent.futures import ThreadPoolExecutor

@pytest.mark.asyncio
async def test_flow_function_tasks():
    """
    Integration test using `function_task`. Each task updates a shared workflow state,
    which is verified at the end of execution.
    """
    backend = ConcurrentExecutionBackend(ThreadPoolExecutor())
    flow = WorkflowEngine(backend=backend)

    # Shared state is passed and returned explicitly across tasks
    @flow.function_task
    async def task1():
        return {"steps": ["task1"], "value": 1}

    @flow.function_task
    async def task2(state):
        state["steps"].append("task2")
        state["value"] += 2
        return state

    @flow.function_task
    async def task3(state):
        state["steps"].append("task3")
        state["value"] *= 3
        return state

    @flow.function_task
    async def task4(state):
        state["steps"].append("task4")
        state["value"] -= 4
        return state

    @flow.function_task
    async def task5(state):
        state["steps"].append("task5")
        state["value"] = state["value"] ** 2
        return state

    async def run_wf(wf_id):
        """
        Runs a chain of function tasks where each builds upon shared state.
        """
        print(f'\n[WF {wf_id}] Start: {time.time():.2f}')
        s1 = await task1()
        s2 = await task2(s1)
        s3 = await task3(s2)
        s4 = await task4(s3)
        s5 = await task5(s4)

        print(f'[WF {wf_id}] Done: {time.time():.2f} — Final state: {s5}')
        return s5

    try:
        num_workflows = 4
        results = await asyncio.gather(*[run_wf(i) for i in range(num_workflows)])

        # Validation: all steps completed and final value is correct
        for res in results:
            assert res["steps"] == ["task1",
                                    "task2", "task3",
                                    "task4", "task5"]
            assert res["value"] == 25

    finally:
        await flow.shutdown()
        print("Workflow engine shutdown complete.")



@pytest.mark.asyncio
async def test_flow_executable_tasks(tmp_path):
    """
    Integration test using `executable_task`. Each task appends to a workflow-local file.
    Final task output is used to validate execution order.
    """
    backend = ConcurrentExecutionBackend(ThreadPoolExecutor())
    flow = WorkflowEngine(backend=backend)

    # Define executable tasks that append their ID to a shared file
    @flow.executable_task
    async def task1(wf_file):
        return f'echo "task1" >> {wf_file}'

    @flow.executable_task
    async def task2(wf_file, t1):
        return f'echo "task2" >> {wf_file}'

    @flow.executable_task
    async def task3(wf_file, t2):
        return f'echo "task3" >> {wf_file}'

    @flow.executable_task
    async def task4(wf_file, t3):
        return f'echo "task4" >> {wf_file}'

    @flow.executable_task
    async def task5(wf_file, t4):
        return f'echo "task5" >> {wf_file}'

    async def run_wf(wf_id):
        """
        Runs executable tasks that log their execution to a local file.
        """
        wf_file = tmp_path / f"workflow_{wf_id}.log"
        wf_file_path = str(wf_file)

        print(f'\n[WF {wf_id}] Start: {time.time():.2f}')

        t1 = task1(wf_file_path)
        t2 = task2(wf_file_path, t1)
        t3 = task3(wf_file_path, t2)
        t4 = task4(wf_file_path, t3)
        t5 = task5(wf_file_path, t4)

        await t5

        print(f'[WF {wf_id}] Done: {time.time():.2f}')
        return wf_file_path

    try:
        num_workflows = 4
        file_paths = await asyncio.gather(*[run_wf(i) for i in range(num_workflows)])

        for path in file_paths:
            with open(path) as f:
                lines = [line.strip() for line in f.readlines()]
                assert lines == ["task1", "task2", "task3", "task4", "task5"], \
                                 f"Unexpected task sequence in {path}: {lines}"

    finally:
        await flow.shutdown()
        print("Workflow engine shutdown complete.")


@pytest.mark.asyncio
async def test_flow_mixed_function_and_executable_tasks(tmp_path):
    """
    Integration test mixing `function_task` and `executable_task`.
    Function tasks modify state, while executable tasks log their invocation.
    """
    backend = ConcurrentExecutionBackend(ThreadPoolExecutor())
    flow = WorkflowEngine(backend=backend)

    # Function tasks
    @flow.function_task
    async def f_task1():
        return {"steps": ["f_task1"], "value": 1}

    @flow.function_task
    async def f_task2(state):
        state["steps"].append("f_task2")
        state["value"] += 2
        return state

    # Executable tasks
    @flow.executable_task
    async def e_task1(wf_file, state):
        return f'echo "e_task1:{state["value"]}" >> {wf_file}'

    @flow.executable_task
    async def e_task2(wf_file, t1):
        return f'echo "e_task2" >> {wf_file}'

    @flow.function_task
    async def f_task3(state):
        state["steps"].append("f_task3")
        state["value"] *= 3
        return state

    @flow.executable_task
    async def e_task3(wf_file, t2):
        return f'echo "e_task3" >> {wf_file}'

    async def run_wf(wf_id):
        """
        Mixed function and executable tasks within one workflow.
        """
        wf_file = tmp_path / f"wf_{wf_id}.log"
        wf_file_path = str(wf_file)

        print(f'\n[WF {wf_id}] Start: {time.time():.2f}')

        s1 = await f_task1()
        s2 = await f_task2(s1)
        t1 = e_task1(wf_file_path, s2)
        t2 = e_task2(wf_file_path, t1)
        s3 = await f_task3(s2)
        t3 = e_task3(wf_file_path, t2)
        await t3

        print(f'[WF {wf_id}] Done: {time.time():.2f} — Final state: {s3}')
        return s3, wf_file

    try:
        num_workflows = 2
        results = await asyncio.gather(*[run_wf(i) for i in range(num_workflows)])

        for state, log_file in results:
            # Validate function task state
            assert state["steps"] == ["f_task1", "f_task2", "f_task3"]
            assert state["value"] == 9  # (1 + 2) * 3

            # Validate log output
            lines = log_file.read_text().strip().splitlines()
            assert lines[0].startswith("e_task1:3")
            assert lines[1] == "e_task2"
            assert lines[2] == "e_task3"

    finally:
        await flow.shutdown()
        print("Workflow engine shutdown complete.")

"""A cancelled executable task must not leave its process running.

These tests run real commands. A command stopped by mistake only fails its test, but one
left running outlives it, so each sleeps for a short, bounded time.
"""

import asyncio
import os
import sys
import time
from unittest.mock import patch

import pytest

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine

IGNORES_TERM = (
    "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
    "print('ready', flush=True); time.sleep(20)"
)


def _running(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def _wait_for(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.01)


async def _pids(path):
    """The pids that a command has written to `path`, once it has."""
    await _wait_for(lambda: path.exists() and path.read_text().endswith("\n"))
    return [int(pid) for pid in path.read_text().split()]


async def _submit(backend, command, shell=True, uid="task.1"):
    task = {
        "uid": uid,
        "executable": command[0] if not shell else command,
        "arguments": list(command[1:]) if not shell else [],
        "task_backend_specific_kwargs": {"shell": shell},
    }
    await backend.submit_tasks([task])
    return task


@pytest.fixture
async def backend():
    backend = await LocalExecutionBackend()
    yield backend
    await backend.shutdown()


async def test_cancelled_task_stops_its_command(tmp_path):
    """The study's reproduction: nothing runs on after the future says CANCELLED."""
    engine = await WorkflowEngine.create(
        backend=await LocalExecutionBackend(), work_dir=str(tmp_path)
    )
    pids = tmp_path / "pids"
    marker = tmp_path / "marker"

    @engine.executable_task
    async def slow():
        return f"echo $$ > {pids} ; sleep 0.6 ; touch {marker}"

    fut = slow()
    shell = (await _pids(pids))[0]
    assert fut.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fut

    assert not _running(shell)
    await asyncio.sleep(0.8)
    assert not marker.exists()
    await engine.shutdown()


@pytest.mark.parametrize(
    "command",
    [
        "echo $$ > {pids} ; sleep 20 ; true",
        "sleep 20 & echo $$ $! > {pids} ; wait",
        "(sleep 20 ; true) | cat & echo $$ $! > {pids} ; wait",
    ],
    ids=["sequence", "background", "pipeline"],
)
async def test_cancel_reaches_the_children_of_a_shell(backend, tmp_path, command):
    """Killing the shell alone would leave these children running."""
    pids = tmp_path / "pids"
    task = await _submit(backend, command.format(pids=pids))
    started = await _pids(pids)

    assert await backend.cancel_task(task["uid"])

    await _wait_for(lambda: not any(_running(pid) for pid in started))


async def test_cancel_reaches_the_children_of_an_exec_command(backend, tmp_path):
    pids = tmp_path / "pids"
    script = (
        "import os, subprocess, sys, time\n"
        "child = subprocess.Popen(['sleep', '20'])\n"
        f"open({str(pids)!r}, 'w').write(f'{{os.getpid()}} {{child.pid}}\\n')\n"
        "time.sleep(20)\n"
    )
    task = await _submit(backend, [sys.executable, "-c", script], shell=False)
    started = await _pids(pids)

    assert await backend.cancel_task(task["uid"])

    await _wait_for(lambda: not any(_running(pid) for pid in started))


async def test_canceled_is_reported_once_the_process_has_stopped(backend, tmp_path):
    pids = tmp_path / "pids"
    alive_at_report = []
    backend.register_callback(
        lambda task, state: alive_at_report.append((state, _running(shell)))
    )
    task = await _submit(backend, f"echo $$ > {pids} ; sleep 20 ; true")
    shell = (await _pids(pids))[0]

    await backend.cancel_task(task["uid"])

    assert alive_at_report == [("CANCELED", False)]


async def test_a_process_that_ignores_sigterm_is_killed(backend, tmp_path):
    backend.term_grace = 0.3
    pids = tmp_path / "pids"
    task = await _submit(backend, f"trap '' TERM ; echo $$ > {pids} ; sleep 20 ; true")
    shell = (await _pids(pids))[0]

    ticks = 0

    async def heartbeat():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    beating = asyncio.ensure_future(heartbeat())
    started = time.monotonic()
    assert await backend.cancel_task(task["uid"])
    elapsed = time.monotonic() - started
    beating.cancel()

    assert not _running(shell)
    assert elapsed >= 0.3
    # The loop kept running during the grace period
    assert ticks >= 10


async def test_a_stopped_command_leaves_no_zombie(backend, tmp_path):
    pids = tmp_path / "pids"
    task = await _submit(backend, f"echo $$ > {pids} ; sleep 20 ; true")
    shell = (await _pids(pids))[0]

    await backend.cancel_task(task["uid"])

    with pytest.raises(ChildProcessError):
        os.waitpid(shell, os.WNOHANG)


async def test_a_child_cleaning_up_gets_the_grace_period(backend, tmp_path):
    """The shell dies on SIGTERM at once.

    Its child still has its grace period.
    """
    ready = tmp_path / "ready"
    cleaned = tmp_path / "cleaned"
    script = (
        "import pathlib, signal, sys, time\n"
        "def stop(*_):\n"
        "    time.sleep(0.2)\n"
        f"    pathlib.Path({str(cleaned)!r}).touch()\n"
        "    sys.exit()\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        f"pathlib.Path({str(ready)!r}).touch()\n"
        "time.sleep(20)\n"
    )
    command = f'{sys.executable} -c "{script}" > /dev/null 2>&1 & wait'
    task = await _submit(backend, command)
    await _wait_for(ready.exists)

    await backend.cancel_task(task["uid"])

    assert cleaned.exists()


async def test_cancel_does_not_wait_for_a_full_pipe(backend):
    """The output of a cancelled command is not read, and it must not block the stop."""
    backend.term_grace = 0.2
    backend.kill_wait = 5.0
    script = (
        "import signal, sys\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "while True:\n"
        "    sys.stdout.write('x' * 65536)\n"
    )
    task = await _submit(backend, [sys.executable, "-c", script], shell=False)
    await asyncio.sleep(0.3)

    started = time.monotonic()
    assert await backend.cancel_task(task["uid"])

    assert time.monotonic() - started < 2.0


async def test_cancel_ends_when_a_process_left_the_group(backend, tmp_path, caplog):
    """A process in its own session is out of reach, and it holds the output pipes."""
    backend.kill_wait = 0.1
    pids = tmp_path / "pids"
    script = (
        "import os, pathlib, time\n"
        "os.setsid()\n"
        f"pathlib.Path({str(pids)!r}).write_text(str(os.getpid()) + chr(10))\n"
        "time.sleep(1)\n"
    )
    task = await _submit(backend, f'{sys.executable} -c "{script}" & wait')
    escaped = (await _pids(pids))[0]

    with caplog.at_level("WARNING", logger="radical.asyncflow.backends"):
        assert await backend.cancel_task(task["uid"])

    assert _running(escaped)
    assert "did not finish" in caplog.text
    await _wait_for(lambda: not _running(escaped))


async def test_cancel_during_the_start_stops_the_command(backend, tmp_path):
    """Asyncio answers a cancel here by killing the shell, not its children."""
    pids = tmp_path / "pids"
    start = asyncio.create_subprocess_shell
    started = asyncio.Event()

    async def slow_start(*args, **kwargs):
        process = await start(*args, **kwargs)
        await _pids(pids)
        started.set()
        await asyncio.sleep(0.1)
        return process

    with patch("asyncio.create_subprocess_shell", slow_start):
        task = await _submit(backend, f"sleep 20 & echo $$ $! > {pids} ; wait")
        await started.wait()
        assert await backend.cancel_task(task["uid"])

    survivors = [int(pid) for pid in pids.read_text().split()]
    await _wait_for(lambda: not any(_running(pid) for pid in survivors))


async def test_cancel_of_a_command_that_fails_to_start(backend):
    reports = []
    backend.register_callback(lambda task, state: reports.append(state))

    async def failing_start(*args, **kwargs):
        await asyncio.sleep(0.05)
        raise OSError("no such command")

    with patch("asyncio.create_subprocess_shell", failing_start):
        task = await _submit(backend, "true")
        await asyncio.sleep(0)
        assert await backend.cancel_task(task["uid"])

    assert reports == ["CANCELED"]


async def test_repeated_cancel_reports_once(backend):
    backend.term_grace = 0.2
    reports = []
    backend.register_callback(lambda task, state: reports.append(state))
    command = [sys.executable, "-c", IGNORES_TERM]
    task = await _submit(backend, command, shell=False)
    await asyncio.sleep(0.2)

    first = asyncio.ensure_future(backend.cancel_task(task["uid"]))
    await asyncio.sleep(0.05)

    assert await backend.cancel_task(task["uid"])
    assert await first
    assert reports == ["CANCELED"]
    assert not await backend.cancel_task(task["uid"])
    assert reports == ["CANCELED"]


async def test_cancelling_all_tasks_cuts_a_grace_period_short(backend, tmp_path):
    backend.term_grace = 15
    pids = tmp_path / "pids"
    task = await _submit(backend, f"trap '' TERM ; echo $$ > {pids} ; sleep 20 ; true")
    shell = (await _pids(pids))[0]

    cancel = asyncio.ensure_future(backend.cancel_task(task["uid"]))
    await asyncio.sleep(0.05)
    assert _running(shell)

    started = time.monotonic()
    await backend.cancel_all_tasks()
    assert time.monotonic() - started < 5
    assert not _running(shell)
    assert await cancel


async def test_shutdown_stops_running_commands(tmp_path):
    backend = await LocalExecutionBackend()
    pids = tmp_path / "pids"
    await _submit(backend, f"sleep 20 & echo $$ $! > {pids} ; wait")
    started = await _pids(pids)

    await backend.shutdown()

    await _wait_for(lambda: not any(_running(pid) for pid in started))


async def test_a_finished_command_is_not_signalled(backend):
    """A cancel that comes after the command ended must not reach a recycled pid."""
    reports = []
    backend.register_callback(lambda task, state: reports.append(state))
    task = await _submit(backend, "true")
    await _wait_for(lambda: reports)

    with patch("os.killpg") as killpg:
        assert not await backend.cancel_task(task["uid"])

    killpg.assert_not_called()
    assert reports == ["DONE"]

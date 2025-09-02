import asyncio
import logging
import os
import time
import pickle
import dill
import cloudpickle
from typing import Any, Callable, Optional

import typeguard

from ...constants import StateMapper
from .base import BaseExecutionBackend, Session

try:
    import dragon
    import multiprocessing as mp
    from dragon.native.process import Process, ProcessTemplate, Popen
    from dragon.native.process_group import ProcessGroup
    from dragon.native.queue import Queue
    from dragon.infrastructure.facts import PMIBackend
    # Ensure the dragon start method is set once.
    try:
        if mp.get_start_method(allow_none=True) != "dragon":
            mp.set_start_method("dragon", force=True)
    except RuntimeError:
        # Start method already set elsewhere; that's fine.
        pass
except ImportError:  # pragma: no cover - environment without Dragon
    dragon = None
    Process = None
    ProcessTemplate = None
    ProcessGroup = None
    Popen = None
    PMIBackend = None
    Queue = None


logger = logging.getLogger(__name__)


def _function_worker(func_data: bytes, result_queue: Queue, stdout_queue: Queue, stderr_queue: Queue):
    """Worker function to execute user functions in separate Dragon processes."""
    import io
    import sys
    import traceback
    import dill
    import cloudpickle
    
    # Capture stdout/stderr
    old_out, old_err = sys.stdout, sys.stderr
    out_buf, err_buf = io.StringIO(), io.StringIO()

    try:
        sys.stdout, sys.stderr = out_buf, err_buf
        
        # Try multiple deserialization methods
        func_info = None
        deserializers = [dill.loads, cloudpickle.loads, pickle.loads]
        
        for deserializer in deserializers:
            try:
                func_info = deserializer(func_data)
                break
            except Exception as e:
                continue
        
        if func_info is None:
            raise RuntimeError("Could not deserialize function with any available method")
        
        function = func_info['function']
        args = func_info.get('args', ())
        kwargs = func_info.get('kwargs', {})
        
        # Execute function
        if asyncio.iscoroutinefunction(function):
            # For async functions, we need to run them in an event loop
            result = asyncio.run(function(*args, **kwargs))
        else:
            raise RuntimeError('Sync functions are not supported, please define it as async')

        # Send results back
        result_data = {
            'success': True,
            'return_value': result,
            'exception': None,
            'exit_code': 0
        }

    except Exception as e:
        result_data = {
            'success': False,
            'return_value': None,
            'exception': str(e),
            'exit_code': 1,
            'traceback': traceback.format_exc()
        }
    finally:
        # Capture output
        stdout_content = out_buf.getvalue()
        stderr_content = err_buf.getvalue()
        
        sys.stdout, sys.stderr = old_out, old_err

        # Send outputs via queues
        try:
            stdout_queue.put(stdout_content)
            stderr_queue.put(stderr_content)
            result_queue.put(result_data)
        except Exception as queue_error:
            # Fallback - try to send at least the error
            try:
                result_queue.put({
                    'success': False,
                    'return_value': None,
                    'exception': f"Queue error: {queue_error}",
                    'exit_code': 1
                })
            except:
                pass  # Last resort - process will just exit


class DragonExecutionBackend(BaseExecutionBackend):
    """Dragon execution backend for distributed task execution .

    Key design choices:
      - **No multiprocessing.Pool**: avoids known deadlocks/hangs when mixing
        Dragon PMI with asyncio/threads.
      - **Executables** (ranks >= 1): launched via Dragon-native
        :class:`ProcessGroup` / :class:`ProcessTemplate`.
      - **Python functions**: executed in separate Dragon processes using
        :class:`Process` and :class:`Queue` for communication.

    Usage:
        backend = await DragonExecutionBackend(resources)
        # or
        async with DragonExecutionBackend(resources) as backend:
            await backend.submit_tasks(tasks)
    """

    @typeguard.typechecked
    def __init__(self, resources: Optional[dict] = None):
        if dragon is None:
            raise ImportError("Dragon is required for DragonExecutionBackend.")

        self.tasks: dict[str, dict[str, Any]] = {}
        self.session = Session()
        self._callback_func: Callable = None
        self._resources = resources or {}
        self._initialized = False

        # Resource / accounting
        self._slots: int = int(self._resources.get("slots", mp.cpu_count() or 1))
        self._free_slots: int = self._slots
        self._working_dir: str = self._resources.get("working_dir", os.getcwd())

        # Tracking
        self._process_groups: dict[str, ProcessGroup] = {}
        self._running_tasks: dict[str, dict[str, Any]] = {}
        self._function_processes: dict[str, Process] = {}
        self._function_queues: dict[str, dict] = {}

        # Async management
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Create queues for communication with the worker process
        self.result_queue = Queue()
        self.stdout_queue = Queue()
        self.stderr_queue = Queue()

    # --------------------------- lifecycle ---------------------------
    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        if not self._initialized:
            try:
                logger.debug("Starting Dragon backend async initialization ...")
                await self._initialize()
                self._initialized = True
                logger.debug("Dragon backend initialization completed, registering with StateMapper...")
                StateMapper.register_backend_states_with_defaults(backend=self)
                logger.debug("Dragon backend fully initialized")
            except Exception as e:
                logger.exception(f"Dragon backend initialization failed: {e}")
                self._initialized = False
                raise
        return self

    async def _initialize(self) -> None:
        try:
            logger.debug("Initializing Dragon backend (no mp.Pool)...")
            await self._initialize_dragon()

            # Start task monitoring in background
            logger.debug("Starting task monitoring...")
            self._monitor_task = asyncio.create_task(self._monitor_tasks())
            await asyncio.sleep(0.1)

            # Log PMI backend (useful for debugging)
            try:
                #pmi_name = getattr(PMIBackend, "name", lambda: str(PMIBackend))
                #logger.debug(f"Dragon PMI backend: {PMIBackend}")
                pass
            except Exception:
                pass

            logger.info(f"Dragon backend initialized with {self._slots} slots")
        except Exception as e:
            logger.exception(f"Failed to initialize Dragon backend: {str(e)}")
            raise

    async def _initialize_dragon(self):
        """Ensure start method is 'dragon' and proceed."""
        try:
            current_method = mp.get_start_method()
            logger.debug(f"Current multiprocessing start method: {current_method}")
            if current_method != "dragon":
                mp.set_start_method("dragon", force=True)
        except RuntimeError:
            # Already set; that's fine.
            pass
        # No pool in this backend
        logger.debug("Poolless configuration active; using ProcessGroup for executables and Dragon Process for Python functions.")

    # --------------------------- callbacks / state map ---------------------------
    def register_callback(self, callback: Callable) -> None:
        self._callback_func = callback

    def get_task_states_map(self):
        return StateMapper(backend=self)

    # --------------------------- submission API ---------------------------
    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        self._ensure_initialized()

        for task in tasks:
            is_func_task = bool(task.get("function"))
            is_exec_task = bool(task.get("executable"))

            if not is_func_task and not is_exec_task:
                err = ValueError("Task must specify either 'function' or 'executable'")
                task["exception"] = err
                self._callback_func(task, "FAILED")
                continue

            self.tasks[task["uid"]] = task

            try:
                if is_exec_task:
                    await self._submit_executable_task(task)
                else:
                    await self._submit_function_task(task)
            except Exception as e:
                task["exception"] = e
                self._callback_func(task, "FAILED")

    async def _submit_executable_task(self, task: dict[str, Any]) -> None:
        uid = task["uid"]
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))

        # Wait for available slots
        while self._free_slots < ranks:
            logger.debug(f"Waiting for {ranks} slots for task {uid}, {self._free_slots} free")
            await asyncio.sleep(0.1)

        self._free_slots -= ranks

        try:
            if ranks > 1:
                await self._launch_mpi_executable(task)
            else:
                await self._launch_single_executable(task)
        except Exception:
            self._free_slots += ranks
            raise

    async def _launch_mpi_executable(self, task: dict[str, Any]) -> None:
        uid = task["uid"]
        executable = task["executable"]
        args = list(task.get("args", []))
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))

        logger.debug(f"Launching MPI task {uid} with {ranks} ranks")

        group = ProcessGroup(restart=False, policy=None)

        for rank in range(ranks):
            env = os.environ.copy()
            env["DRAGON_RANK"] = str(rank)

            template = ProcessTemplate(
                target=executable,
                args=args,
                env=env,
                cwd=self._working_dir,
                stdout=Popen.PIPE,
                stderr=Popen.PIPE,
                stdin=Popen.DEVNULL,
            )
            group.add_process(nproc=1, template=template)

        group.init()
        group.start()

        self._running_tasks[uid] = {
            "type": "mpi",
            "group": group,
            "ranks": ranks,
            "start_time": time.time(),
        }
        self._callback_func(task, "RUNNING")

    async def _launch_single_executable(self, task: dict[str, Any]) -> None:
        uid = task["uid"]
        executable = task["executable"]
        args = list(task.get("args", []))

        logger.debug(f"Launching single executable task {uid} via ProcessGroup")
        try:
            template = ProcessTemplate(
                target=executable,
                args=args,
                cwd=self._working_dir,
                stdout=Popen.PIPE,
                stderr=Popen.PIPE,
                stdin=Popen.DEVNULL,
                env=os.environ.copy(),
            )

            group = ProcessGroup(restart=False, policy=None)
            group.add_process(nproc=1, template=template)
            group.init()
            group.start()

            self._running_tasks[uid] = {
                "type": "executable",
                "group": group,
                "ranks": 1,
                "start_time": time.time(),
            }
        except Exception as e:
            logger.exception(f"Failed to launch executable {uid}: {e}")
            self._free_slots += 1
            raise

        self._callback_func(task, "RUNNING")

    async def _submit_function_task(self, task: dict[str, Any]) -> None:
        uid = task["uid"]
        function = task["function"]
        args = task.get("args", ())
        kwargs = task.get("kwargs", {})

        # Wait for available slots
        while self._free_slots < 1:
            logger.debug(f"Waiting for slot for function task {uid}")
            await asyncio.sleep(0.1)

        self._free_slots -= 1

        try:
            # Try multiple serialization methods for better compatibility
            func_data = None
            serializers = [
                ('dill', dill.dumps),
                ('cloudpickle', cloudpickle.dumps),
                ('pickle', pickle.dumps)
            ]
            
            serialization_error = None
            for name, serializer in serializers:
                try:
                    func_data = serializer({
                        'function': function,
                        'args': args,
                        'kwargs': kwargs
                    })
                    logger.debug(f"Successfully serialized function with {name}")
                    break
                except Exception as e:
                    logger.debug(f"Failed to serialize with {name}: {e}")
                    serialization_error = e
                    continue
            
            if func_data is None:
                raise RuntimeError(f"Could not serialize function with any available method. Last error: {serialization_error}")

            # Create and start the worker process
            process = Process(
                target=_function_worker,
                args=(func_data, self.result_queue, self.stdout_queue, self.stderr_queue)
            )
            
            # Store process and queues for monitoring
            self._function_processes[uid] = process
            self._function_queues[uid] = {
                'result': self.result_queue,
                'stdout': self.stdout_queue,
                'stderr': self.stderr_queue
            }

            self._running_tasks[uid] = {
                "type": "function",
                "ranks": 1,
                "start_time": time.time(),
                "process": process
            }

            # Start the process
            process.start()
            self._callback_func(task, "RUNNING")
            logger.debug(f"Started Dragon process for function task {uid}")

        except Exception as e:
            logger.exception(f"Failed to launch function task {uid}: {e}")
            self._free_slots += 1
            # Clean up queues if they were created
            if uid in self._function_queues:
                del self._function_queues[uid]
            raise

    # --------------------------- monitoring ---------------------------
    async def _monitor_tasks(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                completed = []
                for uid, ti in list(self._running_tasks.items()):
                    if await self._check_task_completion(uid, ti):
                        completed.append(uid)
                for uid in completed:
                    self._running_tasks.pop(uid, None)
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.exception(f"Error in task monitoring: {e}")
                await asyncio.sleep(1)

    async def _check_task_completion(self, uid: str, task_info: dict) -> bool:
        task = self.tasks.get(uid)
        if not task:
            return True

        t = task_info["type"]
        try:
            if t in ("mpi", "executable"):
                return await self._check_mpi_completion(uid, task_info, task)
            elif t == "function":
                return await self._check_function_completion(uid, task_info, task)
        except Exception as e:
            logger.exception(f"Error checking completion for task {uid}: {e}")
            task["exception"] = str(e)
            task["exit_code"] = 1

            self._callback_func(task, "FAILED")
            self._free_slots += task_info.get("ranks", 1)
            return True
        return False

    async def _check_function_completion(self, uid: str, task_info: dict, task: dict) -> bool:
        """Check if a function task has completed and collect results."""
        process = task_info.get("process")
        if not process:
            return True

        # Check if process is still alive
        if process.is_alive:
            return False

        # Process has finished, collect results
        try:
            queues = self._function_queues.get(uid, {})
            self.result_queue = queues.get('result')
            self.stdout_queue = queues.get('stdout')
            self.stderr_queue = queues.get('stderr')

            # Get results from queues with timeout to avoid hanging
            result_data = None
            stdout_content = ""
            stderr_content = ""

            if self.result_queue:
                try:
                    result_data = self.result_queue.get(timeout=1.0)
                except:
                    logger.warning(f"Timeout getting result for task {uid}")

            if self.stdout_queue:
                try:
                    stdout_content = self.stdout_queue.get(timeout=0.1)
                except:
                    pass  # stdout is optional

            if self.stderr_queue:
                try:
                    stderr_content = self.stderr_queue.get(timeout=0.1)
                except:
                    pass  # stderr is optional

            # Process results
            if result_data and result_data.get('success'):
                task["stdout"] = stdout_content
                task["stderr"] = stderr_content
                task["exit_code"] = result_data.get('exit_code', 0)
                task["return_value"] = result_data.get('return_value')
                task["exception"] = None

                self._callback_func(task, "DONE")
                logger.debug(f"Function task {uid} completed successfully")
            else:
                # Handle failure case
                task["stdout"] = stdout_content
                task["stderr"] = stderr_content + (f"\nException: {result_data.get('exception', 'Unknown error')}" if result_data else "\nProcess failed without result")
                task["exit_code"] = result_data.get('exit_code', 1) if result_data else 1
                task["return_value"] = None
                task["exception"] = result_data.get('exception', 'Process failed') if result_data else 'Process failed without result'

                self._callback_func(task, "FAILED")
                logger.warning(f"Function task {uid} failed")

            # Join the process to clean up
            process.join(timeout=1.0)
            
        except Exception as e:
            logger.exception(f"Error collecting results for function task {uid}: {e}")
            task["exception"] = str(e)
            task["exit_code"] = 1

            self._callback_func(task, "FAILED")

        finally:
            # Clean up resources
            self._free_slots += 1
            self._function_processes.pop(uid, None)
            self._function_queues.pop(uid, None)

        return True

    async def _check_mpi_completion(self, uid: str, task_info: dict, task: dict) -> bool:
        group: ProcessGroup = task_info["group"]

        # Still running
        if not group.inactive_puids:
            return False

        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        exit_codes: list[int] = []

        try:
            for puid, exit_code in group.inactive_puids:
                proc = Process(None, ident=puid)

                if getattr(proc, "stdout_conn", None):
                    try:
                        stdout_parts.append(self._get_stdio(proc.stdout_conn))
                    except Exception:
                        pass

                if getattr(proc, "stderr_conn", None):
                    try:
                        stderr_parts.append(self._get_stdio(proc.stderr_conn))
                    except Exception:
                        pass

                exit_codes.append(exit_code)

            task["stdout"] = "\n".join(stdout_parts)
            task["stderr"] = "\n".join(stderr_parts)
            task["exit_code"] = max(exit_codes) if exit_codes else 0
            task["return_value"] = None
            task["exception"] = None

            if task["exit_code"] == 0:
                self._callback_func(task, "DONE")
            else:
                self._callback_func(task, "FAILED")

            # Clean up
            try:
                group.close()
            except Exception as e:
                logger.debug(f"ProcessGroup close warning for {uid}: {e}")

            self._free_slots += task_info.get("ranks", 1)
            return True

        except Exception as e:
            logger.exception(f"Error collecting results for {uid}: {e}")
            task["exception"] = str(e)
            task["exit_code"] = 1
            self._callback_func(task, "FAILED")
            self._free_slots += task_info.get("ranks", 1)
            return True

    @staticmethod
    def _get_stdio(conn) -> str:
        data = ""
        try:
            while True:
                data += conn.recv()
        except EOFError:
            return data
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # --------------------------- cancellation ---------------------------
    async def cancel_task(self, uid: str) -> bool:
        self._ensure_initialized()
        if uid not in self._running_tasks:
            return False

        ti = self._running_tasks[uid]
        ttype = ti.get("type")
        try:
            if ttype in ("mpi", "executable"):
                group: ProcessGroup = ti.get("group")
                if group and not group.inactive_puids:
                    # Best-effort terminate all processes in the group
                    for puid in getattr(group, "puids", []):
                        try:
                            proc = Process(None, ident=puid)
                            proc.terminate()
                        except Exception as e:
                            logger.warning(f"Failed to terminate process {puid}: {e}")
                    return True
            elif ttype == "function":
                # Terminate the function process
                process = ti.get("process")
                if process and process.is_alive:
                    try:
                        process.terminate()
                        process.join(timeout=2.0)
                        if process.is_alive:
                            process.kill()
                        return True
                    except Exception as e:
                        logger.warning(f"Failed to terminate function process for {uid}: {e}")
        except Exception as e:
            logger.exception(f"Error cancelling task {uid}: {e}")
        return False

    async def cancel_all_tasks(self) -> int:
        self._ensure_initialized()
        cancelled = 0
        for task_uid in list(self._running_tasks.keys()):
            try:
                if await self.cancel_task(task_uid):
                    cancelled += 1
            except Exception:
                pass
        return cancelled

    # --------------------------- misc API ---------------------------
    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        pass

    async def state(self) -> str:
        return "CONNECTED" if self._initialized else "DISCONNECTED"

    async def task_state_cb(self, task: dict, state: str) -> None:
        pass

    async def build_task(self, task: dict) -> None:
        pass

    # --------------------------- shutdown / context ---------------------------
    async def shutdown(self) -> None:
        if not self._initialized:
            return
        try:
            self._shutdown_event.set()
            await self.cancel_all_tasks()

            if self._monitor_task and not self._monitor_task.done():
                try:
                    await asyncio.wait_for(self._monitor_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self._monitor_task.cancel()

            # Close/cleanup process groups
            for uid, ti in list(self._running_tasks.items()):
                if ti.get("type") in ("mpi", "executable"):
                    group = ti.get("group")
                    if group:
                        try:
                            group.close()
                        except Exception as e:
                            logger.warning(f"Error closing process group for {uid}: {e}")

            # Clean up function processes
            for uid, process in list(self._function_processes.items()):
                try:
                    if process.is_alive:
                        process.terminate()
                        process.join(timeout=2.0)
                        if process.is_alive:
                            process.kill()
                except Exception as e:
                    logger.warning(f"Error terminating function process for {uid}: {e}")

            logger.info("Dragon execution backend shutdown complete")
        except Exception as e:
            logger.exception(f"Error during shutdown: {e}")
        finally:
            self.tasks.clear()
            self._running_tasks.clear()
            self._process_groups.clear()
            self._function_processes.clear()
            self._function_queues.clear()
            self._initialized = False

    def _ensure_initialized(self):
        if not self._initialized:
            raise RuntimeError(
                "DragonExecutionBackend must be awaited before use. "
                "Use: backend = await DragonExecutionBackend(resources)"
            )

    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    @classmethod
    async def create(cls, resources: Optional[dict] = None):
        backend = cls(resources)
        return await backend

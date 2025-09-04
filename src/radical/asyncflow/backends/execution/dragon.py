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


def _function_worker(func_data: bytes, result_queue: Queue, stdout_queue: Queue, stderr_queue: Queue, task_uid: str, rank: int = 0):
    """Worker function to execute user functions in separate Dragon processes."""
    import io
    import sys
    import traceback
    import dill
    import cloudpickle
    
    # Set environment variable for rank (useful for MPI-like functions)
    os.environ["DRAGON_RANK"] = str(rank)
    
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
            raise RuntimeError("Could not serialize function with any available method")
        
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
            'exit_code': 0,
            'rank': rank,
            'task_uid': task_uid  # Include task_uid to identify which task this result belongs to
        }

    except Exception as e:
        result_data = {
            'success': False,
            'return_value': None,
            'exception': str(e),
            'exit_code': 1,
            'traceback': traceback.format_exc(),
            'rank': rank,
            'task_uid': task_uid
        }
    finally:
        # Capture output
        stdout_content = out_buf.getvalue()
        stderr_content = err_buf.getvalue()
        
        sys.stdout, sys.stderr = old_out, old_err

        # Send outputs via queues with task_uid for identification
        try:
            stdout_queue.put((task_uid, rank, stdout_content))
            stderr_queue.put((task_uid, rank, stderr_content))
            result_queue.put(result_data)
        except Exception as queue_error:
            # Fallback - try to send at least the error
            try:
                result_queue.put({
                    'success': False,
                    'return_value': None,
                    'exception': f"Queue error: {queue_error}",
                    'exit_code': 1,
                    'rank': rank,
                    'task_uid': task_uid
                })
            except:
                pass  # Last resort - process will just exit


class DragonExecutionBackend(BaseExecutionBackend):
    """Dragon execution backend for distributed task execution.

    Key design choices:
      - **No multiprocessing.Pool**: avoids known deadlocks/hangs when mixing
        Dragon PMI with asyncio/threads.
      - **Single-rank tasks**: launched via Dragon-native :class:`Process`.
      - **Multi-rank tasks**: launched via Dragon-native :class:`ProcessGroup` / :class:`ProcessTemplate`.
      - **MPI tasks**: launched via :class:`ProcessGroup` with MPI-aware environment setup.
      - **Functions and executables**: follow the same execution patterns based on rank count and MPI flag.
      - **Shared queues**: Uses 3 shared queues for all function tasks to reduce resource usage.

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
        self._single_processes: dict[str, Process] = {}

        # Shared queues for all function tasks - OPTIMIZATION
        self._shared_result_queue: Optional[Queue] = None
        self._shared_stdout_queue: Optional[Queue] = None
        self._shared_stderr_queue: Optional[Queue] = None

        # Async management
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

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

            # Initialize shared queues for function tasks - OPTIMIZATION
            logger.debug("Creating shared queues for function tasks...")
            self._shared_result_queue = Queue()
            self._shared_stdout_queue = Queue()
            self._shared_stderr_queue = Queue()
            logger.debug("Shared queues created successfully")

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

            logger.info(f"Dragon backend initialized with {self._slots} slots and shared queues")
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
        logger.debug("Unified execution backend active; using Process for single-rank and ProcessGroup for multi-rank tasks.")

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
                await self._submit_task(task)
            except Exception as e:
                task["exception"] = e
                self._callback_func(task, "FAILED")

    async def _submit_task(self, task: dict[str, Any]) -> None:
        """Unified task submission logic for both functions and executables."""
        uid = task["uid"]
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))
        mpi = backend_kwargs.get("mpi", False)
        is_function = bool(task.get("function"))

        # Wait for available slots
        while self._free_slots < ranks:
            logger.debug(f"Waiting for {ranks} slots for task {uid}, {self._free_slots} free")
            await asyncio.sleep(0.1)

        self._free_slots -= ranks

        try:
            if ranks == 1 and not mpi:
                await self._launch_single_rank_task(task)
            elif mpi:
                await self._launch_mpi_task(task)
            else:  # ranks > 1 and not MPI
                await self._launch_multi_rank_task(task)
        except Exception:
            self._free_slots += ranks
            raise

    async def _launch_single_rank_task(self, task: dict[str, Any]) -> None:
        """Launch single-rank task (function or executable) using Process."""
        uid = task["uid"]
        is_function = bool(task.get("function"))
        
        if is_function:
            await self._launch_single_rank_function(task)
        else:
            await self._launch_single_rank_executable(task)

    async def _launch_single_rank_function(self, task: dict[str, Any]) -> None:
        """Launch single-rank function using Process with shared queues."""
        uid = task["uid"]
        function = task["function"]
        args = task.get("args", ())
        kwargs = task.get("kwargs", {})

        logger.debug(f"Launching single-rank function task {uid} using shared queues")

        try:
            # Serialize function data
            func_data = await self._serialize_function_data(function, args, kwargs)
            
            # Use shared queues instead of creating new ones - OPTIMIZATION
            result_queue = self._shared_result_queue
            stdout_queue = self._shared_stdout_queue
            stderr_queue = self._shared_stderr_queue

            # Create and start the worker process with task_uid for identification
            process = Process(
                target=_function_worker,
                args=(func_data, result_queue, stdout_queue, stderr_queue, uid, 0)  # Added uid parameter
            )

            # Store process for monitoring (no need to store queues anymore)
            self._single_processes[uid] = process

            self._running_tasks[uid] = {
                "type": "single_function",
                "ranks": 1,
                "start_time": time.time(),
                "process": process
            }

            # Start the process
            process.start()
            self._callback_func(task, "RUNNING")
            logger.debug(f"Started single-rank Dragon process for function task {uid}")

        except Exception as e:
            logger.exception(f"Failed to launch single-rank function task {uid}: {e}")
            raise

    async def _launch_single_rank_executable(self, task: dict[str, Any]) -> None:
        """Launch single-rank executable using Process."""
        uid = task["uid"]
        executable = task["executable"]
        args = list(task.get("args", []))

        logger.debug(f"Launching single-rank executable task {uid}")

        try:
            process = Process(
                target=executable,
                args=args,
                cwd=self._working_dir,
                stdout=Popen.PIPE,
                stderr=Popen.PIPE,
                stdin=Popen.DEVNULL,
                env=os.environ.copy(),
            )

            self._single_processes[uid] = process
            self._running_tasks[uid] = {
                "type": "single_executable",
                "ranks": 1,
                "start_time": time.time(),
                "process": process
            }

            # Start the process
            process.start()
            self._callback_func(task, "RUNNING")
            logger.debug(f"Started single-rank Dragon process for executable task {uid}")

        except Exception as e:
            logger.exception(f"Failed to launch single-rank executable task {uid}: {e}")
            raise

    async def _launch_multi_rank_task(self, task: dict[str, Any]) -> None:
        """Launch multi-rank task (function or executable) using ProcessGroup."""
        uid = task["uid"]
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))
        is_function = bool(task.get("function"))

        if is_function:
            await self._launch_multi_rank_function(task)
        else:
            await self._launch_multi_rank_executable(task)

    async def _launch_multi_rank_function(self, task: dict[str, Any]) -> None:
        """Launch multi-rank function using ProcessGroup with shared queues."""
        uid = task["uid"]
        function = task["function"]
        args = task.get("args", ())
        kwargs = task.get("kwargs", {})
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))

        logger.debug(f"Launching multi-rank function task {uid} with {ranks} ranks using shared queues")

        try:
            # Serialize function data
            func_data = await self._serialize_function_data(function, args, kwargs)
            
            # Use shared queues instead of creating new ones - OPTIMIZATION
            result_queue = self._shared_result_queue
            stdout_queue = self._shared_stdout_queue
            stderr_queue = self._shared_stderr_queue

            group = ProcessGroup(restart=False, policy=None)

            for rank in range(ranks):
                env = os.environ.copy()
                env["DRAGON_RANK"] = str(rank)

                template = ProcessTemplate(
                    target=_function_worker,
                    args=(func_data, result_queue, stdout_queue, stderr_queue, uid, rank),  # Added uid parameter
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
                "type": "multi_function",
                "group": group,
                "ranks": ranks,
                "start_time": time.time(),
            }
            self._callback_func(task, "RUNNING")

        except Exception as e:
            logger.exception(f"Failed to launch multi-rank function task {uid}: {e}")
            raise

    async def _launch_multi_rank_executable(self, task: dict[str, Any]) -> None:
        """Launch multi-rank executable using ProcessGroup."""
        uid = task["uid"]
        executable = task["executable"]
        args = list(task.get("args", []))
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))

        logger.debug(f"Launching multi-rank executable task {uid} with {ranks} ranks")

        try:
            group = ProcessGroup(restart=False, policy=None)

            template = ProcessTemplate(
                target=executable,
                args=args,
                cwd=self._working_dir,
                stdout=Popen.PIPE,
                stderr=Popen.PIPE,
                stdin=Popen.DEVNULL,
                env=os.environ.copy(),
            )

            group.add_process(nproc=ranks, template=template)
            group.init()
            group.start()

            self._running_tasks[uid] = {
                "type": "multi_executable",
                "group": group,
                "ranks": ranks,
                "start_time": time.time(),
            }
            self._callback_func(task, "RUNNING")

        except Exception as e:
            logger.exception(f"Failed to launch multi-rank executable task {uid}: {e}")
            raise

    async def _launch_mpi_task(self, task: dict[str, Any]) -> None:
        """Launch MPI task (function or executable) using ProcessGroup with MPI setup."""
        uid = task["uid"]
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))
        is_function = bool(task.get("function"))

        logger.debug(f"Launching MPI task {uid} with {ranks} ranks (function: {is_function})")

        try:
            group = ProcessGroup(restart=False, policy=None)

            if is_function:
                function = task["function"]
                args = task.get("args", ())
                kwargs = task.get("kwargs", {})
                
                # Serialize function data
                func_data = await self._serialize_function_data(function, args, kwargs)
                
                # Use shared queues instead of creating new ones - OPTIMIZATION
                result_queue = self._shared_result_queue
                stdout_queue = self._shared_stdout_queue
                stderr_queue = self._shared_stderr_queue

                for rank in range(ranks):
                    env = os.environ.copy()
                    env["DRAGON_RANK"] = str(rank)

                    template = ProcessTemplate(
                        target=_function_worker,
                        args=(func_data, result_queue, stdout_queue, stderr_queue, uid, rank),  # Added uid parameter
                        env=env,
                        cwd=self._working_dir,
                        stdout=Popen.PIPE,
                        stderr=Popen.PIPE,
                        stdin=Popen.DEVNULL,
                    )
                    group.add_process(nproc=1, template=template)
            else:
                executable = task["executable"]
                args = list(task.get("args", []))

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

            task_type = "mpi_function" if is_function else "mpi_executable"
            self._running_tasks[uid] = {
                "type": task_type,
                "group": group,
                "ranks": ranks,
                "start_time": time.time(),
            }
            self._callback_func(task, "RUNNING")

        except Exception as e:
            logger.exception(f"Failed to launch MPI task {uid}: {e}")
            raise

    async def _serialize_function_data(self, function: Callable, args: tuple, kwargs: dict) -> bytes:
        """Serialize function data using multiple serialization methods."""
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
        
        return func_data

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

        task_type = task_info["type"]
        try:
            if task_type.startswith("single_"):
                return await self._check_single_task_completion(uid, task_info, task)
            elif task_type.startswith(("multi_", "mpi_")):
                return await self._check_group_task_completion(uid, task_info, task)
        except Exception as e:
            logger.exception(f"Error checking completion for task {uid}: {e}")
            task["exception"] = str(e)
            task["exit_code"] = 1
            self._callback_func(task, "FAILED")
            self._free_slots += task_info.get("ranks", 1)
            return True
        return False

    async def _check_single_task_completion(self, uid: str, task_info: dict, task: dict) -> bool:
        """Check completion for single-rank tasks (Process-based)."""
        process = task_info.get("process")
        if not process:
            return True

        # Check if process is still alive
        if process.is_alive:
            return False

        # Process has finished, collect results
        task_type = task_info["type"]
        
        if task_type == "single_function":
            await self._collect_single_function_results(uid, task_info, task)
        else:  # single_executable
            await self._collect_single_executable_results(uid, task_info, task)

        # Clean up
        self._free_slots += 1
        self._single_processes.pop(uid, None)

        return True

    async def _collect_single_function_results(self, uid: str, task_info: dict, task: dict) -> None:
        """Collect results from single-rank function task using shared queues."""
        try:
            # Use shared queues - OPTIMIZATION
            result_queue = self._shared_result_queue
            stdout_queue = self._shared_stdout_queue
            stderr_queue = self._shared_stderr_queue

            # Get results from shared queues - need to filter by task_uid
            result_data = None
            stdout_content = ""
            stderr_content = ""

            # Try to get the result for this specific task
            if result_queue:
                try:
                    # We might need to check multiple results until we find ours
                    temp_results = []
                    found_result = False
                    
                    for _ in range(10):  # Reasonable limit to avoid infinite loop
                        try:
                            data = result_queue.get(timeout=0.1)
                            if data.get('task_uid') == uid:
                                result_data = data
                                found_result = True
                                break
                            else:
                                # This result belongs to another task, put it back
                                temp_results.append(data)
                        except:
                            break
                    
                    # Put back results that don't belong to this task
                    for temp_result in temp_results:
                        try:
                            result_queue.put(temp_result)
                        except:
                            pass
                        
                    if not found_result:
                        logger.warning(f"No result found for task {uid}")
                        
                except Exception as e:
                    logger.warning(f"Error getting result for task {uid}: {e}")

            # Similar approach for stdout and stderr
            if stdout_queue:
                try:
                    temp_stdout = []
                    for _ in range(10):
                        try:
                            task_uid, rank, content = stdout_queue.get(timeout=0.1)
                            if task_uid == uid:
                                stdout_content = content
                                break
                            else:
                                temp_stdout.append((task_uid, rank, content))
                        except:
                            break
                    
                    # Put back stdout that doesn't belong to this task
                    for temp_out in temp_stdout:
                        try:
                            stdout_queue.put(temp_out)
                        except:
                            pass
                except:
                    pass

            if stderr_queue:
                try:
                    temp_stderr = []
                    for _ in range(10):
                        try:
                            task_uid, rank, content = stderr_queue.get(timeout=0.1)
                            if task_uid == uid:
                                stderr_content = content
                                break
                            else:
                                temp_stderr.append((task_uid, rank, content))
                        except:
                            break
                    
                    # Put back stderr that doesn't belong to this task
                    for temp_err in temp_stderr:
                        try:
                            stderr_queue.put(temp_err)
                        except:
                            pass
                except:
                    pass

            # Process results
            if result_data and result_data.get('success'):
                task["stdout"] = stdout_content
                task["stderr"] = stderr_content
                task["exit_code"] = result_data.get('exit_code', 0)
                task["return_value"] = result_data.get('return_value')
                task["exception"] = None

                self._callback_func(task, "DONE")
                logger.debug(f"Single function task {uid} completed successfully")
            else:
                # Handle failure case
                error_msg = result_data.get('exception', 'Unknown error') if result_data else 'Process failed without result'
                task["stdout"] = stdout_content
                task["stderr"] = stderr_content + f"\nException: {error_msg}"
                task["exit_code"] = result_data.get('exit_code', 1) if result_data else 1
                task["return_value"] = None
                task["exception"] = error_msg

                self._callback_func(task, "FAILED")
                logger.warning(f"Single function task {uid} failed: {error_msg}")

            # Join the process to clean up
            process = task_info.get("process")
            if process:
                process.join(timeout=1.0)
            
        except Exception as e:
            logger.exception(f"Error collecting results for single function task {uid}: {e}")
            task["exception"] = str(e)
            task["exit_code"] = 1
            self._callback_func(task, "FAILED")

    async def _collect_single_executable_results(self, uid: str, task_info: dict, task: dict) -> None:
        """Collect results from single-rank executable task."""
        try:
            process = task_info.get("process")
            if not process:
                return

            # Get stdout/stderr if available
            stdout_content = ""
            stderr_content = ""
            
            if hasattr(process, 'stdout_conn') and process.stdout_conn:
                try:
                    stdout_content = self._get_stdio(process.stdout_conn)
                except Exception:
                    pass

            if hasattr(process, 'stderr_conn') and process.stderr_conn:
                try:
                    stderr_content = self._get_stdio(process.stderr_conn)
                except Exception:
                    pass

            # Get exit code
            exit_code = getattr(process, 'exitcode', 0) or 0

            task["stdout"] = stdout_content
            task["stderr"] = stderr_content
            task["exit_code"] = exit_code
            task["return_value"] = None
            task["exception"] = None

            if exit_code == 0:
                self._callback_func(task, "DONE")
                logger.debug(f"Single executable task {uid} completed successfully")
            else:
                self._callback_func(task, "FAILED")
                logger.warning(f"Single executable task {uid} failed with exit code {exit_code}")

            # Join the process to clean up
            process.join(timeout=1.0)

        except Exception as e:
            logger.exception(f"Error collecting results for single executable task {uid}: {e}")
            task["exception"] = str(e)
            task["exit_code"] = 1
            self._callback_func(task, "FAILED")

    async def _check_group_task_completion(self, uid: str, task_info: dict, task: dict) -> bool:
        """Check completion for multi-rank and MPI tasks (ProcessGroup-based)."""
        group: ProcessGroup = task_info.get("group")
        if not group:
            return True

        # Still running
        if not group.inactive_puids:
            return False

        task_type = task_info["type"]
        
        if task_type.endswith("_function"):
            await self._collect_group_function_results(uid, task_info, task)
        else:  # ends with "_executable"
            await self._collect_group_executable_results(uid, task_info, task)

        # Clean up
        try:
            group.close()
        except Exception as e:
            logger.debug(f"ProcessGroup close warning for {uid}: {e}")

        self._free_slots += task_info.get("ranks", 1)

        return True

    async def _collect_group_function_results(self, uid: str, task_info: dict, task: dict) -> None:
        """Collect results from multi-rank or MPI function tasks using shared queues."""
        try:
            # Use shared queues - OPTIMIZATION
            result_queue = self._shared_result_queue
            stdout_queue = self._shared_stdout_queue
            stderr_queue = self._shared_stderr_queue
            ranks = task_info.get("ranks", 1)

            # Collect results from all ranks for this specific task
            results = []
            stdout_parts = {}
            stderr_parts = {}

            # Collect all results with timeout - filter by task_uid
            results_found = 0
            temp_results = []
            
            # Try to collect results for all ranks
            max_attempts = ranks * 3  # Give some extra attempts in case of mixed results
            for _ in range(max_attempts):
                if results_found >= ranks:
                    break
                    
                try:
                    if result_queue:
                        result_data = result_queue.get(timeout=0.1)
                        if result_data.get('task_uid') == uid:
                            results.append(result_data)
                            results_found += 1
                        else:
                            # This result belongs to another task, save it for later
                            temp_results.append(result_data)
                except:
                    break

            # Put back results that don't belong to this task
            for temp_result in temp_results:
                try:
                    result_queue.put(temp_result)
                except:
                    pass

            # Collect stdout from all ranks for this task
            stdout_found = 0
            temp_stdout = []
            for _ in range(max_attempts):
                if stdout_found >= ranks:
                    break
                try:
                    if stdout_queue:
                        task_uid, rank, stdout_content = stdout_queue.get(timeout=0.1)
                        if task_uid == uid:
                            stdout_parts[rank] = stdout_content
                            stdout_found += 1
                        else:
                            temp_stdout.append((task_uid, rank, stdout_content))
                except:
                    break

            # Put back stdout that doesn't belong to this task
            for temp_out in temp_stdout:
                try:
                    stdout_queue.put(temp_out)
                except:
                    pass

            # Collect stderr from all ranks for this task
            stderr_found = 0
            temp_stderr = []
            for _ in range(max_attempts):
                if stderr_found >= ranks:
                    break
                try:
                    if stderr_queue:
                        task_uid, rank, stderr_content = stderr_queue.get(timeout=0.1)
                        if task_uid == uid:
                            stderr_parts[rank] = stderr_content
                            stderr_found += 1
                        else:
                            temp_stderr.append((task_uid, rank, stderr_content))
                except:
                    break

            # Put back stderr that doesn't belong to this task
            for temp_err in temp_stderr:
                try:
                    stderr_queue.put(temp_err)
                except:
                    pass

            # Process results
            all_successful = all(r.get('success', False) for r in results)
            exit_codes = [r.get('exit_code', 1) for r in results]
            max_exit_code = max(exit_codes) if exit_codes else 0

            # Combine stdout/stderr from all ranks
            combined_stdout = "\n".join(stdout_parts.get(i, "") for i in range(ranks))
            combined_stderr = "\n".join(stderr_parts.get(i, "") for i in range(ranks))

            task["stdout"] = combined_stdout
            task["stderr"] = combined_stderr
            task["exit_code"] = max_exit_code
            task["return_value"] = [r.get('return_value') for r in results] if results else None
            task["exception"] = None if all_successful else "; ".join(str(r.get('exception', 'Unknown error')) for r in results if not r.get('success', False))

            if all_successful and max_exit_code == 0:
                self._callback_func(task, "DONE")
                logger.debug(f"Group function task {uid} completed successfully")
            else:
                self._callback_func(task, "FAILED")
                logger.warning(f"Group function task {uid} failed")

        except Exception as e:
            logger.exception(f"Error collecting results for group function task {uid}: {e}")
            task["exception"] = str(e)
            task["exit_code"] = 1
            self._callback_func(task, "FAILED")

    async def _collect_group_executable_results(self, uid: str, task_info: dict, task: dict) -> None:
        """Collect results from multi-rank or MPI executable tasks."""
        try:
            group: ProcessGroup = task_info.get("group")
            if not group:
                return

            stdout_parts: list[str] = []
            stderr_parts: list[str] = []
            exit_codes: list[int] = []

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
                logger.debug(f"Group executable task {uid} completed successfully")
            else:
                self._callback_func(task, "FAILED")
                logger.warning(f"Group executable task {uid} failed with exit code {task['exit_code']}")

        except Exception as e:
            logger.exception(f"Error collecting results for group executable task {uid}: {e}")
            task["exception"] = str(e)
            task["exit_code"] = 1
            self._callback_func(task, "FAILED")

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
        task_type = ti.get("type")
        try:
            if task_type.startswith("single_"):
                # Cancel single-rank task
                process = ti.get("process")
                if process and process.is_alive:
                    try:
                        process.terminate()
                        process.join(timeout=2.0)
                        if process.is_alive:
                            process.kill()
                        return True
                    except Exception as e:
                        logger.warning(f"Failed to terminate single process for {uid}: {e}")
            else:
                # Cancel multi-rank or MPI task
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
                if ti.get("type").startswith(("multi_", "mpi_")):
                    group = ti.get("group")
                    if group:
                        try:
                            group.close()
                        except Exception as e:
                            logger.warning(f"Error closing process group for {uid}: {e}")

            # Clean up single processes
            for uid, process in list(self._single_processes.items()):
                try:
                    if process.is_alive:
                        process.terminate()
                        process.join(timeout=2.0)
                        if process.is_alive:
                            process.kill()
                except Exception as e:
                    logger.warning(f"Error terminating single process for {uid}: {e}")

            # Clean up shared queues - OPTIMIZATION CLEANUP
            try:
                if self._shared_result_queue:
                    # Try to drain remaining items (optional cleanup)
                    try:
                        while True:
                            self._shared_result_queue.get(timeout=0.1)
                    except:
                        pass
                    self._shared_result_queue = None

                if self._shared_stdout_queue:
                    try:
                        while True:
                            self._shared_stdout_queue.get(timeout=0.1)
                    except:
                        pass
                    self._shared_stdout_queue = None

                if self._shared_stderr_queue:
                    try:
                        while True:
                            self._shared_stderr_queue.get(timeout=0.1)
                    except:
                        pass
                    self._shared_stderr_queue = None
                    
                logger.debug("Shared queues cleaned up")
            except Exception as e:
                logger.warning(f"Error cleaning up shared queues: {e}")

            logger.info("Dragon execution backend shutdown complete")
        except Exception as e:
            logger.exception(f"Error during shutdown: {e}")
        finally:
            self.tasks.clear()
            self._running_tasks.clear()
            self._process_groups.clear()
            self._single_processes.clear()
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

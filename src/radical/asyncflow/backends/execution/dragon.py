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
    from dragon.data.ddict.ddict import DDict
    from dragon.native.machine import System
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
    DDict = None
    System = None


logger = logging.getLogger(__name__)


def _function_worker(d: DDict, client_id: int, func_data: bytes, task_uid: str, rank: int = 0):
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

        # Capture output
        stdout_content = out_buf.getvalue()
        stderr_content = err_buf.getvalue()
        
        # Store results in DDict using persistent put
        result_key = f"{task_uid}_rank_{rank}"
        result_data = {
            'success': True,
            'return_value': result,
            'stdout': stdout_content,
            'stderr': stderr_content,
            'exception': None,
            'exit_code': 0,
            'rank': rank,
            'task_uid': task_uid
        }

        # Store in shared DDict using pput for persistence
        d.pput(result_key, result_data)

        # Signal completion using a completion flag
        completion_key = f"{task_uid}_rank_{rank}_completed"
        d.pput(completion_key, True)

    except Exception as e:
        # Capture output even on failure
        stdout_content = out_buf.getvalue()
        stderr_content = err_buf.getvalue()
        
        result_key = f"{task_uid}_rank_{rank}"
        result_data = {
            'success': False,
            'return_value': None,
            'stdout': stdout_content,
            'stderr': stderr_content,
            'exception': str(e),
            'exit_code': 1,
            'traceback': traceback.format_exc(),
            'rank': rank,
            'task_uid': task_uid
        }

        # Store error in shared DDict
        d.pput(result_key, result_data)
        
        # Signal completion even on failure
        completion_key = f"{task_uid}_rank_{rank}_completed"
        d.pput(completion_key, True)
        
    finally:
        # Restore stdout/stderr
        sys.stdout, sys.stderr = old_out, old_err
        
        # Detach from DDict
        try:
            d.detach()
        except Exception:
            pass


class DragonExecutionBackend(BaseExecutionBackend):
    """Dragon execution backend for distributed task execution with DDict integration.

    Key design choices:
      - **DDict for results**: All task results stored in distributed dictionary
      - **Proper system allocation**: Uses System() for proper node allocation
      - **Cross-node sharing**: DDict automatically handles cross-node data sharing
      - **Persistent keys**: Uses pput for reliable data storage
    """

    @typeguard.typechecked
    def __init__(self, resources: Optional[dict] = None):
        if dragon is None:
            raise ImportError("Dragon is required for DragonExecutionBackend.")
        if DDict is None:
            raise ImportError("Dragon DDict is required for this backend version.")
        if System is None:
            raise ImportError("Dragon System is required for this backend version.")

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
        self._single_processes: dict[str, Process] = {}
        self._process_groups: dict[str, ProcessGroup] = {}
        self._running_tasks: dict[str, dict[str, Any]] = {}

        # DDict and system allocation
        self._ddict_manager: Optional[DDict] = None
        self._system_alloc: Optional[System] = None

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
            logger.debug("Initializing Dragon backend with DDict...")
            await self._initialize_dragon()

            # Initialize system allocation first - CRITICAL
            logger.debug("Creating System allocation...")
            self._system_alloc = System()
            nnodes = self._system_alloc.nnodes
            logger.debug(f"System allocation created with {nnodes} nodes")

            # Initialize DDict with proper parameters from the example
            logger.debug("Creating DDict with proper parameters...")
            self._ddict_manager = DDict(
                n_nodes=nnodes,
                total_mem=nnodes * int(4 * 1024 * 1024 * 1024),  # 4GB per node
                wait_for_keys=True,  # Block until keys are available
                working_set_size=4,
                timeout=200
            )
            logger.debug("DDict created successfully")

            # Start task monitoring in background
            logger.debug("Starting task monitoring...")
            self._monitor_task = asyncio.create_task(self._monitor_tasks())
            await asyncio.sleep(0.1)

            logger.info(f"Dragon backend initialized with {self._slots} slots and DDict on {nnodes} nodes")
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
        logger.debug("Dragon backend active with DDict integration.")

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
        """Launch single-rank function using Process with DDict."""
        uid = task["uid"]
        function = task["function"]
        args = task.get("args", ())
        kwargs = task.get("kwargs", {})

        logger.debug(f"Launching single-rank function task {uid} using DDict")

        try:
            # Serialize function data
            func_data = await self._serialize_function_data(function, args, kwargs)
            
            # Pass DDict reference and client ID to worker
            ddict_manager = self._ddict_manager
            client_id = 0  # Single rank uses client_id 0

            # Create and start the worker process
            process = Process(
                target=_function_worker,
                args=(ddict_manager, client_id, func_data, uid, 0)
            )

            # Store process for monitoring
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
        is_function = bool(task.get("function"))

        if is_function:
            await self._launch_multi_rank_function(task)
        else:
            await self._launch_multi_rank_executable(task)

    async def _launch_multi_rank_function(self, task: dict[str, Any]) -> None:
        """Launch multi-rank function using ProcessGroup with DDict."""
        uid = task["uid"]
        function = task["function"]
        args = task.get("args", ())
        kwargs = task.get("kwargs", {})
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))

        logger.debug(f"Launching multi-rank function task {uid} with {ranks} ranks using DDict")

        try:
            # Serialize function data
            func_data = await self._serialize_function_data(function, args, kwargs)
            
            # Use DDict
            ddict_manager = self._ddict_manager

            group = ProcessGroup(restart=False, policy=None)

            for rank in range(ranks):
                env = os.environ.copy()
                env["DRAGON_RANK"] = str(rank)

                template = ProcessTemplate(
                    target=_function_worker,
                    args=(ddict_manager, rank, func_data, uid, rank),
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
                
                # Use DDict
                ddict_manager = self._ddict_manager

                for rank in range(ranks):
                    env = os.environ.copy()
                    env["DRAGON_RANK"] = str(rank)

                    template = ProcessTemplate(
                        target=_function_worker,
                        args=(ddict_manager, rank, func_data, uid, rank),  # Added uid parameter
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
            await self._collect_single_function_results_from_ddict(uid, task_info, task)
        else:  # single_executable
            await self._collect_single_executable_results(uid, task_info, task)

        # Clean up
        self._free_slots += 1
        self._single_processes.pop(uid, None)

        return True

    # ... [rest of the methods remain largely the same, but with key changes] ...

    async def _collect_single_function_results_from_ddict(self, uid: str, task_info: dict, task: dict) -> None:
        """Collect results from single-rank function task using DDict."""
        try:
            # Wait for completion flag
            completion_key = f"{uid}_rank_0_completed"
            max_wait = 30  # 30 seconds timeout
            wait_count = 0
            
            while wait_count < max_wait * 10:  # Check every 0.1 seconds
                try:
                    if completion_key in self._ddict_manager:
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)
                wait_count += 1
            
            # Get results from DDict
            result_key = f"{uid}_rank_0"
            result_data = None
            
            try:
                if result_key in self._ddict_manager:
                    result_data = self._ddict_manager[result_key]
            except Exception as e:
                logger.warning(f"Error reading from DDict for task {uid}: {e}")

            # Process results
            if result_data and result_data.get('success'):
                task["stdout"] = result_data.get('stdout', '')
                task["stderr"] = result_data.get('stderr', '')
                task["exit_code"] = result_data.get('exit_code', 0)
                task["return_value"] = result_data.get('return_value')
                task["exception"] = None

                self._callback_func(task, "DONE")
                logger.debug(f"Single function task {uid} completed successfully")
            else:
                # Handle failure case
                error_msg = result_data.get('exception', 'Unknown error') if result_data else 'Process failed without result'
                task["stdout"] = result_data.get('stdout', '') if result_data else ''
                task["stderr"] = (result_data.get('stderr', '') + f"\nException: {error_msg}") if result_data else f"Exception: {error_msg}"
                task["exit_code"] = result_data.get('exit_code', 1) if result_data else 1
                task["return_value"] = None
                task["exception"] = error_msg

                self._callback_func(task, "FAILED")
                logger.warning(f"Single function task {uid} failed: {error_msg}")

            # Clean up DDict entries (they're persistent, so we should clean them)
            try:
                if result_key in self._ddict_manager:
                    del self._ddict_manager[result_key]
                if completion_key in self._ddict_manager:
                    del self._ddict_manager[completion_key]
            except Exception:
                pass

            # Join the process to clean up
            process = task_info.get("process")
            if process:
                try:
                    process.join(timeout=1.0)
                except Exception:
                    pass
            
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
            try:
                process.join(timeout=1.0)
            except Exception:
                pass

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

        # Still running if no inactive processes yet
        if not group.inactive_puids:
            return False

        task_type = task_info["type"]
        
        if task_type.endswith("_function"):
            await self._collect_group_function_results_from_ddict(uid, task_info, task)
        else:  # ends with "_executable"
            await self._collect_group_executable_results(uid, task_info, task)

        # Clean up
        try:
            group.close()
        except Exception as e:
            logger.debug(f"ProcessGroup close warning for {uid}: {e}")

        self._free_slots += task_info.get("ranks", 1)

        return True

    async def _collect_group_function_results_from_ddict(self, uid: str, task_info: dict, task: dict) -> None:
        """Collect results from multi-rank or MPI function tasks using DDict."""
        try:
            ranks = task_info.get("ranks", 1)

            # Wait for completion flags from all ranks
            completion_keys = [f"{uid}_rank_{rank}_completed" for rank in range(ranks)]
            max_wait = 30  # 30 seconds timeout
            wait_count = 0
            
            while wait_count < max_wait * 10:  # Check every 0.1 seconds
                completed_count = 0
                for completion_key in completion_keys:
                    try:
                        if completion_key in self._ddict_manager:
                            completed_count += 1
                    except Exception:
                        pass
                
                if completed_count >= ranks:
                    break
                    
                await asyncio.sleep(0.1)
                wait_count += 1

            # Collect results from DDict for all ranks
            results = []
            stdout_parts = {}
            stderr_parts = {}

            for rank in range(ranks):
                result_key = f"{uid}_rank_{rank}"
                try:
                    if result_key in self._ddict_manager:
                        result_data = self._ddict_manager[result_key]
                        results.append(result_data)
                        stdout_parts[rank] = result_data.get('stdout', '')
                        stderr_parts[rank] = result_data.get('stderr', '')
                    else:
                        # Missing result - create a failure entry
                        results.append({
                            'success': False,
                            'exception': 'No result found in DDict',
                            'exit_code': 1,
                            'rank': rank
                        })
                        stdout_parts[rank] = ""
                        stderr_parts[rank] = "No result found in DDict"
                except Exception as e:
                    logger.warning(f"Error reading DDict result for {uid} rank {rank}: {e}")
                    results.append({
                        'success': False,
                        'exception': f'DDict read error: {e}',
                        'exit_code': 1,
                        'rank': rank
                    })
                    stdout_parts[rank] = ""
                    stderr_parts[rank] = f"DDict read error: {e}"

            # Process results
            all_successful = all(r.get('success', False) for r in results)
            exit_codes = [r.get('exit_code', 1) for r in results]
            max_exit_code = max(exit_codes) if exit_codes else 0

            # Combine stdout/stderr from all ranks
            combined_stdout = "\n".join(f"Rank {i}: {stdout_parts.get(i, '')}" for i in range(ranks))
            combined_stderr = "\n".join(f"Rank {i}: {stderr_parts.get(i, '')}" for i in range(ranks))

            task["stdout"] = combined_stdout
            task["stderr"] = combined_stderr
            task["exit_code"] = max_exit_code
            task["return_value"] = [r.get('return_value') for r in results] if results else None
            task["exception"] = None if all_successful else "; ".join(str(r.get('exception', 'Unknown error')) for r in results if not r.get('success', False))

            # Clean up DDict entries for all ranks
            try:
                for rank in range(ranks):
                    result_key = f"{uid}_rank_{rank}"
                    completion_key = f"{uid}_rank_{rank}_completed"
                    if result_key in self._ddict_manager:
                        del self._ddict_manager[result_key]
                    if completion_key in self._ddict_manager:
                        del self._ddict_manager[completion_key]
            except Exception:
                pass

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
        
        # Clean up any DDict entries for cancelled tasks
        try:
            ranks = ti.get("ranks", 1)
            for rank in range(ranks):
                result_key = f"{uid}_rank_{rank}"
                completion_key = f"{uid}_rank_{rank}_completed"
                if result_key in self._ddict_manager:
                    del self._ddict_manager[result_key]
                if completion_key in self._ddict_manager:
                    del self._ddict_manager[completion_key]
        except Exception as e:
            logger.warning(f"Error cleaning up DDict for cancelled task {uid}: {e}")
            
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
        """Link explicit data dependencies between tasks - placeholder implementation."""
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        """Link implicit data dependencies between tasks - placeholder implementation."""
        pass

    async def state(self) -> str:
        return "CONNECTED" if self._initialized else "DISCONNECTED"

    async def task_state_cb(self, task: dict, state: str) -> None:
        """Task state callback - placeholder implementation."""
        pass

    async def build_task(self, task: dict) -> None:
        """Build task - placeholder implementation."""
        pass

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

            # Clean up DDict first
            try:
                if self._ddict_manager:
                    # Clear any remaining task results
                    self._ddict_manager.clear()
                    # Destroy the DDict properly
                    self._ddict_manager.destroy()
                    self._ddict_manager = None
                logger.debug("DDict cleaned up and destroyed")
            except Exception as e:
                logger.warning(f"Error cleaning up DDict: {e}")

            # Clean up system allocation
            try:
                if self._system_alloc:
                    self._system_alloc = None
                logger.debug("System allocation cleaned up")
            except Exception as e:
                logger.warning(f"Error cleaning up system allocation: {e}")

            logger.info("Dragon execution backend with DDict shutdown complete")
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

    # --------------------------- data sharing utilities ---------------------------
    def get_ddict(self) -> DDict:
        """Get the shared DDict for cross-task data sharing."""
        self._ensure_initialized()
        return self._ddict_manager

    def store_task_data(self, key: str, data: Any) -> None:
        """Store data in shared DDict for cross-task access."""
        self._ensure_initialized()
        self._ddict_manager.pput(key, data)

    def get_task_data(self, key: str, default=None) -> Any:
        """Retrieve data from shared DDict."""
        self._ensure_initialized()
        try:
            if key in self._ddict_manager:
                return self._ddict_manager[key]
            return default
        except Exception:
            return default

    def list_task_data_keys(self) -> list:
        """List all keys in the shared DDict."""
        self._ensure_initialized()
        try:
            return list(self._ddict_manager.keys())
        except Exception:
            return []

    def clear_task_data(self, key: str = None) -> None:
        """Clear specific key or all data from shared DDict."""
        self._ensure_initialized()
        try:
            if key:
                if key in self._ddict_manager:
                    del self._ddict_manager[key]
            else:
                self._ddict_manager.clear()
        except Exception as e:
            logger.warning(f"Error clearing DDict data: {e}")

    # --------------------------- additional completion methods ---------------------------
    async def wait_for_tasks(self, task_uids: list[str] = None, timeout: float = None) -> dict:
        """Wait for specific tasks to complete or all running tasks if no UIDs specified."""
        self._ensure_initialized()
        
        if task_uids is None:
            task_uids = list(self._running_tasks.keys())
        
        start_time = time.time()
        completed = {}
        
        while task_uids:
            if timeout and (time.time() - start_time) > timeout:
                break
                
            still_running = []
            for uid in task_uids:
                if uid not in self._running_tasks:
                    # Task completed
                    task = self.tasks.get(uid)
                    completed[uid] = task
                else:
                    still_running.append(uid)
            
            task_uids = still_running
            if not task_uids:
                break
                
            await asyncio.sleep(0.1)
        
        return completed
    
    async def get_task_status(self, uid: str) -> dict:
        """Get detailed status information for a specific task."""
        self._ensure_initialized()
        
        task = self.tasks.get(uid)
        if not task:
            return {"status": "NOT_FOUND", "message": f"Task {uid} not found"}
        
        if uid in self._running_tasks:
            task_info = self._running_tasks[uid]
            runtime = time.time() - task_info.get("start_time", time.time())
            return {
                "status": "RUNNING",
                "uid": uid,
                "type": task_info.get("type"),
                "ranks": task_info.get("ranks", 1),
                "runtime": runtime,
                "start_time": task_info.get("start_time")
            }
        else:
            # Task completed or failed
            exit_code = task.get("exit_code", 0)
            exception = task.get("exception")
            
            if exception:
                status = "FAILED"
            elif exit_code == 0:
                status = "COMPLETED"
            else:
                status = "FAILED"
            
            return {
                "status": status,
                "uid": uid,
                "exit_code": exit_code,
                "exception": str(exception) if exception else None,
                "return_value": task.get("return_value"),
                "stdout": task.get("stdout", ""),
                "stderr": task.get("stderr", "")
            }
    
    def get_running_tasks(self) -> dict:
        """Get information about all currently running tasks."""
        self._ensure_initialized()
        return {uid: {
            "type": info.get("type"),
            "ranks": info.get("ranks", 1),
            "runtime": time.time() - info.get("start_time", time.time()),
            "start_time": info.get("start_time")
        } for uid, info in self._running_tasks.items()}
    
    def get_resource_usage(self) -> dict:
        """Get current resource usage information."""
        self._ensure_initialized()
        return {
            "total_slots": self._slots,
            "free_slots": self._free_slots,
            "used_slots": self._slots - self._free_slots,
            "running_tasks_count": len(self._running_tasks),
            "total_tasks_submitted": len(self.tasks),
            "ddict_initialized": self._ddict_manager is not None,
            "system_nodes": getattr(self._system_alloc, 'nnodes', 0) if self._system_alloc else 0
        }
    
    # --------------------------- enhanced error handling ---------------------------
    def _handle_task_error(self, task: dict, error: Exception, context: str = "") -> None:
        """Centralized error handling for tasks."""
        error_msg = f"{context}: {str(error)}" if context else str(error)
        
        task["exception"] = error_msg
        task["exit_code"] = getattr(error, 'exit_code', 1)
        task["stderr"] = task.get("stderr", "") + f"\nError: {error_msg}"
        
        logger.error(f"Task {task.get('uid', 'unknown')} failed: {error_msg}")
        
        if self._callback_func:
            try:
                self._callback_func(task, "FAILED")
            except Exception as callback_error:
                logger.error(f"Callback error for task {task.get('uid', 'unknown')}: {callback_error}")
    
    def _validate_task(self, task: dict) -> tuple[bool, str]:
        """Validate task configuration before submission."""
        uid = task.get("uid")
        if not uid:
            return False, "Task must have a 'uid' field"
        
        function = task.get("function")
        executable = task.get("executable")
        
        if not function and not executable:
            return False, "Task must specify either 'function' or 'executable'"
        
        if function and executable:
            return False, "Task cannot specify both 'function' and 'executable'"
        
        if function and not callable(function):
            return False, "Task 'function' must be callable"
        
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = backend_kwargs.get("ranks", 1)
        
        try:
            ranks = int(ranks)
            if ranks < 1:
                return False, "Task 'ranks' must be >= 1"
        except (ValueError, TypeError):
            return False, "Task 'ranks' must be a valid integer"
        
        return True, ""
    
    # --------------------------- checkpoint and synchronization ---------------------------
    async def checkpoint_tasks(self, task_uids: list[str] = None) -> dict:
        """Create a checkpoint for running tasks (useful for fault tolerance)."""
        self._ensure_initialized()
        
        if not self._ddict_manager:
            raise RuntimeError("DDict not available for checkpointing")
        
        checkpoint_data = {
            "timestamp": time.time(),
            "running_tasks": {},
            "resource_usage": self.get_resource_usage()
        }
        
        target_tasks = task_uids or list(self._running_tasks.keys())
        
        for uid in target_tasks:
            if uid in self._running_tasks:
                task_info = self._running_tasks[uid]
                checkpoint_data["running_tasks"][uid] = {
                    "type": task_info.get("type"),
                    "ranks": task_info.get("ranks"),
                    "start_time": task_info.get("start_time"),
                    "runtime": time.time() - task_info.get("start_time", time.time())
                }
        
        # Store checkpoint in DDict
        checkpoint_key = f"backend_checkpoint_{int(time.time())}"
        try:
            self._ddict_manager.pput(checkpoint_key, checkpoint_data)
            logger.info(f"Created checkpoint {checkpoint_key} for {len(target_tasks)} tasks")
            return {"checkpoint_key": checkpoint_key, "task_count": len(target_tasks)}
        except Exception as e:
            logger.error(f"Failed to create checkpoint: {e}")
            raise
    
    async def sync_ddict_checkpoint(self) -> None:
        """Synchronize to the newest DDict checkpoint."""
        self._ensure_initialized()
        
        if self._ddict_manager and hasattr(self._ddict_manager, 'sync_to_newest_checkpoint'):
            try:
                self._ddict_manager.sync_to_newest_checkpoint()
                logger.debug("Synced to newest DDict checkpoint")
            except Exception as e:
                logger.warning(f"Failed to sync to newest checkpoint: {e}")
    
    def get_ddict_stats(self) -> dict:
        """Get DDict statistics if available."""
        self._ensure_initialized()
        
        if not self._ddict_manager:
            return {"error": "DDict not initialized"}
        
        try:
            if hasattr(self._ddict_manager, 'stats'):
                return self._ddict_manager.stats
            else:
                return {"message": "DDict stats not available"}
        except Exception as e:
            return {"error": f"Failed to get DDict stats: {e}"}
    
    # --------------------------- debugging and diagnostics ---------------------------
    async def diagnose_task(self, uid: str) -> dict:
        """Provide diagnostic information for a specific task."""
        self._ensure_initialized()
        
        diagnosis = {
            "task_uid": uid,
            "timestamp": time.time(),
            "backend_state": await self.state(),
            "task_found": uid in self.tasks,
            "task_running": uid in self._running_tasks,
            "ddict_available": self._ddict_manager is not None
        }
        
        if uid in self.tasks:
            task = self.tasks[uid]
            diagnosis["task_info"] = {
                "has_function": "function" in task,
                "has_executable": "executable" in task,
                "has_args": "args" in task,
                "has_kwargs": "kwargs" in task,
                "backend_kwargs": task.get('task_backend_specific_kwargs', {}),
                "current_state": {
                    "stdout": len(task.get("stdout", "")),
                    "stderr": len(task.get("stderr", "")),
                    "exit_code": task.get("exit_code"),
                    "exception": str(task.get("exception")) if task.get("exception") else None,
                    "return_value_type": type(task.get("return_value")).__name__ if "return_value" in task else None
                }
            }
        
        if uid in self._running_tasks:
            task_info = self._running_tasks[uid]
            diagnosis["runtime_info"] = {
                "type": task_info.get("type"),
                "ranks": task_info.get("ranks"),
                "runtime": time.time() - task_info.get("start_time", time.time()),
                "process_alive": None,
                "group_status": None
            }
            
            # Check process/group status
            if task_info.get("type", "").startswith("single_"):
                process = task_info.get("process")
                if process:
                    diagnosis["runtime_info"]["process_alive"] = process.is_alive
            else:
                group = task_info.get("group")
                if group:
                    diagnosis["runtime_info"]["group_status"] = {
                        "inactive_puids_count": len(getattr(group, 'inactive_puids', [])),
                        "has_puids": hasattr(group, 'puids')
                    }
        
        # Check DDict for task-related keys
        if self._ddict_manager:
            diagnosis["ddict_keys"] = {}
            try:
                ranks = 1
                if uid in self._running_tasks:
                    ranks = self._running_tasks[uid].get("ranks", 1)
                elif uid in self.tasks:
                    backend_kwargs = self.tasks[uid].get('task_backend_specific_kwargs', {})
                    ranks = int(backend_kwargs.get("ranks", 1))
                
                for rank in range(ranks):
                    result_key = f"{uid}_rank_{rank}"
                    completion_key = f"{uid}_rank_{rank}_completed"
                    diagnosis["ddict_keys"][f"rank_{rank}"] = {
                        "result_key_exists": result_key in self._ddict_manager,
                        "completion_key_exists": completion_key in self._ddict_manager
                    }
            except Exception as e:
                diagnosis["ddict_keys"]["error"] = str(e)
        
        return diagnosis
    
    def get_backend_diagnostics(self) -> dict:
        """Get comprehensive backend diagnostic information."""
        self._ensure_initialized()
        
        return {
            "backend_type": "DragonExecutionBackend",
            "initialized": self._initialized,
            "system_info": {
                "nodes": getattr(self._system_alloc, 'nnodes', 0) if self._system_alloc else 0,
                "working_dir": self._working_dir
            },
            "resource_usage": self.get_resource_usage(),
            "task_counts": {
                "total_submitted": len(self.tasks),
                "currently_running": len(self._running_tasks),
                "single_processes": len(self._single_processes),
                "process_groups": len(self._process_groups)
            },
            "ddict_info": {
                "initialized": self._ddict_manager is not None,
                "stats": self.get_ddict_stats()
            },
            "monitor_task_running": self._monitor_task is not None and not self._monitor_task.done() if self._monitor_task else False,
            "shutdown_event_set": self._shutdown_event.is_set()
        }

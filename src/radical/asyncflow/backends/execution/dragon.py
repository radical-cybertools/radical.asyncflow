import asyncio
import logging
import os
import time
import sys
import uuid
from typing import Any, Callable, Optional, Dict, List, Tuple
from enum import Enum
from dataclasses import dataclass

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
    from dragon.native.process_group import DragonUserCodeError
except ImportError:  # pragma: no cover - environment without Dragon
    dragon = None
    Process = None
    ProcessTemplate = None
    ProcessGroup = None
    Popen = None
    Queue = None
    DDict = None
    System = None

logger = logging.getLogger(__name__)

class TaskType(Enum):
    """Enumeration of supported task types."""
    SINGLE_FUNCTION = "single_function"
    SINGLE_EXECUTABLE = "single_executable"
    MULTI_FUNCTION = "multi_function"
    MULTI_EXECUTABLE = "multi_executable"
    MPI_FUNCTION = "mpi_function"
    MPI_EXECUTABLE = "mpi_executable"

@dataclass
class TaskInfo:
    """Container for task runtime information."""
    task_type: TaskType
    ranks: int
    start_time: float
    canceled: bool = False
    process: Optional[Process] = None
    group: Optional[ProcessGroup] = None

@dataclass
class ExecutableTaskCompletion:
    """Task completion data from executable process."""
    task_uid: str
    rank: int
    process_id: int
    stdout: str
    stderr: str
    exit_code: int
    timestamp: float
    
    def to_result_dict(self) -> dict:
        """Convert to task result dictionary."""
        return {
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.exit_code,
            "return_value": None,
            "exception": None if self.exit_code == 0 else f"Process exited with code {self.exit_code}"
        }

@dataclass
class FunctionTaskCompletion:
    """Task completion data from function process - sent via queue."""
    task_uid: str
    rank: int
    process_id: int
    stdout: str
    stderr: str
    exit_code: int
    timestamp: float
    success: bool
    exception: Optional[str]
    traceback: Optional[str]
    return_value: Any  # Actual value or DataReference
    stored_in_ddict: bool  # True if return_value is in DDict
    
    def to_result_dict(self) -> dict:
        """Convert to task result dictionary."""
        return {
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": self.exit_code,
            "return_value": self.return_value,
            "exception": self.exception,
            "success": self.success
        }

class DataReference:
    """Zero-copy reference to function results stored in DDict.
    
    Points directly to result keys written by function wrapper.
    User controls when to resolve and fetch data from DDict.
    """

    def __init__(self, task_uid: str, ranks: int, ddict: DDict, backend_id: str):
        self._task_uid = task_uid
        self._ranks = ranks
        self._ddict = ddict
        self._backend_id = backend_id

    @property
    def task_uid(self) -> str:
        return self._task_uid
    
    @property
    def ranks(self) -> int:
        return self._ranks

    @property
    def backend_id(self) -> str:
        return self._backend_id

    def resolve(self) -> Any:
        """Resolve reference to actual return values from DDict.
        
        Fetches data directly from function-wrapper-written keys in DDict:
        - Single rank (ranks=1): Returns single return_value
        - Multi-rank (ranks>1): Returns list of return_values indexed by rank
        """
        if self._ranks == 1:
            # Single rank - return single value
            result_key = f"return_{self._task_uid}_rank_0"
            if result_key not in self._ddict:
                raise KeyError(f"Result data not found for task: {self._task_uid}")
            
            return self._ddict[result_key]
        else:
            # Multi-rank - return list of values
            return_values = []
            for rank in range(self._ranks):
                result_key = f"return_{self._task_uid}_rank_{rank}"
                if result_key not in self._ddict:
                    raise KeyError(f"Result data not found for task {self._task_uid} rank {rank}")
                
                return_values.append(self._ddict[result_key])
            
            return return_values

    def __repr__(self) -> str:
        return f"DataReference(task_uid='{self._task_uid}', ranks={self._ranks}, backend_id='{self._backend_id}')"

class SharedMemoryManager:
    """Manages optional DDict storage for large function results."""

    def __init__(self, ddict: DDict, system: System, logger: logging.Logger):
        self.ddict = ddict
        self.system = system
        self.logger = logger
        self.backend_id = f"dragon_{uuid.uuid4().hex[:8]}"

    async def initialize(self):
        """Initialize the storage manager."""
        self.logger.debug(f"SharedMemoryManager initialized with optional DDict storage")

    def create_data_reference(self, task_uid: str, ranks: int) -> DataReference:
        """Create a zero-copy reference to existing result keys in DDict.
        
        Creates a reference object that points to
        keys: return_{task_uid}_rank_{i}
        """
        return DataReference(task_uid, ranks, self.ddict, self.backend_id)

    def cleanup_reference(self, ref: DataReference):
        """Clean up reference data from DDict."""
        try:
            for rank in range(ref.ranks):
                result_key = f"return_{ref.task_uid}_rank_{rank}"
                if result_key in self.ddict:
                    del self.ddict[result_key]
        except Exception as e:
            self.logger.warning(f"Error cleaning up reference {ref.task_uid}: {e}")

    # DDict operations for direct task data sharing
    def store_task_data(self, key: str, data: Any) -> None:
        """Store data in shared DDict."""
        self.ddict.pput(key, data)

    def get_task_data(self, key: str, default=None) -> Any:
        """Retrieve data from shared DDict."""
        try:
            if key in self.ddict:
                return self.ddict[key]
            return default
        except Exception:
            return default

    def list_task_data_keys(self) -> list:
        """List all keys in the shared DDict."""
        try:
            return list(self.ddict.keys())
        except Exception:
            return []

    def clear_task_data(self, key: str = None) -> None:
        """Clear specific key or all data from shared DDict."""
        try:
            if key:
                if key in self.ddict:
                    del self.ddict[key]
            else:
                self.ddict.clear()
        except Exception as e:
            self.logger.warning(f"Error clearing DDict data: {e}")

class ResultCollector:
    """Unified queue-based result collection for both executable and function tasks."""

    def __init__(self, shared_memory_manager: SharedMemoryManager, result_queue: Queue, logger: logging.Logger):
        self.shared_memory = shared_memory_manager
        self.ddict = shared_memory_manager.ddict
        self.result_queue = result_queue
        self.logger = logger
        
        # Track completion for all task types
        self.completion_counts: Dict[str, int] = {}  # task_uid -> received_count
        self.expected_counts: Dict[str, int] = {}   # task_uid -> expected_count
        self.aggregated_results: Dict[str, List] = {}  # task_uid -> [completions]

    def register_task(self, task_uid: str, ranks: int):
        """Register any task (executable or function) for result tracking."""
        self.expected_counts[task_uid] = ranks
        self.completion_counts[task_uid] = 0
        self.aggregated_results[task_uid] = []

    def try_consume_result(self) -> Optional[str]:
        """Try to consume one result from queue. Returns task_uid if task completed, None otherwise."""
        try:
            result = self.result_queue.get(block=False)
            if result == "SHUTDOWN":
                return None
            
            if isinstance(result, (ExecutableTaskCompletion, FunctionTaskCompletion)):
                return self._process_completion(result)
                
        except Exception:
            # Queue empty
            pass
        return None

    def _process_completion(self, completion) -> Optional[str]:
        """Process completion and return task_uid if task is now complete."""
        task_uid = completion.task_uid
        
        if task_uid not in self.expected_counts:
            self.logger.warning(f"Received completion for unregistered task {task_uid}")
            return None

        # Increment completion count
        self.completion_counts[task_uid] += 1
        self.aggregated_results[task_uid].append(completion)
        
        expected = self.expected_counts[task_uid]
        received = self.completion_counts[task_uid]
        
        if received >= expected:
            return task_uid
                
        return None

    def get_completed_task(self, task_uid: str) -> Optional[dict]:
        """Get completed task data and clean up tracking."""
        if task_uid not in self.expected_counts:
            return None
            
        results = self.aggregated_results.get(task_uid, [])
        if not results:
            return None
            
        # Sort by rank
        results.sort(key=lambda r: r.rank)
        
        # Determine if this is function or executable task
        is_function_task = isinstance(results[0], FunctionTaskCompletion)
        
        if is_function_task:
            result_data = self._aggregate_function_results(task_uid, results)
        else:
            result_data = self._aggregate_executable_results(results)
        
        # Clean up tracking
        self.cleanup_task(task_uid)
        return result_data

    def _aggregate_function_results(self, task_uid: str, results: List[FunctionTaskCompletion]) -> dict:
        """Aggregate function task results."""
        if len(results) == 1:
            # Single rank - return as-is
            result = results[0]
            return {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
                "return_value": result.return_value,
                "exception": result.exception,
                "success": result.success
            }
        else:
            # Multi-rank - aggregate
            stdout_parts = [f"Rank {r.rank}: {r.stdout}" for r in results]
            stderr_parts = [f"Rank {r.rank}: {r.stderr}" for r in results]
            max_exit_code = max(r.exit_code for r in results)
            all_successful = all(r.success for r in results)
            
            # Collect return values
            return_values = [r.return_value for r in results]
            
            # If any stored in DDict, create single reference for all ranks
            if any(r.stored_in_ddict for r in results):
                return_value = self.shared_memory.create_data_reference(task_uid, len(results))
            else:
                return_value = return_values
            
            return {
                "stdout": "\n".join(stdout_parts),
                "stderr": "\n".join(stderr_parts),
                "exit_code": max_exit_code,
                "return_value": return_value,
                "exception": None if all_successful else "; ".join(
                    str(r.exception) for r in results if not r.success
                ),
                "success": all_successful
            }

    def _aggregate_executable_results(self, results: List[ExecutableTaskCompletion]) -> dict:
        """Aggregate executable task results."""
        if len(results) == 1:
            return results[0].to_result_dict()
        else:
            stdout_parts = [f"Rank {r.rank}: {r.stdout}" for r in results]
            stderr_parts = [f"Rank {r.rank}: {r.stderr}" for r in results]
            max_exit_code = max(r.exit_code for r in results)
            
            return {
                "stdout": "\n".join(stdout_parts),
                "stderr": "\n".join(stderr_parts),
                "exit_code": max_exit_code,
                "return_value": None,
                "exception": None if max_exit_code == 0 else f"One or more processes failed"
            }

    def cleanup_task(self, task_uid: str):
        """Clean up task tracking data."""
        self.completion_counts.pop(task_uid, None)
        self.expected_counts.pop(task_uid, None)
        self.aggregated_results.pop(task_uid, None)

    def is_task_complete(self, task_uid: str) -> bool:
        """Check if task is complete based on completion counts."""
        if task_uid not in self.expected_counts:
            return False
        
        expected = self.expected_counts[task_uid]
        received = self.completion_counts.get(task_uid, 0)
        return received >= expected

class TaskLauncher:
    """Unified task launching for all task types."""

    def __init__(self, ddict: DDict, result_queue: Queue, working_dir: str, logger: logging.Logger):
        self.ddict = ddict
        self.result_queue = result_queue
        self.working_dir = working_dir
        self.logger = logger

    async def launch_task(self, task: dict) -> TaskInfo:
        """Launch any type of task and return TaskInfo."""
        task_type = self._determine_task_type(task)

        if task_type.name.startswith("SINGLE_"):
            return await self._launch_single_task(task, task_type)
        else:
            return await self._launch_group_task(task, task_type)

    def _determine_task_type(self, task: dict) -> TaskType:
        """Determine task type based on task configuration."""
        is_function = bool(task.get("function"))
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))
        mpi = backend_kwargs.get("pmi", None)

        if mpi:
            if ranks < 2:
                raise ValueError("MPI tasks must have ranks > 1")
            return TaskType.MPI_FUNCTION if is_function else TaskType.MPI_EXECUTABLE
        if ranks == 1:
            return TaskType.SINGLE_FUNCTION if is_function else TaskType.SINGLE_EXECUTABLE
        else:  # ranks > 1 and not MPI
            return TaskType.MULTI_FUNCTION if is_function else TaskType.MULTI_EXECUTABLE

    async def _launch_single_task(self, task: dict, task_type: TaskType) -> TaskInfo:
        """Launch single-rank task."""
        uid = task["uid"]

        if task_type == TaskType.SINGLE_FUNCTION:
            process = await self._create_function_process(task, 0)
        else:
            process = self._create_executable_process(task, 0)

        process.start()
        self.logger.debug(f"Started single-rank Dragon process for task {uid}")

        return TaskInfo(
            task_type=task_type,
            ranks=1,
            start_time=time.time(),
            process=process
        )

    async def _launch_group_task(self, task: dict, task_type: TaskType) -> TaskInfo:
        """Launch multi-rank or MPI task."""
        uid = task["uid"]
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 2))

        if task_type.name.startswith("MPI_"):
            mpi = backend_kwargs.get("pmi", None)
            if mpi is None:
                raise ValueError("Missing required 'pmi' value in backend_kwargs.")

            group = ProcessGroup(restart=False, policy=None, pmi=mpi)
            self.logger.debug(f"Started MPI group task {uid} ({mpi}) with {ranks} ranks")
        else:
            group = ProcessGroup(restart=False, policy=None)
            self.logger.debug(f"Started group task {uid} with {ranks} ranks")

        if task_type.name.endswith("_FUNCTION"):
            await self._add_function_processes_to_group(group, task, ranks)
        else:
            self._add_executable_processes_to_group(group, task, ranks)

        group.init()
        group.start()

        self.logger.debug(f"Started group task {uid} with {ranks} ranks")

        return TaskInfo(
            task_type=task_type,
            ranks=ranks,
            start_time=time.time(),
            group=group
        )

    async def _create_function_process(self, task: dict, rank: int) -> Process:
        """Create a single function process."""
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        use_ddict_storage = backend_kwargs.get('use_ddict_storage', False)

        return Process(
            target=_function_wrapper,
            args=(self.result_queue, self.ddict, task, rank, use_ddict_storage)
        )

    def _create_executable_process(self, task: dict, rank: int) -> Process:
        """Create a single executable process."""
        executable = task["executable"]
        args = list(task.get("args", []))
        uid = task["uid"]

        return Process(
            target=_executable_wrapper,
            args=(self.result_queue, executable, args, uid, rank, self.working_dir)
        )

    async def _add_function_processes_to_group(self, group: ProcessGroup, task: dict, ranks: int) -> None:
        """Add function processes to process group."""
        uid = task["uid"]
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        use_ddict_storage = backend_kwargs.get('use_ddict_storage', False)

        for rank in range(ranks):
            env = os.environ.copy()
            env["DRAGON_RANK"] = str(rank)

            template = ProcessTemplate(
                target=_function_wrapper,
                args=(self.result_queue, self.ddict, task, rank, use_ddict_storage),
                env=env,
                cwd=self.working_dir,
                stdout=Popen.PIPE,
                stderr=Popen.PIPE,
                stdin=Popen.DEVNULL,
            )
            group.add_process(nproc=1, template=template)

    def _add_executable_processes_to_group(self, group: ProcessGroup, task: dict, ranks: int) -> None:
        """Add executable processes to process group."""
        executable = task["executable"]
        args = list(task.get("args", []))
        uid = task["uid"]

        for rank in range(ranks):
            env = os.environ.copy()
            env["DRAGON_RANK"] = str(rank)

            template = ProcessTemplate(
                target=_executable_wrapper,
                args=(self.result_queue, executable, args, uid, rank, self.working_dir),
                env=env,
                cwd=self.working_dir,
            )
            group.add_process(nproc=1, template=template)


def _executable_wrapper(result_queue: Queue, executable: str, args: list, task_uid: str, rank: int, working_dir: str):
    """wrapper function that executes executable and pushes completion to queue."""
    import subprocess
    import time
    
    try:
        # Execute the process and capture output
        result = subprocess.run(
            [executable] + args,
            cwd=working_dir,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        # Create completion object
        completion = ExecutableTaskCompletion(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout=result.stdout,
            stderr=result.stderr,
            exit_code=result.returncode,
            timestamp=time.time()
        )
        
        # Push completion directly to queue
        result_queue.put(completion, block=True)
        
    except subprocess.TimeoutExpired:
        completion = ExecutableTaskCompletion(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout="",
            stderr="Process timed out",
            exit_code=124,
            timestamp=time.time()
        )
        result_queue.put(completion, block=True)
        
    except Exception as e:
        completion = ExecutableTaskCompletion(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout="",
            stderr=f"Execution error: {str(e)}",
            exit_code=1,
            timestamp=time.time()
        )
        result_queue.put(completion, block=True)


def _function_wrapper(result_queue: Queue, ddict: DDict, task: dict, rank: int, use_ddict_storage: bool):
    """wrapper function that executes user functions and pushes completion to queue.
    
    DDict is only used when user explicitly sets use_ddict_storage=True.
    Otherwise, return value is sent directly via queue.
    """
    import io
    import sys
    import traceback
    
    task_uid = task["uid"]
    os.environ["DRAGON_RANK"] = str(rank)

    # Capture stdout/stderr
    old_out, old_err = sys.stdout, sys.stderr
    out_buf, err_buf = io.StringIO(), io.StringIO()

    function = task["function"]
    args = task.get("args", ())
    kwargs = task.get("kwargs", {})

    try:
        sys.stdout, sys.stderr = out_buf, err_buf

        # Execute function
        if asyncio.iscoroutinefunction(function):
            result = asyncio.run(function(*args, **kwargs))
        else:
            raise RuntimeError('Sync functions are not supported, please define it as async')

        # Store in DDict only if user requested
        stored_in_ddict = False
        return_value_for_queue = result
        
        if use_ddict_storage:
            result_key = f"return_{task_uid}_rank_{rank}"
            ddict.pput(result_key, result)
            stored_in_ddict = True
            return_value_for_queue = None

        # Create completion and send via queue
        completion = FunctionTaskCompletion(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout=out_buf.getvalue(),
            stderr=err_buf.getvalue(),
            exit_code=0,
            timestamp=time.time(),
            success=True,
            exception=None,
            traceback=None,
            return_value=return_value_for_queue,
            stored_in_ddict=stored_in_ddict
        )

    except Exception as e:
        # Error case
        completion = FunctionTaskCompletion(
            task_uid=task_uid,
            rank=rank,
            process_id=os.getpid(),
            stdout=out_buf.getvalue(),
            stderr=err_buf.getvalue(),
            exit_code=1,
            timestamp=time.time(),
            success=False,
            exception=str(e),
            traceback=traceback.format_exc(),
            return_value=None,
            stored_in_ddict=False
        )

    finally:
        sys.stdout, sys.stderr = old_out, old_err

        # Send completion to queue
        try:
            result_queue.put(completion, block=True, timeout=30)
        except Exception as queue_error:
            print(f"Failed to send completion to queue: {queue_error}", file=sys.stderr)

        # Detach from DDict if used
        try:
            if stored_in_ddict:
                ddict.detach()
        except Exception:
            pass


class DragonExecutionBackend(BaseExecutionBackend):
    """Dragon execution backend with unified queue-based architecture
    
                ┌────────────────────────────┐
                │  DRAGON EXECUTION BACKEND  │
                └────────────────────────────┘
                              |    
                       ┌──────────────┐
                       │ TaskLauncher │
                       └──────┬───────┘
              ┌────────────-──┴──────────────┐
              ▼                              ▼
     ┌────────────────────┐       ┌────────────────────┐
     │ Executable Tasks   │       │ Function Tasks     │
     │ _executable_wrapper │       │ _function_wrapper   │
     └──────────┬─────────┘       └──────────┬─────────┘
                ▼                            ▼
     ┌──────────────────────────────────────────┐
     │         DRAGON QUEUE (Unified)           │
     │  ExecutableCompletion | FunctionCompletion │
     └──────────────────┬───────────────────────┘
                        ▼
              ┌───────────────────┐
              │  ResultCollector  │
              │   (Unified Logic) │
              └─────────┬─────────┘
                        ▼
           ┌────────────┴────────────┐
           ▼                         ▼
    ┌──────────────┐      ┌──────────────────┐
    │ Small Results│      │ Large Results    │
    │ (via Queue)  │      │ (DDict optional) │
    └──────────────┘      └──────────────────┘
                        ▼
              ┌──────────────┐
              │ DataReference│
              │  .resolve()  │
              └──────────────┘
    
    Characteristics:
    - Single unified queue for all tasks
    - DDict only for large results or user-requested storage
    - Consistent result collection pattern
    - Lower latency for small function results
    """

    @typeguard.typechecked
    def __init__(self, resources: Optional[dict] = None, ddict: Optional[DDict] = None):
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

        # Resource management
        self._slots: int = int(self._resources.get("slots", mp.cpu_count() or 1))
        self._free_slots: int = self._slots
        self._working_dir: str = self._resources.get("working_dir", os.getcwd())

        # Task tracking
        self._running_tasks: dict[str, TaskInfo] = {}

        # Dragon components
        self._ddict: Optional[DDict] = ddict
        self._system_alloc: Optional[System] = None
        self._result_queue: Optional[Queue] = None

        # Shared memory manager
        self._shared_memory: Optional[SharedMemoryManager] = None

        # Utilities
        self._result_collector: Optional[ResultCollector] = None
        self._task_launcher: Optional[TaskLauncher] = None

        # Async management
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    # --------------------------- Lifecycle ---------------------------
    def __await__(self):
        return self._async_init().__await__()

    async def _async_init(self):
        if not self._initialized:
            try:
                logger.debug("Starting Dragon backend async initialization...")
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
            logger.debug("Initializing Dragon backend with unified queue architecture...")
            await self._initialize_dragon()

            # Initialize system allocation
            logger.debug("Creating System allocation...")
            self._system_alloc = System()
            nnodes = self._system_alloc.nnodes
            logger.debug(f"System allocation created with {nnodes} nodes")

            # Initialize DDict (optional - only for large results)
            logger.debug("Creating DDict")
            if not self._ddict:
                self._ddict = DDict(
                    n_nodes=nnodes,
                    total_mem=nnodes * int(4 * 1024 * 1024 * 1024),  # 4GB per node
                    wait_for_keys=True,
                    working_set_size=4,
                    timeout=200
                )
            logger.debug("DDict created successfully")

            # Initialize global result queue (unified for all task types)
            logger.debug("Creating unified result queue...")
            self._result_queue = Queue()
            logger.debug("Result queue created successfully")

            # Initialize shared memory manager
            self._shared_memory = SharedMemoryManager(
                self._ddict, 
                self._system_alloc, 
                logger
            )
            await self._shared_memory.initialize()

            # Initialize utilities
            self._result_collector = ResultCollector(self._shared_memory, self._result_queue, logger)
            self._task_launcher = TaskLauncher(
                self._ddict, 
                self._result_queue, 
                self._working_dir, 
                logger
            )

            # Start task monitoring
            logger.debug("Starting unified task monitoring...")
            self._monitor_task = asyncio.create_task(self._monitor_tasks())
            await asyncio.sleep(0.1)

            logger.info(
                f"Dragon backend initialized with {self._slots} slots, "
                f"unified queue architecture"
            )
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
            pass
        logger.debug("Dragon backend active with unified queue-based architecture.")

    def register_callback(self, callback: Callable) -> None:
        self._callback_func = callback

    def get_task_states_map(self):
        return StateMapper(backend=self)

    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        self._ensure_initialized()

        for task in tasks:
            # Validate task
            is_valid, error_msg = self._validate_task(task)
            if not is_valid:
                task["exception"] = ValueError(error_msg)
                self._callback_func(task, "FAILED")
                continue

            self.tasks[task["uid"]] = task

            try:
                await self._submit_task(task)
            except Exception as e:
                task["exception"] = e
                self._callback_func(task, "FAILED")

    async def _submit_task(self, task: dict[str, Any]) -> None:
        """Submit a single task for execution."""
        uid = task["uid"]
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))

        # Wait for available slots
        while self._free_slots < ranks:
            logger.debug(f"Waiting for {ranks} slots for task {uid}, {self._free_slots} free")
            await asyncio.sleep(0.1)

        self._free_slots -= ranks

        try:
            # Register all tasks with unified result collector
            self._result_collector.register_task(uid, ranks)

            # Launch task using unified launcher
            task_info = await self._task_launcher.launch_task(task)
            self._running_tasks[uid] = task_info
            self._callback_func(task, "RUNNING")

        except Exception:
            self._free_slots += ranks
            raise

    async def _monitor_tasks(self) -> None:
        """Monitor running tasks with unified queue consumption."""
        while not self._shutdown_event.is_set():
            try:
                # Batch consume queue results (unified for all task types)
                completed_tasks = []

                for _ in range(1000):  # Process up to 1000 results per iteration
                    completed_task_uid = self._result_collector.try_consume_result()
                    if completed_task_uid:
                        completed_tasks.append(completed_task_uid)
                    else:
                        break

                # Process all completed tasks
                for uid in completed_tasks:
                    if uid in self._running_tasks:
                        task_info = self._running_tasks[uid]
                        task = self.tasks.get(uid)
                        
                        if task:
                            # Get aggregated results from collector
                            result_data = self._result_collector.get_completed_task(uid)
                            if result_data:
                                task.update(result_data)
                            
                            # Determine task status and notify callback
                            if task.get("canceled", False):
                                self._callback_func(task, "CANCELED")
                            elif task.get("exception") or task.get("exit_code", 0) != 0:
                                self._callback_func(task, "FAILED")
                            else:
                                self._callback_func(task, "DONE")

                        # Free up slots
                        self._free_slots += task_info.ranks
                        
                        # Remove from running tasks
                        self._running_tasks.pop(uid, None)

                await asyncio.sleep(0.01)  # Short sleep for responsiveness

            except Exception as e:
                logger.exception(f"Error in task monitoring: {e}")
                await asyncio.sleep(1)

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a specific running task with proper cleanup."""
        self._ensure_initialized()

        task_info = self._running_tasks.get(uid)
        if not task_info:
            return False

        try:
            success = await self._cancel_task_by_info(task_info)
            
            if success:
                task_info.canceled = True

                # Clean up result collector tracking
                self._result_collector.cleanup_task(uid)
                
                # Clean up data references if stored in DDict
                try:
                    task_result = self.tasks.get(uid, {}).get("return_value")
                    if isinstance(task_result, DataReference):
                        self._shared_memory.cleanup_reference(task_result)
                except Exception as e:
                    logger.warning(f"Error cleaning up references for task {uid}: {e}")
                    
            return success

        except Exception as e:
            logger.exception(f"Error cancelling task {uid}: {e}")
            return False

    async def _cancel_task_by_info(self, task_info: TaskInfo) -> bool:
        """Cancel task based on TaskInfo."""
        proc, group = task_info.process, task_info.group

        if proc:
            if proc.is_alive:
                proc.terminate(); proc.join(2.0)
                if proc.is_alive:
                    proc.kill(); proc.join(1.0)
            return True

        if group and not group.inactive_puids:
            try:
                group.stop()
                group.close()
            except DragonUserCodeError:
                pass
            return True
        return False

    async def cancel_all_tasks(self) -> int:
        """Cancel all running tasks."""
        self._ensure_initialized()
        canceled = 0
        for task_uid in list(self._running_tasks.keys()):
            try:
                if await self.cancel_task(task_uid):
                    canceled += 1
            except Exception:
                pass
        return canceled

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

    def _ensure_initialized(self):
        """Ensure backend is properly initialized."""
        if not self._initialized:
            raise RuntimeError(
                "DragonExecutionBackend must be awaited before use. "
                "Use: backend = await DragonExecutionBackend(resources)"
            )

    def get_ddict(self) -> DDict:
        """Get the shared DDict for cross-task data sharing."""
        self._ensure_initialized()
        return self._ddict

    def get_result_queue(self) -> Queue:
        """Get the global result queue."""
        self._ensure_initialized()
        return self._result_queue

    # Data management methods delegated to SharedMemoryManager
    def store_task_data(self, key: str, data: Any) -> None:
        """Store data in shared DDict for cross-task access."""
        self._ensure_initialized()
        self._shared_memory.store_task_data(key, data)

    def get_task_data(self, key: str, default=None) -> Any:
        """Retrieve data from shared DDict."""
        self._ensure_initialized()
        return self._shared_memory.get_task_data(key, default)

    def list_task_data_keys(self) -> list:
        """List all keys in the shared DDict."""
        self._ensure_initialized()
        return self._shared_memory.list_task_data_keys()

    def clear_task_data(self, key: str = None) -> None:
        """Clear specific key or all data from shared DDict."""
        self._ensure_initialized()
        self._shared_memory.clear_task_data(key)

    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        """Link explicit data dependencies between tasks."""
        pass

    def link_implicit_data_deps(self, src_task, dst_task):
        """Link implicit data dependencies between tasks."""
        pass

    async def state(self) -> str:
        """Get backend state."""
        return "CONNECTED" if self._initialized else "DISCONNECTED"

    async def task_state_cb(self, task: dict, state: str) -> None:
        """Task state callback."""
        pass

    async def build_task(self, task: dict) -> None:
        """Build task."""
        pass

    async def shutdown(self) -> None:
        """Shutdown with proper cleanup."""
        if not self._initialized:
            return

        try:
            self._shutdown_event.set()
            await self.cancel_all_tasks()

            # Signal result collector to stop
            try:
                if self._result_queue:
                    self._result_queue.put("SHUTDOWN", block=False)
            except Exception as e:
                logger.warning(f"Error signaling queue shutdown: {e}")

            # Stop monitoring task
            if self._monitor_task and not self._monitor_task.done():
                try:
                    await asyncio.wait_for(self._monitor_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self._monitor_task.cancel()

            # Clean up result queue
            try:
                if self._result_queue:
                    while True:
                        try:
                            self._result_queue.get(block=False)
                        except:
                            break
                    self._result_queue = None
                logger.debug("Result queue cleaned up")
            except Exception as e:
                logger.warning(f"Error cleaning up result queue: {e}")

            # Clean up DDict
            try:
                if self._ddict:
                    self._ddict.clear()
                    self._ddict.destroy()
                    self._ddict = None
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

            logger.info("Dragon execution backend shutdown complete")

        except Exception as e:
            logger.exception(f"Error during shutdown: {e}")
        finally:
            self.tasks.clear()
            self._running_tasks.clear()
            self._initialized = False

    async def __aenter__(self):
        if not self._initialized:
            await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    @classmethod
    async def create(cls, resources: Optional[dict] = None):
        """Create and initialize a DragonExecutionBackend."""
        backend = cls(resources)
        return await backend

import asyncio
import logging
import os
import time
import pickle
import dill
import cloudpickle
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
    from dragon.infrastructure.facts import PMIBackend
    from dragon.managed_memory import MemoryPool
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
    MemoryPool = None

logger = logging.getLogger(__name__)

SHARED_MEMORY_THRESHOLD = 1024 * 1024  # 1MB

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
    process: Optional[Process] = None
    group: Optional[ProcessGroup] = None

@dataclass
class ProcessOutput:
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    error: Optional[str] = None

class DataReference:
    """Reference to data stored in shared memory."""

    def __init__(self, ref_id: str, backend_id: str):
        self._ref_id = ref_id
        self._backend_id = backend_id
        self._is_resolved = False

    @property
    def ref_id(self) -> str:
        return self._ref_id

    @property
    def backend_id(self) -> str:
        return self._backend_id

    def __repr__(self) -> str:
        return f"DataReference(ref_id='{self._ref_id}', backend_id='{self._backend_id}')"

class SharedMemoryManager:
    """Manages data in shared memory with fallback to DDict."""

    def __init__(self, ddict: DDict, system: System, logger: logging.Logger):
        self.ddict = ddict
        self.system = system
        self.logger = logger
        self.memory_pools: Dict[int, MemoryPool] = {}
        self.managed_allocations: Dict[str, Any] = {}
        self.backend_id = f"dragon_{uuid.uuid4().hex[:8]}"

    async def initialize(self):
        """Initialize memory pools per node."""
        try:
            for node_id in range(self.system.nnodes):
                pool_size = int(os.environ.get('DRAGON_MEMORY_POOL_SIZE', 2 * 1024**3))
                self.memory_pools[node_id] = MemoryPool(
                    size=pool_size,
                    fname=f"dragon_pool_{node_id}_{self.backend_id}",
                    uid=node_id + hash(self.backend_id) % 10000,
                )
                self.logger.debug(f"Initialized memory pool on node {node_id}")
        except Exception as e:
            self.logger.warning(f"Failed to initialize memory pools: {e}")
            self.memory_pools.clear()

    def should_use_shared_memory(self, data: Any) -> bool:
        """Determine if data should be stored in shared memory based
           on the size thresold of SHARED_MEMORY_THRESHOLD."""
        if not self.memory_pools:
            return False

        try:
            estimated_size = self._estimate_size(data)
            return estimated_size >= SHARED_MEMORY_THRESHOLD
        except Exception:
            return False

    def _estimate_size(self, data: Any) -> int:
        """Estimate serialized size of data."""
        if isinstance(data, (str, bytes)):
            return len(data)
        elif isinstance(data, (list, tuple, dict)) and hasattr(data, '__len__'):
            return len(data) * 100  # Rough estimate

        try:
            return len(pickle.dumps(data))
        except Exception:
            return 1000

    async def store_data(self, data: Any, node_id: int = 0) -> DataReference:
        """Store data and return reference."""
        ref_id = f"ref_{uuid.uuid4().hex}"

        if self.should_use_shared_memory(data):
            try:
                await self._store_in_shared_memory(ref_id, data, node_id)
                self.logger.debug(f"Stored data {ref_id} in shared memory")
            except Exception as e:
                self.logger.warning(f"Shared memory storage failed for {ref_id}: {e}, using DDict")
                self._store_in_ddict(ref_id, data)
        else:
            self._store_in_ddict(ref_id, data)

        return DataReference(ref_id, self.backend_id)

    async def _store_in_shared_memory(self, ref_id: str, data: Any, node_id: int):
        """Store data in managed memory and record metadata in DDict."""

        # Choose best representation of payload
        if isinstance(data, (bytes, bytearray, memoryview)):
            payload = data
        elif hasattr(data, "tobytes"):  # NumPy, torch, etc.
            payload = data.tobytes()
        else:
            payload = self._serialize(data)

        # Allocate from the target node's pool
        pool = self.memory_pools[node_id]
        allocation = pool.alloc(len(payload))

        # Copy into managed memory
        mem_view = allocation.get_memview()
        mem_view[:len(payload)] = payload

        # Store allocation and metadata
        self.managed_allocations[ref_id] = allocation
        self.ddict.pput(
            f"meta_{ref_id}",
            {
                "node": node_id,
                "size": len(payload),
                "backend_id": self.backend_id,
                "in_shared_memory": True,
            },
        )

    def _store_in_ddict(self, ref_id: str, data: Any):
        """Store data directly in DDict."""
        self.ddict.pput(f"data_{ref_id}", data)
        self.ddict.pput(f"meta_{ref_id}", {
            'backend_id': self.backend_id,
            'in_shared_memory': False
        })

    async def resolve_reference(self, ref: DataReference) -> Any:
        """Resolve reference to actual data."""
        if ref.backend_id != self.backend_id:
            raise ValueError(f"Cannot resolve reference from different backend: {ref.backend_id}")

        meta_key = f"meta_{ref.ref_id}"
        if meta_key not in self.ddict:
            raise KeyError(f"Reference metadata not found: {ref.ref_id}")

        metadata = self.ddict[meta_key]

        if metadata['in_shared_memory']:
            return await self._retrieve_from_shared_memory(ref.ref_id, metadata)
        else:
            data_key = f"data_{ref.ref_id}"
            if data_key not in self.ddict:
                raise KeyError(f"Reference data not found: {ref.ref_id}")
            return self.ddict[data_key]

    async def _retrieve_from_shared_memory(self, ref_id: str, metadata: dict) -> Any:
        """Retrieve data from shared memory."""
        allocation = self.managed_allocations.get(ref_id)

        if not allocation:
            raise KeyError(f"Memory allocation not found for reference {ref_id}")

        size = metadata['size']
        mem_view = allocation.get_memview()
        serialized_data = bytes(mem_view[:size])

        return self._deserialize(serialized_data)

    def cleanup_reference(self, ref: DataReference):
        """Clean up reference data."""
        try:
            meta_key = f"meta_{ref.ref_id}"
            data_key = f"data_{ref.ref_id}"

            if meta_key in self.ddict:
                del self.ddict[meta_key]
            if data_key in self.ddict:
                del self.ddict[data_key]
            if ref.ref_id in self.managed_allocations:
                del self.managed_allocations[ref.ref_id]
        except Exception as e:
            self.logger.warning(f"Error cleaning up reference {ref.ref_id}: {e}")

    # DDict operations moved here
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

    def _serialize(self, data: Any) -> bytes:
        """Serialize data using best available method."""
        for serializer in [cloudpickle.dumps, dill.dumps, pickle.dumps]:
            try:
                return serializer(data)
            except Exception:
                continue
        raise RuntimeError("Failed to serialize data")

    def _deserialize(self, data: bytes) -> Any:
        """Deserialize data using best available method."""
        for deserializer in [cloudpickle.loads, dill.loads, pickle.loads]:
            try:
                return deserializer(data)
            except Exception:
                continue
        raise RuntimeError("Failed to deserialize data")

class AsyncCollector:
    """Eager async stdio collector - collects results immediately when process completes."""

    def __init__(self, logger: logging.Logger, buffer_size: int = 65536, timeout: float = 30.0):
        self.logger = logger
        self.buffer_size = buffer_size
        self.timeout = timeout

    async def collect_process_output(self, process) -> ProcessOutput:
        """Collect output from process immediately - called when process completes."""
        try:
            stdout_conn = getattr(process, 'stdout_conn', None)
            stderr_conn = getattr(process, 'stderr_conn', None)
            exit_code = getattr(process, 'exitcode', 0) or 0

            # Collect stdout and stderr concurrently with timeout
            try:
                stdout, stderr = await asyncio.wait_for(
                    asyncio.gather(
                        self._collect_stream(stdout_conn, "stdout"),
                        self._collect_stream(stderr_conn, "stderr"),
                        return_exceptions=True
                    ),
                    timeout=self.timeout
                )
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout collecting process output after {self.timeout}s")
                return ProcessOutput(
                    stdout="",
                    stderr="Output collection timeout",
                    exit_code=exit_code,
                    error=f"Collection timeout after {self.timeout}s"
                )

            # Handle collection exceptions
            if isinstance(stdout, Exception):
                stdout = f"Error collecting stdout: {stdout}"
            if isinstance(stderr, Exception):
                stderr = f"Error collecting stderr: {stderr}"

            return ProcessOutput(
                stdout=stdout,
                stderr=stderr,
                exit_code=exit_code,
            )

        except Exception as e:
            self.logger.exception(f"Unexpected error in collect_process_output: {e}")
            return ProcessOutput(
                stdout="",
                stderr=f"Collection error: {e}",
                exit_code=getattr(process, 'exitcode', 1) or 1,
                error=str(e)
            )

    async def collect_group_output(self, group) -> ProcessOutput:
        """Collect output from all processes in group concurrently."""
        try:
            # Create collection tasks for all processes concurrently
            collection_tasks = []
            process_info = []

            for puid, exit_code in group.inactive_puids:
                proc = Process(None, ident=puid)
                task = self._collect_process_streams(proc, exit_code)
                collection_tasks.append(task)
                process_info.append((puid, exit_code))

            if not collection_tasks:
                return ProcessOutput()

            # Wait for all collections with timeout
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*collection_tasks, return_exceptions=True),
                    timeout=self.timeout
                )
            except asyncio.TimeoutError:
                return ProcessOutput(
                    stdout="",
                    stderr="Group collection timeout",
                    exit_code=1,
                    error=f"Group collection timeout after {self.timeout}s"
                )

            # Aggregate results
            stdout_parts = []
            stderr_parts = []
            exit_codes = []

            for i, (result, (puid, exit_code)) in enumerate(zip(results, process_info)):
                if isinstance(result, Exception):
                    stdout_parts.append(f"Rank {i}: Error collecting output")
                    stderr_parts.append(f"Rank {i}: {result}")
                    exit_codes.append(1)
                else:
                    stdout, stderr = result
                    stdout_parts.append(f"Rank {i}: {stdout}")
                    stderr_parts.append(f"Rank {i}: {stderr}")
                    exit_codes.append(exit_code)

            return ProcessOutput(
                stdout="\n".join(stdout_parts),
                stderr="\n".join(stderr_parts),
                exit_code=max(exit_codes) if exit_codes else 0,
            )

        except Exception as e:
            self.logger.exception(f"Unexpected error in collect_group_output: {e}")
            return ProcessOutput(
                stdout="",
                stderr=f"Group collection error: {e}",
                exit_code=1,
                error=str(e)
            )

    async def _collect_process_streams(self, process, exit_code: int) -> Tuple[str, str]:
        """Collect stdout/stderr from a single process in group."""
        stdout_conn = getattr(process, 'stdout_conn', None)
        stderr_conn = getattr(process, 'stderr_conn', None)

        stdout, stderr = await asyncio.gather(
            self._collect_stream(stdout_conn, "stdout"),
            self._collect_stream(stderr_conn, "stderr"),
            return_exceptions=True
        )

        if isinstance(stdout, Exception):
            stdout = f"Error: {stdout}"
        if isinstance(stderr, Exception):
            stderr = f"Error: {stderr}"

        return stdout, stderr

    async def _collect_stream(self, conn, stream_name: str) -> str:
        """Non-blocking stream collection with cooperative yielding."""
        if not conn:
            return ""

        chunks = []
        total_size = 0
        max_size = 10 * 1024 * 1024  # 10MB limit
        retry_count = 0
        max_retries = 100  # Prevent infinite loops

        try:
            while total_size < max_size and retry_count < max_retries:
                try:
                    # Non-blocking check for data
                    if hasattr(conn, 'poll') and not conn.poll(0):
                        retry_count += 1
                        await asyncio.sleep(0.001)  # Brief yield
                        continue

                    # Reset retry count when data is available
                    retry_count = 0

                    # Try to read data
                    chunk = conn.recv()
                    if not chunk:  # EOF
                        break

                    chunk_str = str(chunk) if chunk else ""
                    chunks.append(chunk_str)
                    total_size += len(chunk_str)

                    # Yield control every few chunks
                    if len(chunks) % 20 == 0:
                        await asyncio.sleep(0)

                except (BlockingIOError, OSError) as e:
                    if "would block" in str(e).lower():
                        retry_count += 1
                        await asyncio.sleep(0.001)
                        continue
                    else:
                        # Real I/O error
                        break
                except EOFError:
                    # Normal end of stream
                    break
                except Exception as e:
                    self.logger.warning(f"Unexpected error reading {stream_name}: {e}")
                    break

            if retry_count >= max_retries:
                self.logger.warning(f"Max retries reached collecting {stream_name}")

            return "".join(chunks)

        except Exception as e:
            self.logger.warning(f"Error collecting {stream_name}: {e}")
            raise  # Re-raise to be handled by caller
        finally:
            self._safe_close(conn)

    def _safe_close(self, conn):
        """Safely close connection, ignoring errors."""
        try:
            if hasattr(conn, 'close'):
                conn.close()
        except Exception:
            pass

class ResultCollector:
    """ResultCollector for result collection with shared memory optimization."""

    def __init__(self, shared_memory_manager: SharedMemoryManager, logger: logging.Logger):
        self.shared_memory = shared_memory_manager
        self.ddict = shared_memory_manager.ddict
        self.logger = logger
        self.stdio_collector = AsyncCollector(logger, timeout=30.0)

    async def collect_results(self, uid: str, task_info, task: dict) -> bool:
        """
        Collect results immediately when task completes.
        Returns True when task is complete WITH results already collected.
        """
        try:
            if task_info.task_type.name.startswith("SINGLE_"):
                return await self._collect_single_task_results(uid, task_info, task)
            else:
                return await self._collect_group_task_results(uid, task_info, task)
        except Exception as e:
            self.logger.exception(f"Error collecting results for task {uid}: {e}")
            self._set_task_failed(task, str(e))
            return True  # Task is "done" (with failure)

    async def _collect_single_task_results(self, uid: str, task_info, task: dict) -> bool:
        """Collect single task results - only return True when COMPLETE with results."""
        process = task_info.process
        if not process:
            return True  # No process, consider done

        if process.is_alive:
            return False  # Still running

        # Process is complete, collect results NOW
        if task_info.task_type.name.endswith("_FUNCTION"):
            await self._collect_function_results_from_ddict(uid, 1, task)
        else:  # executable
            output = await self.stdio_collector.collect_process_output(process)
            self._set_executable_results(task, output)

        return True  # Complete with results

    async def _collect_group_task_results(self, uid: str, task_info, task: dict) -> bool:
        """Collect group task results - only return True when COMPLETE with results."""
        group = task_info.group
        if not group:
            return True  # No group, consider done

        if not hasattr(group, 'inactive_puids') or not group.inactive_puids:
            return False  # Still running

        # Group is complete, collect results NOW
        if task_info.task_type.name.endswith("_FUNCTION"):
            await self._collect_function_results_from_ddict(uid, task_info.ranks, task)
        else:  # executable
            output = await self.stdio_collector.collect_group_output(group)
            self._set_executable_results(task, output)

        return True  # Complete with results

    def _set_executable_results(self, task: dict, output: ProcessOutput):
        """Set task results from collected output."""
        task.update({
            "stdout": output.stdout,
            "stderr": output.stderr,
            "exit_code": output.exit_code,
            "return_value": None,
            "exception": output.error  # Include collection errors
        })

    async def _collect_function_results_from_ddict(self, uid: str, ranks: int, task: dict):
        """Collect function results from DDict with shared memory optimization."""
        completion_keys = [f"{uid}_rank_{rank}_completed" for rank in range(ranks)]
        await self._wait_for_completion_keys(completion_keys)

        results = []
        stdout_parts = {}
        stderr_parts = {}
        return_values = []

        for rank in range(ranks):
            result_key = f"{uid}_rank_{rank}"
            result_data = self._get_ddict_result(result_key, rank)
            results.append(result_data)
            stdout_parts[rank] = result_data.get('stdout', '')
            stderr_parts[rank] = result_data.get('stderr', '')

            # Optimize return value storage
            if result_data.get('success', False) and result_data.get('return_value') is not None:
                referenced_value = await self._store_return_value_in_shared_memory(
                    result_data['return_value'], uid, rank
                )
                return_values.append(referenced_value)
            else:
                return_values.append(result_data.get('return_value'))

        self._set_function_task_results(task, results, stdout_parts, stderr_parts, return_values, ranks)
        self._cleanup_ddict_entries(uid, ranks)

    async def _store_return_value_in_shared_memory(self, value: Any, uid: str, rank: int) -> Any:
        """Optimize storage of return value using shared memory."""
        if self.shared_memory.should_use_shared_memory(value):
            try:
                ref = await self.shared_memory.store_data(value, node_id=0)
                self.logger.debug(f"Created reference for {uid}_rank_{rank} result")
                return ref
            except Exception as e:
                self.logger.warning(f"Failed to create reference for {uid}_rank_{rank}: {e}")

        return value

    async def _wait_for_completion_keys(self, completion_keys, timeout: int = 30):
        """Wait for DDict completion keys."""
        wait_count = 0
        max_wait = timeout * 10  # Check every 0.1 seconds

        while wait_count < max_wait:
            completed = sum(1 for key in completion_keys if key in self.ddict)
            if completed >= len(completion_keys):
                break
            await asyncio.sleep(0.1)
            wait_count += 1

    def _get_ddict_result(self, result_key: str, rank: int) -> dict:
        """Get result from DDict with error handling."""
        try:
            if result_key in self.ddict:
                return self.ddict[result_key]
        except Exception as e:
            self.logger.warning(f"Error reading DDict result for rank {rank}: {e}")

        return {
            'success': False,
            'exception': f'No result found for rank {rank}',
            'exit_code': 1,
            'rank': rank,
            'stdout': '',
            'stderr': f'No result found for rank {rank}'
        }

    def _set_function_task_results(self, task: dict, results, stdout_parts, stderr_parts, return_values, ranks: int):
        """Set aggregated function results with return values."""
        all_successful = all(r.get('success', False) for r in results)
        max_exit_code = max((r.get('exit_code', 1) for r in results), default=0)

        combined_stdout = "\n".join(f"Rank {i}: {stdout_parts.get(i, '')}" for i in range(ranks))
        combined_stderr = "\n".join(f"Rank {i}: {stderr_parts.get(i, '')}" for i in range(ranks))

        task.update({
            "stdout": combined_stdout,
            "stderr": combined_stderr,
            "exit_code": max_exit_code,
            "return_value": return_values[0] if len(return_values) == 1 else return_values,
            "exception": None if all_successful else "; ".join(
                str(r.get('exception', 'Unknown error')) 
                for r in results if not r.get('success', False)
            )
        })

    def _cleanup_ddict_entries(self, uid: str, ranks: int):
        """Clean up DDict entries."""
        try:
            for rank in range(ranks):
                result_key = f"{uid}_rank_{rank}"
                completion_key = f"{uid}_rank_{rank}_completed"
                if result_key in self.ddict:
                    del self.ddict[result_key]
                if completion_key in self.ddict:
                    del self.ddict[completion_key]
        except Exception as e:
            self.logger.warning(f"Error cleaning DDict entries for {uid}: {e}")

    def _set_task_failed(self, task: dict, error_msg: str):
        """Mark task as failed."""
        task.update({
            "exception": error_msg,
            "exit_code": 1,
            "stderr": task.get("stderr", "") + f"\nError: {error_msg}",
            "return_value": None
        })

class TaskLauncher:
    """Unified task launching for all task types."""

    def __init__(self, ddict: DDict, working_dir: str, logger: logging.Logger):
        self.ddict = ddict
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
        backend_kwargs = task.get('task_backend_specific_kwargs', {})
        ranks = int(backend_kwargs.get("ranks", 1))
        mpi = backend_kwargs.get("mpi", False)
        is_function = bool(task.get("function"))

        if ranks == 1 and not mpi:
            return TaskType.SINGLE_FUNCTION if is_function else TaskType.SINGLE_EXECUTABLE
        elif mpi:
            return TaskType.MPI_FUNCTION if is_function else TaskType.MPI_EXECUTABLE
        else:  # ranks > 1 and not MPI
            return TaskType.MULTI_FUNCTION if is_function else TaskType.MULTI_EXECUTABLE

    async def _launch_single_task(self, task: dict, task_type: TaskType) -> TaskInfo:
        """Launch single-rank task."""
        uid = task["uid"]

        if task_type == TaskType.SINGLE_FUNCTION:
            process = await self._create_function_process(task, 0)
        else:
            process = self._create_executable_process(task)

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
        ranks = int(backend_kwargs.get("ranks", 1))

        group = ProcessGroup(restart=False, policy=None)

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
        uid = task["uid"]
        function = task["function"]
        args = task.get("args", ())
        kwargs = task.get("kwargs", {})

        func = await self._serialize_function(function, args, kwargs)

        return Process(
            target=_function_worker,
            args=(self.ddict, rank, func, uid, rank)
        )

    def _create_executable_process(self, task: dict) -> Process:
        """Create a single executable process."""
        executable = task["executable"]
        args = list(task.get("args", []))

        return Process(
            target=executable,
            args=args,
            cwd=self.working_dir,
            stdout=Popen.PIPE,
            stderr=Popen.PIPE,
            stdin=Popen.DEVNULL,
            env=os.environ.copy(),
        )

    async def _add_function_processes_to_group(self, group: ProcessGroup, task: dict, ranks: int) -> None:
        """Add function processes to process group."""
        uid = task["uid"]
        function = task["function"]
        args = task.get("args", ())
        kwargs = task.get("kwargs", {})

        func = await self._serialize_function(function, args, kwargs)

        for rank in range(ranks):
            env = os.environ.copy()
            env["DRAGON_RANK"] = str(rank)

            template = ProcessTemplate(
                target=_function_worker,
                args=(self.ddict, rank, func, uid, rank),
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

        for rank in range(ranks):
            env = os.environ.copy()
            env["DRAGON_RANK"] = str(rank)

            template = ProcessTemplate(
                target=executable,
                args=args,
                env=env,
                cwd=self.working_dir,
                stdout=Popen.PIPE,
                stderr=Popen.PIPE,
                stdin=Popen.DEVNULL,
            )
            group.add_process(nproc=1, template=template)

    async def _serialize_function(self, function: Callable, args: tuple, kwargs: dict) -> bytes:
        """Serialize function data using multiple serialization methods."""
        serializers = [
            ('dill', dill.dumps),
            ('cloudpickle', cloudpickle.dumps),
            ('pickle', pickle.dumps)
        ]

        func_info = {
            'function': function,
            'args': args,
            'kwargs': kwargs
        }

        for name, serializer in serializers:
            try:
                func = serializer(func_info)
                self.logger.debug(f"Successfully serialized function with {name}")
                return func
            except Exception as e:
                self.logger.debug(f"Failed to serialize with {name}: {e}")
                continue

        raise RuntimeError("Could not serialize function with any available method")


def _function_worker(d: DDict, client_id: int, func: bytes, task_uid: str, rank: int = 0):
    """Worker function to execute user functions in separate Dragon processes."""
    import io
    import sys
    import traceback
    import dill
    import cloudpickle

    # Set environment variable for rank
    os.environ["DRAGON_RANK"] = str(rank)

    # Capture stdout/stderr
    old_out, old_err = sys.stdout, sys.stderr
    out_buf, err_buf = io.StringIO(), io.StringIO()

    try:
        sys.stdout, sys.stderr = out_buf, err_buf

        # FIXME: Keep the serializer type when we serialize and use it to Deserialize function data
        func_info = None
        deserializers = [dill.loads, cloudpickle.loads, pickle.loads]

        for deserializer in deserializers:
            try:
                func_info = deserializer(func)
                break
            except Exception:
                continue

        if func_info is None:
            raise RuntimeError("Could not deserialize function")

        function = func_info['function']
        args = func_info.get('args', ())
        kwargs = func_info.get('kwargs', {})

        # Execute function
        if asyncio.iscoroutinefunction(function):
            result = asyncio.run(function(*args, **kwargs))
        else:
            raise RuntimeError('Sync functions are not supported, please define it as async')

        # Store successful result
        result_data = {
            'success': True,
            'return_value': result,
            'stdout': out_buf.getvalue(),
            'stderr': err_buf.getvalue(),
            'exception': None,
            'exit_code': 0,
            'rank': rank,
            'task_uid': task_uid
        }

    except Exception as e:
        # Store error result
        result_data = {
            'success': False,
            'return_value': None,
            'stdout': out_buf.getvalue(),
            'stderr': err_buf.getvalue(),
            'exception': str(e),
            'exit_code': 1,
            'traceback': traceback.format_exc(),
            'rank': rank,
            'task_uid': task_uid
        }

    finally:
        # Restore stdout/stderr
        sys.stdout, sys.stderr = old_out, old_err

        # Store results in DDict
        try:
            result_key = f"{task_uid}_rank_{rank}"
            completion_key = f"{task_uid}_rank_{rank}_completed"

            d.pput(result_key, result_data)
            d.pput(completion_key, True)
        except Exception:
            pass

        # Detach from DDict
        try:
            d.detach()
        except Exception:
            pass


class DragonExecutionBackend(BaseExecutionBackend):
    """Dragon execution backend for distributed task execution with DDict integration and shared memory optimization."""

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

        # Resource management
        self._slots: int = int(self._resources.get("slots", mp.cpu_count() or 1))
        self._free_slots: int = self._slots
        self._working_dir: str = self._resources.get("working_dir", os.getcwd())

        # Task tracking
        self._running_tasks: dict[str, TaskInfo] = {}

        # Dragon components
        self._ddict: Optional[DDict] = None
        self._system_alloc: Optional[System] = None

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
            logger.debug("Initializing Dragon backend with DDict...")
            await self._initialize_dragon()

            # Initialize system allocation
            logger.debug("Creating System allocation...")
            self._system_alloc = System()
            nnodes = self._system_alloc.nnodes
            logger.debug(f"System allocation created with {nnodes} nodes")

            # Initialize DDict
            logger.debug("Creating DDict with proper parameters...")

            #FIXME: make it dynamic and per user
            self._ddict = DDict(
                n_nodes=nnodes,
                total_mem=nnodes * int(4 * 1024 * 1024 * 1024),  # 4GB per node
                wait_for_keys=True,
                working_set_size=4,
                timeout=200
            )
            logger.debug("DDict created successfully")

            # Initialize shared memory manager
            self._shared_memory = SharedMemoryManager(self._ddict, self._system_alloc, logger)
            await self._shared_memory.initialize()

            # Initialize utilities
            self._result_collector = ResultCollector(self._shared_memory, logger)
            self._task_launcher = TaskLauncher(self._ddict, self._working_dir, logger)

            # Start task monitoring
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
            pass
        logger.debug("Dragon backend active with DDict integration.")

    def register_callback(self, callback: Callable) -> None:
        self._callback_func = callback

    def get_task_states_map(self):
        return StateMapper(backend=self)

    async def get_data_from_reference(self, ref: DataReference) -> Any:
        """Resolve data reference to actual data."""
        if not self._initialized:
            raise RuntimeError("Backend not initialized")

        if not isinstance(ref, DataReference):
            raise TypeError("Expected DataReference object")

        return await self._shared_memory.resolve_reference(ref)

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
            # Launch task using unified launcher
            task_info = await self._task_launcher.launch_task(task)
            self._running_tasks[uid] = task_info
            self._callback_func(task, "RUNNING")

        except Exception:
            self._free_slots += ranks
            raise

    async def _monitor_tasks(self) -> None:
        """Monitor running tasks for completion."""
        while not self._shutdown_event.is_set():
            try:
                completed_tasks = []

                for uid, task_info in list(self._running_tasks.items()):
                    task = self.tasks.get(uid)
                    if not task:
                        completed_tasks.append(uid)
                        continue

                    # This will return True only when results are FULLY collected
                    if await self._result_collector.collect_results(uid, task_info, task):
                        completed_tasks.append(uid)

                        # Determine task status and notify callback
                        if task.get("exception") or task.get("exit_code", 0) != 0:
                            self._callback_func(task, "FAILED")
                        else:
                            self._callback_func(task, "DONE")

                        # Free up slots
                        self._free_slots += task_info.ranks

                # Clean up completed tasks
                for uid in completed_tasks:
                    self._running_tasks.pop(uid, None)

                await asyncio.sleep(0.1)

            except Exception as e:
                logger.exception(f"Error in task monitoring: {e}")
                await asyncio.sleep(1)

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a specific running task."""
        self._ensure_initialized()

        task_info = self._running_tasks.get(uid)
        if not task_info:
            return False

        try:
            success = await self._cancel_task_by_info(task_info)
            if success:
                # Clean up DDict entries
                self._result_collector._cleanup_ddict_entries(uid, task_info.ranks)

                # Clean up any data references for this task
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
        try:
            if task_info.process:
                # Single process cancellation
                if task_info.process.is_alive:
                    task_info.process.terminate()
                    task_info.process.join(timeout=2.0)
                    if task_info.process.is_alive:
                        task_info.process.kill()
                return True

            elif task_info.group:
                # Process group cancellation
                if not task_info.group.inactive_puids:
                    for puid in getattr(task_info.group, "puids", []):
                        try:
                            proc = Process(None, ident=puid)
                            proc.terminate()
                        except Exception as e:
                            logger.warning(f"Failed to terminate process {puid}: {e}")
                return True

        except Exception as e:
            logger.warning(f"Failed to cancel task: {e}")

        return False

    async def cancel_all_tasks(self) -> int:
        """Cancel all running tasks."""
        self._ensure_initialized()
        cancelled = 0
        for task_uid in list(self._running_tasks.keys()):
            try:
                if await self.cancel_task(task_uid):
                    cancelled += 1
            except Exception:
                pass
        return cancelled

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
        """Shutdown the backend and cleanup resources."""
        if not self._initialized:
            return

        try:
            self._shutdown_event.set()
            await self.cancel_all_tasks()

            # Stop monitoring task
            if self._monitor_task and not self._monitor_task.done():
                try:
                    await asyncio.wait_for(self._monitor_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self._monitor_task.cancel()

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

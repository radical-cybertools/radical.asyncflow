import asyncio
import logging
import os
import time
import pickle
import uuid
from typing import Any, Callable, Optional, Dict, List, Tuple
from enum import Enum
from dataclasses import dataclass, field

import typeguard

from ...constants import StateMapper
from .base import BaseExecutionBackend, Session

try:
    import dragon
    import multiprocessing as mp
    from dragon.native.process import ProcessTemplate
    from dragon.native.process_group import ProcessGroup
    from dragon.native.queue import Queue
    from dragon.data.ddict.ddict import DDict
    from dragon.native.machine import System
    from dragon.native.process_group import DragonUserCodeError
    from dragon.infrastructure.policy import Policy
except ImportError:
    dragon = None
    ProcessTemplate = None
    ProcessGroup = None
    Queue = None
    DDict = None
    System = None
    Policy = None

logger = logging.getLogger(__name__)

DRAGON_DEFAULT_REF_THRESHOLD = int(
    os.environ.get("DRAGON_DEFAULT_REF_THRESHOLD", 1024 * 1024)
)

class TaskType(Enum):
    """Enumeration of supported task types."""
    FUNCTION = "function"
    EXECUTABLE = "executable"

class WorkerPinningPolicy(Enum):
    """Worker pinning policy for task assignment."""
    STRICT = "strict"      # Wait indefinitely for hinted worker
    SOFT = "soft"          # Wait N seconds, then fallback to any worker
    AFFINITY = "affinity"  # Prefer hinted worker, use others if not immediately available
    EXCLUSIVE = "exclusive" # Only hinted worker can run, reject if insufficient capacity

@dataclass
class WorkerRequest:
    """Request sent to worker pool."""
    task_uid: str
    task_type: TaskType
    rank: int
    total_ranks: int
    gpu_ids: List[int] = field(default_factory=list)
    function: Optional[Callable] = None
    args: tuple = ()
    kwargs: dict = None
    executable: Optional[str] = None
    exec_args: list = None
    working_dir: str = "."
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}
        if self.exec_args is None:
            self.exec_args = []

@dataclass
class WorkerResponse:
    """Response from worker."""
    task_uid: str
    rank: int
    success: bool
    worker_name: str = ""
    return_value: Any = None
    stdout: str = ""
    stderr: str = ""
    exception: Optional[str] = None
    exit_code: int = 0
    timestamp: float = 0.0

@dataclass
class TaskInfo:
    """Container for task runtime information."""
    task_type: TaskType
    ranks: int
    start_time: float
    worker_name: str = ""
    gpu_allocations: Dict[int, List[int]] = field(default_factory=dict)  # rank -> gpu_ids
    canceled: bool = False
    completed_ranks: int = 0

@dataclass
class PolicyConfig:
    """Configuration for a single policy (ProcessTemplate)."""
    nprocs: int
    policy: Optional[Policy] = None
    ngpus: int = 0

@dataclass
class WorkerGroupConfig:
    """Configuration for a worker group (ProcessGroup) with node placement.
    
    Attributes:
        name: Worker group name for identification
        policies: List of PolicyConfig, each becomes a ProcessTemplate
    """
    name: str
    policies: List[PolicyConfig] = field(default_factory=list)
    
    def total_slots(self) -> int:
        """Total CPU slots provided by this worker group."""
        return sum(p.nprocs for p in self.policies)
    
    def total_gpus(self) -> int:
        """Total GPUs provided by this worker group."""
        return sum(p.ngpus for p in self.policies)

class DataReference:
    """Reference to data stored in Cross Node Distributed Dict."""

    def __init__(self, ref_id: str, backend_id: str, ddict: DDict):
        self._ref_id = ref_id
        self._backend_id = backend_id
        self._ddict = ddict

    @property
    def ref_id(self) -> str:
        return self._ref_id

    @property
    def backend_id(self) -> str:
        return self._backend_id

    def resolve(self) -> Any:
        """Resolve reference to actual data."""
        data_key = f"data_{self._ref_id}"
        if data_key not in self._ddict:
            raise KeyError(f"Reference data not found: {self._ref_id}")
        return self._ddict[data_key]

    def __repr__(self) -> str:
        return f"DataReference(ref_id='{self._ref_id}', backend_id='{self._backend_id}')"

class SharedMemoryManager:
    """Manages data storage using DDict exclusively."""

    def __init__(self, ddict: DDict, system: System, logger: logging.Logger, 
                 reference_threshold: int = DRAGON_DEFAULT_REF_THRESHOLD):
        self.ddict = ddict
        self.system = system
        self.logger = logger
        self.backend_id = f"dragon_{uuid.uuid4().hex[:8]}"
        self.reference_threshold = reference_threshold

    async def initialize(self):
        """Initialize the storage manager."""
        self.logger.debug(f"SharedMemoryManager initialized (threshold: {self.reference_threshold} bytes)")

    def should_use_reference(self, data: Any) -> bool:
        """Determine if data should be stored as reference based on size threshold."""
        try:
            estimated_size = self._estimate_size(data)
            return estimated_size >= self.reference_threshold
        except Exception:
            return False

    def _estimate_size(self, data: Any) -> int:
        """Estimate serialized size of data."""
        if isinstance(data, (str, bytes)):
            return len(data)
        elif isinstance(data, (list, tuple, dict)) and hasattr(data, '__len__'):
            return len(data) * 100
        try:
            return len(pickle.dumps(data))
        except Exception:
            return 1000

    async def store_data(self, data: Any, node_id: int = 0) -> DataReference:
        """Store data in DDict and return reference."""
        ref_id = f"ref_{uuid.uuid4().hex}"
        try:
            self._store_in_ddict(ref_id, data)
            self.logger.debug(f"Stored data {ref_id} in DDict")
        except Exception as e:
            self.logger.error(f"Failed to store data {ref_id}: {e}")
            raise
        return DataReference(ref_id, self.backend_id, self.ddict)

    def _store_in_ddict(self, ref_id: str, data: Any):
        """Store data directly in DDict."""
        self.ddict.pput(f"data_{ref_id}", data)
        self.ddict.pput(f"meta_{ref_id}", {
            'backend_id': self.backend_id,
            'stored_at': time.time()
        })

    def cleanup_reference(self, ref: DataReference):
        """Clean up reference data."""
        try:
            for key in [f"meta_{ref.ref_id}", f"data_{ref.ref_id}"]:
                if key in self.ddict:
                    del self.ddict[key]
        except Exception as e:
            self.logger.warning(f"Error cleaning up reference {ref.ref_id}: {e}")


def _worker_loop(worker_id: int, worker_name: str, input_queue: Queue, output_queue: Queue, 
                 ddict: DDict, working_dir: str) -> None:
    """Persistent worker that processes tasks from queue.
    
    Handles both function and executable tasks:
    - Functions: Execute Python callables
    - Executables: Run external processes via subprocess
    """
    import io
    import sys
    import traceback
    import subprocess
    
    os.environ["DRAGON_WORKER_ID"] = str(worker_id)
    os.environ["DRAGON_WORKER_NAME"] = worker_name
    
    try:
        while True:
            # Get task from queue (blocking)
            request = input_queue.get()
            
            # Shutdown signal
            if request is None:
                break
            
            if not isinstance(request, WorkerRequest):
                continue
            
            # Set rank environment variable for the task
            os.environ["DRAGON_RANK"] = str(request.rank)
            
            # Set GPU visibility for this rank
            if request.gpu_ids:
                os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, request.gpu_ids))
            else:
                os.environ["CUDA_VISIBLE_DEVICES"] = ""
            
            response = WorkerResponse(
                task_uid=request.task_uid,
                rank=request.rank,
                worker_name=worker_name,
                success=False,
                timestamp=time.time()
            )
            
            try:
                if request.task_type == TaskType.FUNCTION:
                    # Execute Python function
                    old_out, old_err = sys.stdout, sys.stderr
                    out_buf, err_buf = io.StringIO(), io.StringIO()
                    
                    try:
                        sys.stdout, sys.stderr = out_buf, err_buf
                        
                        # Handle async functions
                        if asyncio.iscoroutinefunction(request.function):
                            result = asyncio.run(request.function(*request.args, **request.kwargs))
                        else:
                            # Sync functions not supported - enforce async
                            raise RuntimeError('Sync functions are not supported, please define as async')
                        
                        response.success = True
                        response.return_value = result
                        response.stdout = out_buf.getvalue()
                        response.stderr = err_buf.getvalue()
                        response.exit_code = 0
                        
                    except Exception as e:
                        response.success = False
                        response.exception = str(e)
                        response.stderr = err_buf.getvalue() + "\n" + traceback.format_exc()
                        response.exit_code = 1
                        
                    finally:
                        sys.stdout, sys.stderr = old_out, old_err
                
                elif request.task_type == TaskType.EXECUTABLE:
                    # Execute external process
                    result = subprocess.run(
                        [request.executable] + request.exec_args,
                        cwd=request.working_dir,
                        capture_output=True,
                        text=True,
                        timeout=3600  # FIXME: should be user defined
                    )
                    
                    response.success = (result.returncode == 0)
                    response.stdout = result.stdout
                    response.stderr = result.stderr
                    response.exit_code = result.returncode
                    if result.returncode != 0:
                        response.exception = f"Process exited with code {result.returncode}"
                
            except subprocess.TimeoutExpired:
                response.exception = "Process timed out"
                response.exit_code = 124
                response.stderr = "Process timed out"
                
            except Exception as e:
                response.exception = f"Worker error: {str(e)}"
                response.exit_code = 1
                response.stderr = traceback.format_exc()
            
            # Send response back
            output_queue.put(response)
            
    except Exception as e:
        print(f"Worker {worker_id} ({worker_name}) fatal error: {e}")
        raise
    finally:
        # FIXME: return value should be stored in ddict (not used)
        # while the output should be flushed to the queue
        # Detach from DDict on exit
        try:
            ddict.detach()
        except Exception:
            pass


class WorkerPool:
    """Manages persistent worker pool with per-worker queues and slot reservation.
    
    Key features:
    - Each worker group has its own input queue
    - Slot tracking per worker for load balancing
    - Tasks are assigned to workers with sufficient free slots
    """

    def __init__(self, worker_configs: List[WorkerGroupConfig], ddict: DDict, 
                 working_dir: str, logger: logging.Logger, system: System):
        self.worker_configs = worker_configs
        self.ddict = ddict
        self.working_dir = working_dir
        self.logger = logger
        self.system = system
        
        self.output_queue: Optional[Queue] = None
        self.worker_queues: Dict[str, Queue] = {}
        
        # CPU slot tracking
        self.worker_slots: Dict[str, int] = {}
        self.worker_free_slots: Dict[str, int] = {}
        
        # GPU tracking
        self.worker_gpus: Dict[str, int] = {}  # Total GPUs per worker
        self.worker_free_gpus: Dict[str, List[int]] = {}  # Available GPU IDs per worker
        
        self.process_groups: List[ProcessGroup] = []
        self.total_workers = len(worker_configs)
        self.total_slots = sum(cfg.total_slots() for cfg in worker_configs)
        self.total_gpus = sum(cfg.total_gpus() for cfg in worker_configs)
        self.initialized = False
        
    async def initialize(self):
        """Initialize worker pool with per-worker queues."""
        if self.initialized:
            return
        
        try:
            # Single shared output queue
            self.output_queue = Queue()
            
            worker_id = 0
            
            for worker_config in self.worker_configs:
                worker_name = worker_config.name
                
                # Create dedicated input queue for this worker
                input_queue = Queue()
                self.worker_queues[worker_name] = input_queue
                
                # Track CPU slots
                total_worker_slots = worker_config.total_slots()
                self.worker_slots[worker_name] = total_worker_slots
                self.worker_free_slots[worker_name] = total_worker_slots
                
                # Track GPUs - assign sequential IDs
                total_worker_gpus = worker_config.total_gpus()
                self.worker_gpus[worker_name] = total_worker_gpus
                
                # Build list of available GPU IDs for this worker
                gpu_ids = list(range(total_worker_gpus))
                self.worker_free_gpus[worker_name] = gpu_ids.copy()
                
                process_group = ProcessGroup(restart=False)
                
                # Add templates for each policy
                for policy_config in worker_config.policies:
                    env = os.environ.copy()
                    env["DRAGON_WORKER_ID"] = str(worker_id)
                    env["DRAGON_WORKER_NAME"] = worker_name

                    template = ProcessTemplate(
                        target=_worker_loop,
                        args=(worker_id, worker_name, input_queue, self.output_queue, 
                              self.ddict, self.working_dir),
                        env=env,
                        cwd=self.working_dir,
                        policy=policy_config.policy
                    )
                    
                    process_group.add_process(nproc=policy_config.nprocs, template=template)
                
                # Initialize and start
                process_group.init()
                process_group.start()
                self.process_groups.append(process_group)
                worker_id += 1
            
            self.initialized = True
            

            config_summary = []
            for cfg in self.worker_configs:
                config_summary.append(f"{cfg.name}: {cfg.total_slots()} slots, {cfg.total_gpus()} GPUs")
            
            self.logger.info(
                f"Worker pool initialized: {self.total_workers} workers, "
                f"{self.total_slots} total slots, {self.total_gpus} total GPUs. "
                f"Config: {'; '.join(config_summary)}"
            )
            
        except Exception as e:
            self.logger.exception(f"Failed to initialize worker pool: {e}")
            raise
    
    def find_worker_for_task(self, ranks: int, gpus_per_rank: int = 0, 
                            preferred_worker: Optional[str] = None) -> Optional[str]:
        """Find worker with sufficient free CPU slots and GPUs."""
        total_gpus_needed = ranks * gpus_per_rank
        
        if preferred_worker and preferred_worker in self.worker_free_slots:
            if (self.worker_free_slots[preferred_worker] >= ranks and
                len(self.worker_free_gpus[preferred_worker]) >= total_gpus_needed):
                return preferred_worker
        
        for worker_name in self.worker_free_slots.keys():
            if (self.worker_free_slots[worker_name] >= ranks and
                len(self.worker_free_gpus[worker_name]) >= total_gpus_needed):
                return worker_name
        return None
    
    def worker_has_capacity(self, worker_name: str, ranks: int, gpus_per_rank: int = 0) -> bool:
        """Check if specific worker has CPU and GPU capacity."""
        total_gpus_needed = ranks * gpus_per_rank
        return (worker_name in self.worker_free_slots and 
                self.worker_free_slots[worker_name] >= ranks and
                len(self.worker_free_gpus[worker_name]) >= total_gpus_needed)
    
    def worker_exists(self, worker_name: str) -> bool:
        """Check if worker exists."""
        return worker_name in self.worker_slots
    
    def reserve_resources(self, worker_name: str, ranks: int, gpus_per_rank: int = 0) -> Tuple[bool, Dict[int, List[int]]]:
        """Reserve CPU slots and GPUs. Returns (success, gpu_allocations)."""
        if worker_name not in self.worker_free_slots:
            return False, {}
        
        total_gpus_needed = ranks * gpus_per_rank
        
        if (self.worker_free_slots[worker_name] < ranks or
            len(self.worker_free_gpus[worker_name]) < total_gpus_needed):
            return False, {}
        
        # Reserve CPU slots
        self.worker_free_slots[worker_name] -= ranks
        
        # Reserve GPUs and create allocation map
        gpu_allocations = {}
        if gpus_per_rank > 0:
            for rank in range(ranks):
                allocated_gpus = []
                for _ in range(gpus_per_rank):
                    gpu_id = self.worker_free_gpus[worker_name].pop(0)
                    allocated_gpus.append(gpu_id)
                gpu_allocations[rank] = allocated_gpus
        
        self.logger.debug(
            f"Reserved {ranks} slots + {total_gpus_needed} GPUs on {worker_name} "
            f"({self.worker_free_slots[worker_name]}/{self.worker_slots[worker_name]} slots free, "
            f"{len(self.worker_free_gpus[worker_name])}/{self.worker_gpus[worker_name]} GPUs free)"
        )
        
        return True, gpu_allocations
    
    def release_resources(self, worker_name: str, ranks: int, gpu_allocations: Dict[int, List[int]]):
        """Release CPU slots and GPUs back to worker."""
        if worker_name in self.worker_free_slots:
            self.worker_free_slots[worker_name] += ranks
            
            # Return GPUs to free pool
            for rank_gpus in gpu_allocations.values():
                self.worker_free_gpus[worker_name].extend(rank_gpus)
            self.worker_free_gpus[worker_name].sort()
            
            total_gpus_returned = sum(len(gpus) for gpus in gpu_allocations.values())
            
            self.logger.debug(
                f"Released {ranks} slots + {total_gpus_returned} GPUs on {worker_name} "
                f"({self.worker_free_slots[worker_name]}/{self.worker_slots[worker_name]} slots free, "
                f"{len(self.worker_free_gpus[worker_name])}/{self.worker_gpus[worker_name]} GPUs free)"
            )
    
    def submit_request(self, worker_name: str, request: WorkerRequest):
        """Submit task request to specific worker."""
        if not self.initialized:
            raise RuntimeError("Worker pool not initialized")
        
        if worker_name not in self.worker_queues:
            raise ValueError(f"Unknown worker: {worker_name}")
        
        try:
            self.worker_queues[worker_name].put(request, timeout=10)
        except Exception as e:
            self.logger.error(f"Failed to submit request to {worker_name}: {e}")
            raise
    
    def try_get_response(self) -> Optional[WorkerResponse]:
        """Try to get response from output queue (non-blocking)."""
        try:
            return self.output_queue.get(block=False)
        except:
            return None
    
    async def shutdown(self):
        """Shutdown worker pool gracefully."""
        if not self.initialized:
            return
        
        try:
            for worker_name, input_queue in self.worker_queues.items():
                worker_slots = self.worker_slots[worker_name]
                for _ in range(worker_slots):
                    input_queue.put(None)
            
            # Stop all process groups
            for idx, process_group in enumerate(self.process_groups):
                try:
                    if not process_group.inactive_puids:
                        process_group.stop()
                        process_group.close()
                        self.logger.debug(f"Stopped ProcessGroup {idx + 1}")
                except DragonUserCodeError:
                    pass
                except Exception as e:
                    self.logger.warning(f"Error stopping ProcessGroup {idx + 1}: {e}")
            
            self.logger.info("Worker pool shutdown complete")
            
        except Exception as e:
            self.logger.exception(f"Error shutting down worker pool: {e}")
        finally:
            self.initialized = False
            self.process_groups.clear()
            self.worker_queues.clear()


class ResultCollector:
    """Collects and aggregates results from worker pool."""
    
    def __init__(self, shared_memory_manager: SharedMemoryManager, logger: logging.Logger):
        self.shared_memory = shared_memory_manager
        self.logger = logger
        
        # Track task completions
        self.task_responses: Dict[str, List[WorkerResponse]] = {}
        self.task_expected: Dict[str, int] = {}
    
    def register_task(self, task_uid: str, ranks: int):
        """Register a task for result tracking."""
        self.task_expected[task_uid] = ranks
        self.task_responses[task_uid] = []
    
    def process_response(self, response: WorkerResponse) -> Optional[str]:
        """Process worker response."""
        task_uid = response.task_uid
        
        if task_uid not in self.task_expected:
            self.logger.warning(f"Received response for unregistered task {task_uid}")
            return None
        
        self.task_responses[task_uid].append(response)
        
        if len(self.task_responses[task_uid]) >= self.task_expected[task_uid]:
            return task_uid
        
        return None
    
    async def get_task_result(self, task_uid: str) -> Optional[dict]:
        """Get aggregated task result."""
        if task_uid not in self.task_responses:
            return None
        
        responses = self.task_responses[task_uid]
        responses.sort(key=lambda r: r.rank)
        
        if len(responses) == 1:
            r = responses[0]
            result = {
                "stdout": r.stdout,
                "stderr": r.stderr,
                "exit_code": r.exit_code,
                "return_value": await self._maybe_create_reference(r.return_value, task_uid, r.rank),
                "exception": r.exception
            }
        else:
            stdout_parts = [f"Rank {r.rank}: {r.stdout}" for r in responses]
            stderr_parts = [f"Rank {r.rank}: {r.stderr}" for r in responses]
            max_exit_code = max(r.exit_code for r in responses)
            all_successful = all(r.success for r in responses)
            
            # Collect return values
            return_values = []
            for r in responses:
                if r.success and r.return_value is not None:
                    ref_value = await self._maybe_create_reference(r.return_value, task_uid, r.rank)
                    return_values.append(ref_value)
                else:
                    return_values.append(r.return_value)
            
            result = {
                "stdout": "\n".join(stdout_parts),
                "stderr": "\n".join(stderr_parts),
                "exit_code": max_exit_code,
                "return_value": return_values[0] if len(return_values) == 1 else return_values,
                "exception": None if all_successful else "; ".join(
                    r.exception for r in responses if r.exception
                )
            }
        
        # Cleanup
        self.cleanup_task(task_uid)
        return result
    
    async def _maybe_create_reference(self, value: Any, task_uid: str, rank: int) -> Any:
        """Create reference if value is large enough."""
        if value is None:
            return None
            
        if self.shared_memory.should_use_reference(value):
            try:
                ref = await self.shared_memory.store_data(value)
                self.logger.debug(f"Created reference for {task_uid}_rank_{rank}")
                return ref
            except Exception as e:
                self.logger.warning(f"Failed to create reference: {e}")
        return value
    
    def cleanup_task(self, task_uid: str):
        """Clean up task tracking."""
        self.task_responses.pop(task_uid, None)
        self.task_expected.pop(task_uid, None)


class DragonExecutionBackend(BaseExecutionBackend):
    """Dragon execution backend with per-worker slot reservation.
    
    Features:
    - Per-worker queue architecture for true load balancing
    - Slot reservation ensures tasks go to workers with capacity
    - Tasks with same rank requirement run in parallel on different workers
    - High performance with minimal coordination overhead
    Example configuration:
        resources = {
            "workers": [
                {
                    "name": "cpu_worker",
                    "policies": [
                        {"nprocs": 128, "policy": policy_n0},
                        {"nprocs": 128, "policy": policy_n1}
                    ]
                },
                {
                    "name": "gpu_worker",
                    "policies": [
                        {"nprocs": 128, "ngpus": 2, "policy": policy_n2},
                        {"nprocs": 128, "ngpus": 2, "policy": policy_n3}
                    ]
                }
            ]
        }
    """
    
    @typeguard.typechecked
    def __init__(self, resources: Optional[dict] = None, ddict: Optional[DDict] = None):
        if dragon is None:
            raise ImportError("Dragon is required for DragonExecutionBackend.")
        
        self.tasks: dict[str, dict[str, Any]] = {}
        self.session = Session()
        self._callback_func: Callable = None
        self._resources = resources or {}
        self._initialized = False
        
        # Parse worker configuration
        self._worker_configs = self._parse_worker_config(self._resources)
        self._total_slots = sum(cfg.total_slots() for cfg in self._worker_configs)
        self._total_gpus = sum(cfg.total_gpus() for cfg in self._worker_configs)
        
        # Other resources
        self._working_dir: str = self._resources.get("working_dir", os.getcwd())
        self._reference_threshold: int = int(
            self._resources.get("reference_threshold", DRAGON_DEFAULT_REF_THRESHOLD)
        )
        
        # Task tracking
        self._running_tasks: dict[str, TaskInfo] = {}
        self._pending_tasks: asyncio.Queue = asyncio.Queue()
        
        # Dragon components
        self._ddict: Optional[DDict] = ddict
        self._system_alloc: Optional[System] = None
        self._shared_memory: Optional[SharedMemoryManager] = None
        self._result_collector: Optional[ResultCollector] = None
        self._worker_pool: Optional[WorkerPool] = None

        # Async management
        self._monitor_task: Optional[asyncio.Task] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
    
    def _parse_worker_config(self, resources: dict) -> List[WorkerGroupConfig]:
        """Parse worker configuration from resources."""
        if "workers" in resources:
            configs = []
            for idx, worker_cfg in enumerate(resources["workers"]):
                name = worker_cfg.get("name")
                if not name:
                    raise ValueError(f"Worker config {idx}: 'name' is required")
                
                policies_list = worker_cfg.get("policies")
                if not policies_list:
                    raise ValueError(f"Worker '{name}': 'policies' list is required")
                
                if not isinstance(policies_list, list):
                    raise TypeError(f"Worker '{name}': 'policies' must be a list")
                
                policy_configs = []
                for policy_idx, policy_dict in enumerate(policies_list):
                    nprocs = policy_dict.get("nprocs", 1)
                    ngpus = policy_dict.get("ngpus", 0)
                    policy = policy_dict.get("policy", None)
                    
                    if policy is not None and not isinstance(policy, Policy):
                        raise TypeError(
                            f"Worker '{name}' policy {policy_idx}: 'policy' must be a Dragon Policy object or None"
                        )
                    
                    policy_configs.append(PolicyConfig(nprocs=nprocs, policy=policy, ngpus=ngpus))
                
                configs.append(WorkerGroupConfig(name=name, policies=policy_configs))
            
            return configs
        else:
            slots = int(resources.get("slots", mp.cpu_count() or 1))
            return [WorkerGroupConfig(
                name="default_workers", 
                policies=[PolicyConfig(nprocs=slots, policy=None, ngpus=0)]
            )]
    
    def __await__(self):
        return self._async_init().__await__()
    
    async def _async_init(self):
        if not self._initialized:
            try:
                logger.debug("Starting Dragon backend async initialization...")
                await self._initialize()
                self._initialized = True
                logger.debug("Registering with StateMapper...")
                StateMapper.register_backend_states_with_defaults(backend=self)
                logger.debug("Dragon backend fully initialized")
            except Exception as e:
                logger.exception(f"Dragon backend initialization failed: {e}")
                raise
        return self
    
    async def _initialize(self) -> None:
        try:
            # Set multiprocessing method
            try:
                if mp.get_start_method() != "dragon":
                    mp.set_start_method("dragon", force=True)
            except RuntimeError:
                pass
            
            # Initialize system
            self._system_alloc = System()
            nnodes = self._system_alloc.nnodes
            logger.debug(f"System allocation created with {nnodes} nodes")
            
            # Initialize DDict
            if not self._ddict:
                self._ddict = DDict(
                    n_nodes=nnodes,
                    total_mem=nnodes * int(4 * 1024 * 1024 * 1024),
                    wait_for_keys=True,
                    working_set_size=4,
                    timeout=200
                )
            logger.debug("DDict initialized")
            
            # Initialize shared memory manager
            self._shared_memory = SharedMemoryManager(
                self._ddict, self._system_alloc, logger, self._reference_threshold
            )
            await self._shared_memory.initialize()
            
            # Initialize result collector
            self._result_collector = ResultCollector(self._shared_memory, logger)
            
            # Initialize worker pool with per-worker queues
            self._worker_pool = WorkerPool(
                self._worker_configs, self._ddict, self._working_dir, logger, self._system_alloc
            )
            await self._worker_pool.initialize()
            
            # Start monitoring and scheduling
            self._monitor_task = asyncio.create_task(self._monitor_tasks())
            self._scheduler_task = asyncio.create_task(self._schedule_tasks())

            logger.info(
                f"Dragon backend initialized: {len(self._worker_configs)} workers, "
                f"{self._total_slots} total slots, {self._total_gpus} total GPUs"
            )

        except Exception as e:
            logger.exception(f"Failed to initialize Dragon backend: {e}")
            raise
    
    def register_callback(self, callback: Callable) -> None:
        self._callback_func = callback
    
    def get_task_states_map(self):
        return StateMapper(backend=self)
    
    async def submit_tasks(self, tasks: list[dict[str, Any]]) -> None:
        self._ensure_initialized()
        
        for task in tasks:
            is_valid, error_msg = self._validate_task(task)
            if not is_valid:
                task["exception"] = ValueError(error_msg)
                self._callback_func(task, "FAILED")
                continue
            
            self.tasks[task["uid"]] = task
            
            try:
                # Add to pending queue for scheduler
                await self._pending_tasks.put(task)
            except Exception as e:
                task["exception"] = e
                self._callback_func(task, "FAILED")
    
    async def _schedule_tasks(self) -> None:
        """Scheduler: assigns tasks to workers with GPU-aware placement."""
        while not self._shutdown_event.is_set():
            try:
                # Get pending task (with timeout to check shutdown)
                try:
                    task = await asyncio.wait_for(self._pending_tasks.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                
                uid = task["uid"]
                backend_kwargs = task.get('task_backend_specific_kwargs', {})
                ranks = int(backend_kwargs.get("ranks", 1))
                gpus_per_rank = int(backend_kwargs.get("gpus_per_rank", 0))
                worker_hint = backend_kwargs.get("worker_hint")
                
                pinning_policy_str = backend_kwargs.get("pinning_policy", "").lower()
                try:
                    pinning_policy = WorkerPinningPolicy(pinning_policy_str) if pinning_policy_str else None
                except ValueError:
                    pinning_policy = None
                
                pinning_timeout = float(backend_kwargs.get("pinning_timeout", 30.0))
                
                worker_name = await self._apply_pinning_policy(
                    task, ranks, gpus_per_rank, worker_hint, pinning_policy, pinning_timeout
                )
                
                if not worker_name:
                    continue
                
                success, gpu_allocations = self._worker_pool.reserve_resources(worker_name, ranks, gpus_per_rank)
                if not success:
                    # Race condition - put back and retry
                    await self._pending_tasks.put(task)
                    continue
                
                try:
                    # Submit task to this specific worker
                    await self._submit_task_to_worker(task, worker_name, ranks, gpu_allocations)
                except Exception as e:
                    # Release slots on failure
                    self._worker_pool.release_resources(worker_name, ranks, gpu_allocations)
                    task["exception"] = e
                    self._callback_func(task, "FAILED")
                
            except Exception as e:
                logger.exception(f"Error in task scheduler: {e}")
                await asyncio.sleep(0.1)

    async def _apply_pinning_policy(
        self, 
        task: dict, 
        ranks: int,
        gpus_per_rank: int,
        worker_hint: Optional[str],
        pinning_policy: Optional[WorkerPinningPolicy],
        timeout: float
    ) -> Optional[str]:
        """Apply worker pinning policy to find appropriate worker."""
        
        if not worker_hint or not pinning_policy:
            worker_name = self._worker_pool.find_worker_for_task(ranks, gpus_per_rank)
            while not worker_name and not self._shutdown_event.is_set():
                await asyncio.sleep(0.01)
                worker_name = self._worker_pool.find_worker_for_task(ranks, gpus_per_rank)
            return worker_name
        
        if not self._worker_pool.worker_exists(worker_hint):
            error_msg = f"Worker hint '{worker_hint}' does not exist. Available workers: {list(self._worker_pool.worker_slots.keys())}"
            logger.error(error_msg)
            task["exception"] = ValueError(error_msg)
            self._callback_func(task, "FAILED")
            return None
        
        if pinning_policy == WorkerPinningPolicy.AFFINITY:
            if self._worker_pool.worker_has_capacity(worker_hint, ranks, gpus_per_rank):
                logger.debug(f"Task {task['uid']}: AFFINITY policy - using preferred worker {worker_hint}")
                return worker_hint
            else:
                worker_name = self._worker_pool.find_worker_for_task(ranks, gpus_per_rank)
                if worker_name:
                    logger.debug(f"Task {task['uid']}: AFFINITY policy - fallback to {worker_name}")
                    return worker_name
                while not worker_name and not self._shutdown_event.is_set():
                    await asyncio.sleep(0.01)
                    worker_name = self._worker_pool.find_worker_for_task(ranks, gpus_per_rank)
                return worker_name
        
        elif pinning_policy == WorkerPinningPolicy.STRICT:
            logger.debug(f"Task {task['uid']}: STRICT policy - waiting for worker {worker_hint}")
            while not self._shutdown_event.is_set():
                if self._worker_pool.worker_has_capacity(worker_hint, ranks, gpus_per_rank):
                    logger.debug(f"Task {task['uid']}: STRICT policy - worker {worker_hint} now available")
                    return worker_hint
                await asyncio.sleep(0.01)
            return None
        
        elif pinning_policy == WorkerPinningPolicy.SOFT:
            logger.debug(f"Task {task['uid']}: SOFT policy - waiting {timeout}s for worker {worker_hint}")
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                if self._worker_pool.worker_has_capacity(worker_hint, ranks, gpus_per_rank):
                    logger.debug(f"Task {task['uid']}: SOFT policy - worker {worker_hint} available")
                    return worker_hint
                await asyncio.sleep(0.01)
                if self._shutdown_event.is_set():
                    return None
            
            logger.debug(f"Task {task['uid']}: SOFT policy - timeout reached, using fallback")
            worker_name = self._worker_pool.find_worker_for_task(ranks, gpus_per_rank)
            while not worker_name and not self._shutdown_event.is_set():
                await asyncio.sleep(0.01)
                worker_name = self._worker_pool.find_worker_for_task(ranks, gpus_per_rank)
            
            if worker_name:
                logger.debug(f"Task {task['uid']}: SOFT policy - fallback to {worker_name}")
            return worker_name
        
        elif pinning_policy == WorkerPinningPolicy.EXCLUSIVE:
            if self._worker_pool.worker_has_capacity(worker_hint, ranks, gpus_per_rank):
                logger.debug(f"Task {task['uid']}: EXCLUSIVE policy - using worker {worker_hint}")
                return worker_hint
            else:
                total_capacity = self._worker_pool.worker_slots.get(worker_hint, 0)
                total_gpu_capacity = self._worker_pool.worker_gpus.get(worker_hint, 0)
                total_gpus_needed = ranks * gpus_per_rank
                
                if ranks > total_capacity or total_gpus_needed > total_gpu_capacity:
                    error_msg = (
                        f"Task {task['uid']}: EXCLUSIVE policy - worker '{worker_hint}' "
                        f"has insufficient total capacity ({total_capacity} slots, {total_gpu_capacity} GPUs) "
                        f"for {ranks} ranks × {gpus_per_rank} GPUs/rank"
                    )
                else:
                    error_msg = (
                        f"Task {task['uid']}: EXCLUSIVE policy - worker '{worker_hint}' "
                        f"currently has insufficient free resources"
                    )
                
                logger.error(error_msg)
                task["exception"] = ValueError(error_msg)
                self._callback_func(task, "FAILED")
                return None
        
        return None
    
    async def _submit_task_to_worker(self, task: dict[str, Any], worker_name: str, 
                                     ranks: int, gpu_allocations: Dict[int, List[int]]) -> None:
        """Submit task to specific worker with GPU assignments."""
        uid = task["uid"]
        
        try:
            # Register task with result collector
            self._result_collector.register_task(uid, ranks)
            
            # Determine task type
            is_function = bool(task.get("function"))
            task_type = TaskType.FUNCTION if is_function else TaskType.EXECUTABLE
            
            # Create and submit requests for each rank to the specific worker
            for rank in range(ranks):
                gpu_ids = gpu_allocations.get(rank, [])
                
                if is_function:
                    request = WorkerRequest(
                        task_uid=uid,
                        task_type=TaskType.FUNCTION,
                        rank=rank,
                        total_ranks=ranks,
                        gpu_ids=gpu_ids,
                        function=task["function"],
                        args=task.get("args", ()),
                        kwargs=task.get("kwargs", {})
                    )
                else:
                    request = WorkerRequest(
                        task_uid=uid,
                        task_type=TaskType.EXECUTABLE,
                        rank=rank,
                        total_ranks=ranks,
                        gpu_ids=gpu_ids,
                        executable=task["executable"],
                        exec_args=list(task.get("args", [])),
                        working_dir=self._working_dir
                    )
                
                # Submit to specific worker's queue
                self._worker_pool.submit_request(worker_name, request)
            
            # Track task
            self._running_tasks[uid] = TaskInfo(
                task_type=task_type,
                ranks=ranks,
                worker_name=worker_name,
                gpu_allocations=gpu_allocations,
                start_time=time.time()
            )
            
            self._callback_func(task, "RUNNING")
            
        except Exception:
            raise
    
    async def _monitor_tasks(self) -> None:
        """Monitor tasks by consuming responses from worker pool."""
        while not self._shutdown_event.is_set():
            try:
                completed_tasks = []
                
                # Consume responses from worker pool (batch processing)
                for _ in range(1000):
                    response = self._worker_pool.try_get_response()
                    if response:
                        completed_uid = self._result_collector.process_response(response)
                        if completed_uid and completed_uid not in completed_tasks:
                            completed_tasks.append(completed_uid)
                    else:
                        break
                
                for uid in completed_tasks:
                    if uid in self._running_tasks:
                        task_info = self._running_tasks[uid]
                        task = self.tasks.get(uid)
                        
                        if task:
                            result = await self._result_collector.get_task_result(uid)
                            if result:
                                task.update(result)
                            
                            self._worker_pool.release_resources(
                                task_info.worker_name, 
                                task_info.ranks, 
                                task_info.gpu_allocations
                            )
                            
                            if task.get("canceled", False):
                                self._callback_func(task, "CANCELED")
                            elif task.get("exception") or task.get("exit_code", 0) != 0:
                                self._callback_func(task, "FAILED")
                            else:
                                self._callback_func(task, "DONE")
                        
                        self._running_tasks.pop(uid, None)
                
                await asyncio.sleep(0.01)
                
            except Exception as e:
                logger.exception(f"Error in task monitoring: {e}")
                await asyncio.sleep(1)
    
    async def cancel_task(self, uid: str) -> bool:
        """Cancel a running task.

        Note: With worker pool architecture, cancellation is best-effort.
        Workers that already picked up the task will complete it.
        """
        task_info = self._running_tasks.get(uid)
        if not task_info:
            return False
        
        task_info.canceled = True
        self._result_collector.cleanup_task(uid)
        
        self._worker_pool.release_resources(
            task_info.worker_name, 
            task_info.ranks, 
            task_info.gpu_allocations
        )
        
        if uid in self.tasks:
            self.tasks[uid]["canceled"] = True
        
        return True
    
    async def cancel_all_tasks(self) -> int:
        """Cancel all running tasks."""
        canceled = 0
        for uid in list(self._running_tasks.keys()):
            if await self.cancel_task(uid):
                canceled += 1
        return canceled
    
    def _validate_task(self, task: dict) -> tuple[bool, str]:
        """Validate task configuration."""
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
        
        gpus_per_rank = backend_kwargs.get("gpus_per_rank", 0)
        try:
            gpus_per_rank = int(gpus_per_rank)
            if gpus_per_rank < 0:
                return False, "Task 'gpus_per_rank' must be >= 0"
        except (ValueError, TypeError):
            return False, "Task 'gpus_per_rank' must be a valid integer"
        
        pinning_policy = backend_kwargs.get("pinning_policy", "").lower()
        if pinning_policy:
            try:
                WorkerPinningPolicy(pinning_policy)
            except ValueError:
                valid_policies = [p.value for p in WorkerPinningPolicy]
                return False, f"Invalid pinning_policy '{pinning_policy}'. Must be one of: {valid_policies}"
        
        worker_hint = backend_kwargs.get("worker_hint")
        if worker_hint and not isinstance(worker_hint, str):
            return False, "worker_hint must be a string"
        
        timeout = backend_kwargs.get("pinning_timeout")
        if timeout is not None:
            try:
                float(timeout)
            except (ValueError, TypeError):
                return False, "pinning_timeout must be a number"
        
        return True, ""
    
    def _ensure_initialized(self):
        """Ensure backend is initialized."""
        if not self._initialized:
            raise RuntimeError(
                "DragonExecutionBackend must be awaited before use. "
                "Use: backend = await DragonExecutionBackend(resources)"
            )
    
    def get_ddict(self) -> DDict:
        """Get shared DDict for cross-task data sharing."""
        self._ensure_initialized()
        return self._ddict
    
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
    
    async def shutdown(self) -> None:
        """Shutdown backend gracefully."""
        if not self._initialized:
            return
        
        try:
            self._shutdown_event.set()
            
            # Cancel all tasks
            await self.cancel_all_tasks()
            
            if self._scheduler_task and not self._scheduler_task.done():
                try:
                    await asyncio.wait_for(self._scheduler_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self._scheduler_task.cancel()
            
            # Stop monitoring
            if self._monitor_task and not self._monitor_task.done():
                try:
                    await asyncio.wait_for(self._monitor_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self._monitor_task.cancel()
            
            # Shutdown worker pool
            if self._worker_pool:
                await self._worker_pool.shutdown()
            
            # Cleanup DDict
            if self._ddict:
                try:
                    self._ddict.clear()
                    self._ddict.destroy()
                except Exception as e:
                    logger.warning(f"Error cleaning up DDict: {e}")
            
            logger.info("Dragon backend shutdown complete")
            
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
        """Create and initialize backend."""
        backend = cls(resources)
        return await backend

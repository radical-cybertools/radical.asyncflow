"""
Dragon Batch Execution Backend - Wait-Based (Fast!)

Instead of polling the return queue, we simply call .wait() on each compiled
batch in a separate thread. This is what Dragon expects and it's fast!

Performance: Native batch 100K tasks in 5.6s, this should be similar!
"""

import asyncio
import logging
import threading
from typing import Any, Callable, Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor

try:
    from dragon.workflows.batch import Batch
    from dragon.native.process import ProcessTemplate
    DRAGON_AVAILABLE = True
except ImportError:
    DRAGON_AVAILABLE = False
    Batch = None
    ProcessTemplate = None

from .base import BaseExecutionBackend, Session

logger = logging.getLogger(__name__)


class TaskStateMapper:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    terminal_states = {DONE, FAILED, CANCELED}


class DragonExecutionBackend(BaseExecutionBackend):
    """
    Fast Dragon Batch integration using .wait() in threads.
    
    No polling! Each compiled batch gets a thread that calls .wait()
    and triggers callbacks when done. This is the Dragon-native way.
    """
    
    def __init__(
        self,
        num_workers: Optional[int] = None,
        working_directory: Optional[str] = None,
        disable_background_batching: bool = False,
        disable_telemetry: bool = False,
    ):
        if not DRAGON_AVAILABLE:
            raise RuntimeError("Dragon not available")
        
        self.batch = Batch(
            num_workers=num_workers or 0,
            disable_telem=disable_telemetry,
            disable_background_batching=disable_background_batching,
        )
        
        self.session = Session()
        if working_directory:
            self.session.path = working_directory
        
        self._state = "idle"
        self._callback = None
        self._task_registry: Dict[str, Any] = {}
        self._task_states = TaskStateMapper()
        
        # Thread pool for waiting on batches
        self._wait_executor = ThreadPoolExecutor(
            max_workers=32,  # Can handle many concurrent batches
            thread_name_prefix="batch-wait"
        )
        
        self._shutdown_event = threading.Event()
        
        logger.info(
            f"DragonExecutionBackend: {self.batch.num_workers} workers, "
            f"{self.batch.num_managers} managers"
        )
    
    def _wait_for_batch(self, compiled_tasks, task_uids: List[str]):
        """
        Wait for batch to complete and trigger callbacks.
        
        Simple: just wait(), then check results. No state tracking needed.
        """
        try:
            # Wait for batch - efficient blocking
            compiled_tasks.wait()
            
            # Get results for all tasks
            for uid in task_uids:
                task_info = self._task_registry.get(uid)
                if not task_info:
                    continue

                batch_task = task_info['batch_task']
                task_desc = task_info['description']
                
                try:
                    # Get result - instant after wait()
                    result = batch_task.result.get()
                    task_desc['return_value'] = result
                    self._callback(task_desc, 'DONE')
                    
                except Exception as e:
                    # Task failed
                    task_desc['exception'] = e
                    task_desc['stderr'] = str(e)
                    self._callback(task_desc, 'FAILED')
        
        except Exception as e:
            # Batch-level failure
            logger.error(f"Batch wait failed: {e}", exc_info=True)
            
            for uid in task_uids:
                task_info = self._task_registry.get(uid)
                if task_info:
                    task_info['description']['exception'] = e
                    self._callback(task_info['description'], 'FAILED')
    
    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Submit a batch of tasks and start a wait thread for them."""
        if self._state == "shutting_down":
            raise RuntimeError("Cannot submit during shutdown")
        
        self._state = "running"
        
        # Create Batch tasks (don't start yet)
        batch_tasks = []
        task_uids = []
        
        for task in tasks:
            try:
                batch_task = await self._create_batch_task(task)
                batch_tasks.append(batch_task)
                task_uids.append(task['uid'])
            except Exception as e:
                logger.error(f"Failed to create task {task.get('uid')}: {e}", exc_info=True)
                task['exception'] = e
                self._callback(task, 'FAILED')
        
        if not batch_tasks:
            return

        # Compile into a single batch
        compiled_tasks = self.batch.compile(batch_tasks)

        # Start the batch
        compiled_tasks.start()

        logger.info(f"Submitted batch of {len(batch_tasks)} tasks for execution")

        # Launch thread to wait for completion
        self._wait_executor.submit(
            self._wait_for_batch,
            compiled_tasks,
            task_uids
        )

    async def _create_batch_task(self, task: dict):
            """Create a single Batch task (don't start it)."""
            uid = task['uid']
            is_function = bool(task.get("function"))
            target = task.get('function' if is_function else 'executable')
            
            if not target:
                raise ValueError(f"Task {uid} missing target")

            args = task.get('args', [])
            kwargs = task.get('kwargs', {})
            backend_kwargs = task.get("task_backend_specific_kwargs", {})
            
            is_mpi = backend_kwargs.get('type') == 'mpi'
            timeout = backend_kwargs.get('timeout', 1000000000.0)
            name = task.get('name', uid)

            # Wrap async functions
            if is_function and asyncio.iscoroutinefunction(target):
                original_target = target
                target = lambda *a, **kw: asyncio.run(original_target(*a, **kw))

            # Create appropriate batch task
            if is_mpi:
                templates = task.get('process_templates') or [
                    (task.get('ranks', 2), ProcessTemplate(target=target, args=args, **kwargs))
                ]
                batch_task = self.batch.job(templates, name=name, timeout=timeout)
            elif is_function:
                batch_task = self.batch.function(target, *args, name=name, timeout=timeout, **kwargs)
            else:
                batch_task = self.batch.process(ProcessTemplate(target=target, args=args, **kwargs), 
                                            name=name, timeout=timeout)

            # Register and return
            self._task_registry[uid] = {
                'uid': uid,
                'description': task.copy(),
                'batch_task': batch_task,
            }

            logger.debug(f"Created {'function' if is_function else 'executable'} task: {uid} (MPI: {is_mpi})")
            return batch_task
    
    def build_task(self, task: dict) -> None:
        pass
    
    def link_implicit_data_deps(self, src_task, dst_task):
        pass
    
    def link_explicit_data_deps(self, src_task=None, dst_task=None, file_name=None, file_path=None):
        pass
    
    def state(self) -> str:
        return self._state
    
    def task_state_cb(self, task: dict, state: str) -> None:
        self._callback(task, state)
    
    def register_callback(self, func: Callable) -> None:
        self._callback = func
    
    def get_task_states_map(self):
        return self._task_states
    
    async def cancel_task(self, uid: str) -> bool:
        if uid not in self._task_registry:
            raise ValueError(f"Task {uid} not found")

        self._callback(self._task_registry[uid]['description'], 'CANCELED')
        return True

    async def shutdown(self) -> None:
        if self._state == "shutting_down":
            return
        
        logger.info("Shutting down")
        self._state = "shutting_down"
        self._shutdown_event.set()
        
        # Shutdown wait executor
        self._wait_executor.shutdown(wait=True)
        
        # Close Batch
        if self.batch:
            try:
                self.batch.close()
                self.batch.join(timeout=10.0)
            except:
                try:
                    self.batch.terminate()
                except:
                    pass
        
        self._task_registry.clear()
        self._state = "idle"
        logger.info("Shutdown complete")
    
    # Batch features
    def fence(self):
        self.batch.fence()
    
    def create_ddict(self, *args, **kwargs):
        return self.batch.ddict(*args, **kwargs)

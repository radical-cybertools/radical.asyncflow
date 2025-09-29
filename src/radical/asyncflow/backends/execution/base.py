"""Base execution backend compatibility layer.

This module provides a compatibility layer for execution backends to work with
AsyncFlow's type system.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ...constants import StateMapper


@runtime_checkable
class ExecutionBackendProtocol(Protocol):
    """Protocol defining the interface that execution backends must implement.

    This protocol allows both internal AsyncFlow backends and external backends (like
    Rhapsody) to be used with WorkflowEngine type checking.
    """

    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Submit a list of tasks for execution."""
        ...

    async def shutdown(self) -> None:
        """Gracefully shutdown the execution backend."""
        ...

    def state(self) -> str:
        """Get the current state of the execution backend."""
        ...

    def register_callback(self, func) -> None:
        """Register a callback function for task state changes."""
        ...

    def get_task_states_map(self) -> Any:
        """Get the task states mapping."""
        ...

    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task by its UID."""
        ...

    def link_implicit_data_deps(self, src_task, dst_task):
        """Link implicit data dependencies between tasks."""
        ...

    def link_explicit_data_deps(
        self, src_task=None, dst_task=None, file_name=None, file_path=None
    ):
        """Link explicit data dependencies between tasks."""
        ...

    # Required for WorkflowEngine initialization - make it flexible
    session: Any  # Backend must have a session with a path attribute


class BaseExecutionBackend(ABC):
    """Abstract base class for execution backends that manage task execution and state.

    This class defines the interface for execution backends that handle task submission,
    state management, and dependency linking in a distributed or parallel execution
    environment.
    """

    @abstractmethod
    async def submit_tasks(self, tasks: list[dict]) -> None:
        """Submit a list of tasks for execution.

        Args:
            tasks: A list of dictionaries containing task definitions and metadata.
                Each task dictionary should contain the necessary information for
                task execution.
        """

    @abstractmethod
    async def shutdown(self) -> None:
        """Gracefully shutdown the execution backend.

        This method should clean up resources, terminate running tasks if necessary, and
        prepare the backend for termination.
        """

    @abstractmethod
    def state(self) -> str:
        """Get the current state of the execution backend.

        Returns:
            A string representing the current state of the backend (e.g., 'running',
            'idle', 'shutting_down', 'error').
        """

    @abstractmethod
    def task_state_cb(self, task: dict, state: str) -> None:
        """Callback function invoked when a task's state changes.

        Args:
            task: Dictionary containing task information and metadata.
            state: The new state of the task (e.g., 'pending', 'running',
            'completed', 'failed').
        """

    @abstractmethod
    def register_callback(self, func) -> None:
        """Register a callback function for task state changes.

        Args:
            func: A callable that will be invoked when task states change.
                The function should accept task and state parameters.
        """

    @abstractmethod
    def get_task_states_map(self) -> StateMapper:
        """Retrieve a mapping of task IDs to their current states.

        Returns:
            A StateMapper object containing task state mappings.
        """

    @abstractmethod
    def build_task(self, uid, task_desc, task_specific_kwargs) -> None:
        """Build or prepare a task for execution.

        Args:
            uid: Unique identifier for the task.
            task_desc: Dictionary containing task description and metadata.
            task_specific_kwargs: Backend-specific keyword arguments.
        """

    @abstractmethod
    def link_implicit_data_deps(self, src_task, dst_task):
        """Link implicit data dependencies between two tasks.

        Creates a dependency relationship where the destination task depends on
        data produced by the source task, with the dependency being inferred
        automatically.

        Args:
            src_task: The source task that produces data.
            dst_task: The destination task that depends on the source task's output.
        """

    @abstractmethod
    def link_explicit_data_deps(
        self, src_task=None, dst_task=None, file_name=None, file_path=None
    ):
        """Link explicit data dependencies between tasks or files.

        Creates explicit dependency relationships based on specified file names
        or paths, allowing for more precise control over task execution order.

        Args:
            src_task: The source task that produces the dependency.
            dst_task: The destination task that depends on the source.
            file_name: Name of the file that represents the dependency.
            file_path: Full path to the file that represents the dependency.
        """

    @abstractmethod
    async def cancel_task(self, uid: str) -> bool:
        """Cancel a task in the execution backend.

        Args:
            uid: Task identifier

        Returns:
            bool: True if cancellation was successful, False otherwise.
        """


class Session:
    """Manages execution session state and working directory.

    This class maintains session-specific information including the current working
    directory path for task execution.
    """

    def __init__(self):
        """Initialize a new session with the current working directory.

        Sets the session path to the current working directory at the time of
        initialization.
        """
        self.path = os.getcwd()


__all__ = ["BaseExecutionBackend", "Session"]

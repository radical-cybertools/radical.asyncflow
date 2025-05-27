from enum import Enum
from typing import Dict, Any, Union, Optional

class TasksMainStates(Enum):
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    RUNNING = "RUNNING"

class StateMapper:
    """
    StateMapper provides a unified interface for mapping task states between a main workflow
    and various backend systems (e.g., 'radical', 'dask'). It supports dynamic registration
    of backend-specific state mappings and allows conversion between main states and backend
    states.
    Attributes:
        _backend_registry (Dict[str, Dict[TasksMainStates, Any]]): Class-level registry of
            backend names/modules to their state mappings.
    Args:
        backend (Union[str, Any]): The backend identifier, either as a string (e.g., 'radical')
            or as a backend module/object.
    Raises:
        ValueError: If the specified backend is not registered or cannot be detected.
    Methods:
        register_backend_states(cls, backend, done_state, failed_state, canceled_state,
                                running_state, **additional_states):
            Register a new backend's state mappings.
        register_backend_states_with_defaults(cls, backend):
            Register a backend using default main state values.
        _detect_backend_name(self):
            Detect the backend name from the provided module/object.
        __getattr__(self, name):
            Access backend-specific state by main state name (e.g., mapper.DONE).
        to_main_state(self, backend_state):
            Convert a backend-specific state to the corresponding main state.
        get_backend_state(self, main_state):
            Get the backend-specific state for a given main state.
        terminal_states (property):
            Tuple of backend-specific terminal states (DONE, FAILED, CANCELED).
    """
    
    _backend_registry: Dict[str, Dict[TasksMainStates, Any]] = {}
    
    def __init__(self, backend: Union[str, Any]):
        """
        Initialize with either:
        - backend name (str): 'radical', 'dask', etc.
        - backend module/object: radical.pilot or backend instance
        """
        self.backend_name: str
        self.backend_module: Optional[Any] = None

        if isinstance(backend, str):
            self.backend_name = backend.lower()
        else:
            self.backend_module = backend
            self.backend_name = self._detect_backend_name()

        if self.backend_module not in self._backend_registry:
            raise ValueError(
                f"Backend '{self.backend_module}' not registered. "
                f"Available backends: {list(self._backend_registry.keys())}")

        self._state_map = self._backend_registry[self.backend_module]
        self._reverse_map = {v: k for k, v in self._state_map.items()}

    @classmethod
    def register_backend_states(
        cls,
        backend: Any,
        done_state: Any,
        failed_state: Any,
        canceled_state: Any,
        running_state: Any,
        **additional_states) -> None:
        """
        Registers the state mappings for a new backend.

        This method associates a backend identifier with its corresponding task state values.
        It maps the standard task states (DONE, FAILED, CANCELED, RUNNING) to the backend-specific
        state representations, and allows for additional custom state mappings via keyword arguments.

        Args:
            backend (Any): The identifier for the backend to register.
            done_state (Any): The backend's representation of the DONE state.
            failed_state (Any): The backend's representation of the FAILED state.
            canceled_state (Any): The backend's representation of the CANCELED state.
            running_state (Any): The backend's representation of the RUNNING state.
            **additional_states: Additional state mappings, where the key is the state name (str)
                and the value is the backend's representation of that state.

        Returns:
            None
        """
        cls._backend_registry[backend] = {
            TasksMainStates.DONE: done_state,
            TasksMainStates.FAILED: failed_state,
            TasksMainStates.CANCELED: canceled_state,
            TasksMainStates.RUNNING: running_state,
            **{TasksMainStates[k.upper()]: v for k, v in additional_states.items()}}

    @classmethod
    def register_backend_states_with_defaults(cls, backend: Any):

        return cls.register_backend_states(backend,
                                           done_state=TasksMainStates.DONE.value,
                                           failed_state=TasksMainStates.FAILED.value,
                                           canceled_state=TasksMainStates.CANCELED.value,
                                           running_state=TasksMainStates.RUNNING.value)

    def _detect_backend_name(self) -> str:
        """Detect backend name from module/object"""
        if hasattr(self.backend_module, '__name__'):
            module_name = self.backend_module.__name__.lower()
            return module_name

        # Try to detect from class name if it's an instance
        if hasattr(self.backend_module, '__class__'):
            class_name = self.backend_module.__class__.__name__.lower()
            return class_name

        raise ValueError(f"Could not detect backend from {self.backend_module}")
    
    def __getattr__(self, name: str) -> Any:
        """Access states directly like mapper.DONE"""
        try:
            main_state = TasksMainStates[name]
            return self._state_map[main_state]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' has no state '{name}'")

    def to_main_state(self, backend_state: Any) -> TasksMainStates:
        """Convert backend state to main state"""
        try:
            return self._reverse_map[backend_state]
        except KeyError:
            raise ValueError(f"Unknown backend state: {backend_state}")

    def get_backend_state(self, main_state: Union[TasksMainStates, str]) -> Any:
        """Get backend-specific state for a main state"""
        if isinstance(main_state, str):
            main_state = TasksMainStates[main_state]
        return self._state_map[main_state]

    @property
    def terminal_states(self) -> tuple:
        """Get all terminal states for the current backend"""
        return (self.DONE, self.FAILED, self.CANCELED)

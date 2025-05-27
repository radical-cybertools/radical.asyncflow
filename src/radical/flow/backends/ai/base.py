from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class AIBackend(ABC):
    """
    Abstract base class for all AI backends (e.g., OpenAI, Ollama, HuggingFace).
    Provides a standard interface for initialization, model preparation, inference, and shutdown.
    """

    def __init__(self, flow: Any, model_name: str, **config):
        self.flow = flow                         # Workflow engine or execution context
        self.model_name = model_name             # Name of the model (e.g., "llama2", "gpt-4")
        self.config: Dict[str, Any] = config     # Any backend-specific configs
        self.memory: list[dict] = []             # Chat memory / conversation history

    @abstractmethod
    async def prepare(self, **kwargs):
        """
        Prepare the backend (e.g., authenticate, start server, load model).
        This is typically called once before inference.
        """
        pass

    @abstractmethod
    async def infer(self, prompt: str, **kwargs) -> str:
        """
        Run inference using the model. Should return raw or structured output.
        Can optionally update memory/context internally.
        """
        pass

    @abstractmethod
    async def shutdown(self, **kwargs):
        """
        Shutdown or cleanup any resources (e.g., servers, files, memory).
        """
        pass

    def reset_memory(self):
        """
        Reset conversation memory/history. Useful for stateless inference or new sessions.
        """
        self.memory = []

    def inject_context(self, messages: list[dict]):
        """
        Set or override memory explicitly (e.g., for predefined prompts or tools).
        """
        self.memory = messages

    def get_model_name(self) -> str:
        """
        Return the model name used by this backend.
        """
        return self.model_name

    def get_config(self) -> dict:
        """
        Return the backend configuration dictionary.
        """
        return self.config
    def get_memory(self) -> list[dict]:
        """
        Return the current memory/context of the backend.
        """
        return self.memory
    def set_memory(self, memory: list[dict]):
        """
        Set the memory/context of the backend.
        """
        self.memory = memory

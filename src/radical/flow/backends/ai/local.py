from .base import AIBackend
from ollama import AsyncClient
from radical.flow import WorkflowEngine


class OllamaBackend(AIBackend):
    def __init__(self, flow: WorkflowEngine, model_name: str, **kwargs):
        self.flow = flow
        self.model_name = model_name
        self.client = AsyncClient()
        self.max_memory = kwargs.get("max_memory", 10)

        super().__init__(flow, model_name, **kwargs)

    async def prepare(self, **kwargs):
        pass

    async def start(self, **kwargs):
        @self.flow.executable_task(service=True)
        async def launch_ollama():
            return "ollama serve"

        return launch_ollama()

    async def infer(self, prompt: str, **kwargs):
        @self.flow.function_task
        async def chat():
            self.memory.append({"role": "user", "content": prompt})
            self.memory = self.memory[-self.max_memory:]  # Trim if too long

            try:
                response = await self.client.chat(model=self.model_name,
                                                  messages=self.memory,
                                                  **kwargs)
                content = response.get('message', {}).get('content', '').strip()
                self.memory.append({"role": "assistant", "content": content})
                return content
            except Exception as e:
                return f"[Ollama error] {str(e)}"

        return chat()

    async def shutdown(self, **kwargs):
        pass


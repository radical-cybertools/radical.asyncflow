import typeguard
from .base import AIAgent
from radical.flow.backends.ai.base import AIBackend

class DecisionAgent(AIAgent):
    @typeguard.typechecked
    def __init__(self, backend: AIBackend, prompt_template: str, **kwargs):
        super().__init__(**kwargs)
        self.backend = backend
        self.prompt_template = prompt_template

    def ingest(self, input_data): 
        return input_data.strip()

    def analyze(self, percepts):
        return percepts

    def decide(self, analysis):
        prompt = self.prompt_template.format(input=analysis)
        return self.backend.infer(prompt)
    
    def act(self, *args, **kwargs):
        pass

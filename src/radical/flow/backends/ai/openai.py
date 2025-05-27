import openai
from .base import AIBackend

class OpenAIBackend(AIBackend):
    def prepare(self):
        openai.api_key = self.config.get("api_key")

    def infer(self, prompt, **kwargs):
        response = openai.ChatCompletion.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            **kwargs
        )
        return response.choices[0].message.content.strip()

    def shutdown(self):
        pass  # Nothing to shut down for OpenAI

from abc import ABC, abstractmethod

class AIAgent(ABC):
    def __init__(self, memory=None, tools=None, policies=None, dispatcher=None, context=None):
        self.memory = memory
        self.tools = tools or {}
        self.policies = policies or {}
        self.dispatcher = dispatcher
        self.context = context or {}

    def run(self, input_data):
        percepts = self.ingest(input_data)
        analysis = self.analyze(percepts)
        decision = self.decide(analysis)
        result = self.act(decision)
        self.learn(result)
        return result

    @abstractmethod
    def ingest(self, input_data):
        """Input parsing or preprocessing"""
        pass

    @abstractmethod
    def analyze(self, percepts):
        """Context extraction, classification, semantic embedding"""
        pass

    @abstractmethod
    def decide(self, analysis):
        """Core decision-making or planning"""
        pass

    @abstractmethod
    def act(self, decision):
        """Performs or delegates action"""
        pass

    def learn(self, feedback):
        """Optional: update memory or improve models"""
        pass

    def remember(self, key):
        if self.memory:
            return self.memory.get(key)
        return None

    def store(self, key, value):
        if self.memory:
            self.memory[key] = value

from abc import ABC, abstractmethod
from typing import Dict, Any

class ProcessingStep(ABC):
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> None:
        pass
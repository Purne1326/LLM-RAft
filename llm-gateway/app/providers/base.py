from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Optional, Dict, Any

class ProviderResponse(BaseModel):
    content: str
    model: str
    provider: str
    tokens_used: int
    raw_response: Optional[Dict[str, Any]] = None

class BaseProvider(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def generate(self, prompt: str, **kwargs) -> ProviderResponse:
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        pass


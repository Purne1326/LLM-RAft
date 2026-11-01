from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Optional
from app.models import ChatRequest

class RoutingDecision(BaseModel):
    selected_backend: str  # "local" or "cloud"
    reason: str
    confidence: Optional[float] = 1.0

class BaseRouter(ABC):
    @abstractmethod
    async def route(self, request: ChatRequest) -> RoutingDecision:
        pass


from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

class ChatMessage(BaseModel):
    role: str = Field(..., description="Role: system, user, or assistant")
    content: str = Field(..., description="Message text")

class ChatRequest(BaseModel):
    prompt: Optional[str] = None
    messages: Optional[List[ChatMessage]] = None
    max_tokens: Optional[int] = 512
    temperature: Optional[float] = 0.7
    latency_budget_ms: Optional[int] = None
    no_cache: Optional[bool] = False

    def get_prompt_text(self) -> str:
        if self.prompt:
            return self.prompt
        if self.messages:
            return "\n".join(f"{m.role}: {m.content}" for m in self.messages)
        return ""

class ChatResponse(BaseModel):
    id: str
    response: str
    provider: str
    model: str
    cached: bool = False
    latency_ms: float
    route_decision: Optional[str] = None
    route_reason: Optional[str] = None
    tokens_used: Optional[int] = None


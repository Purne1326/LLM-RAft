import re
from app.router.base import BaseRouter, RoutingDecision
from app.models import ChatRequest

HARD_KEYWORDS = re.compile(r"\b(code|algorithm|math|proof|translate|summarise|summarize|refactor|regex|sql)\b", re.IGNORECASE)

class RulesRouter(BaseRouter):
    """
    v1: Rule-based routing based on prompt length, keywords, tokens, and latency budget.
    """
    def __init__(self, length_threshold: int = 150, max_token_threshold: int = 1000):
        self.length_threshold = length_threshold
        self.max_token_threshold = max_token_threshold

    async def route(self, request: ChatRequest) -> RoutingDecision:
        prompt = request.get_prompt_text()

        # Check strict latency budget
        if request.latency_budget_ms is not None and request.latency_budget_ms < 150:
            return RoutingDecision(
                selected_backend="local",
                reason="strict latency budget prefers fast local execution"
            )

        # Keyword checks for complex tasks
        kw_match = HARD_KEYWORDS.search(prompt)
        if kw_match:
            return RoutingDecision(
                selected_backend="cloud",
                reason=f"complex keyword detected: '{kw_match.group(0)}'"
            )

        # Large prompt or high output length requirement
        if len(prompt.split()) > self.length_threshold:
            return RoutingDecision(
                selected_backend="cloud",
                reason=f"long prompt length ({len(prompt.split())} words > {self.length_threshold})"
            )

        if (request.max_tokens or 0) > self.max_token_threshold:
            return RoutingDecision(
                selected_backend="cloud",
                reason=f"large max_tokens requested ({request.max_tokens})"
            )

        # Default to local
        return RoutingDecision(
            selected_backend="local",
            reason="short general conversational query suits local model"
        )


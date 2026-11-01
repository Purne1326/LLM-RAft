import asyncio
import random
from app.providers.base import BaseProvider, ProviderResponse

# Smart contextual responses for common queries so the demo feels alive
DYNAMIC_RESPONSES = {
    "capital of france": "The capital of France is Paris. It is renowned for its art, gastronomy, and cultural landmarks like the Eiffel Tower and the Louvre.",
    "quicksort": """Here is an optimized Quicksort implementation in Python:

def quicksort(arr):
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quicksort(left) + middle + quicksort(right)

# Example usage:
print(quicksort([3, 6, 8, 10, 1, 2, 1]))
# Output: [1, 1, 2, 3, 6, 8, 10]""",
    "raft": "Raft is a consensus algorithm designed for understandability. It coordinates a replicated log across nodes through leader election, log replication, and commitment safety.",
    "tcp": "TCP (Transmission Control Protocol) is connection-oriented and guarantees reliable, ordered byte streams. UDP (User Datagram Protocol) is connectionless, prioritizing low latency over guaranteed delivery."
}

class FakeProvider(BaseProvider):
    """
    Simulated LLM provider for zero-cost testing and load benchmarking.
    Includes smart contextual answer generation for common questions.
    """
    def __init__(
        self,
        name: str = "fake",
        model: str = "mock-gpt",
        base_latency_ms: float = 60.0,
        jitter_ms: float = 20.0,
        failure_rate: float = 0.0,
        canned_response: str = ""
    ):
        super().__init__(name)
        self.model = model
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self.failure_rate = failure_rate
        self.canned_response = canned_response
        self.healthy = True

    async def generate(self, prompt: str, **kwargs) -> ProviderResponse:
        latency = (self.base_latency_ms + random.uniform(0, self.jitter_ms)) / 1000.0
        await asyncio.sleep(latency)

        if random.random() < self.failure_rate:
            raise ConnectionError(f"Simulated upstream failure in {self.name}")

        prompt_lower = prompt.lower()
        matched_answer = None

        for key, ans in DYNAMIC_RESPONSES.items():
            if key in prompt_lower:
                matched_answer = ans
                break

        if matched_answer:
            response_text = matched_answer
        else:
            response_text = (
                f"Generated response for '{prompt}':\n\n"
                f"This is an intelligent response served by the {self.name} backend ({self.model}). "
                f"The request satisfied your prompt constraints and was processed with full semantic fidelity."
            )

        return ProviderResponse(
            content=response_text,
            model=self.model,
            provider=self.name,
            tokens_used=len(response_text.split()) + len(prompt.split())
        )

    async def health_check(self) -> bool:
        return self.healthy

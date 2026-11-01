import httpx
from typing import Optional
from app.providers.base import BaseProvider, ProviderResponse
from app.metrics.logger import get_logger

logger = get_logger("provider_cloud")

class CloudOpenAIProvider(BaseProvider):
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4o-mini"
    ):
        super().__init__(name="cloud_openai")
        self.api_key = api_key or "mock-key"
        self.base_url = base_url.rstrip("/")
        self.model = model

    async def generate(self, prompt: str, **kwargs) -> ProviderResponse:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": kwargs.get("max_tokens", 512),
            "temperature": kwargs.get("temperature", 0.7)
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            choice = data["choices"][0]
            usage = data.get("usage", {})
            return ProviderResponse(
                content=choice["message"]["content"],
                model=self.model,
                provider=self.name,
                tokens_used=usage.get("total_tokens", 0),
                raw_response=data
            )

    async def health_check(self) -> bool:
        return bool(self.api_key)


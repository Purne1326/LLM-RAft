import httpx
from app.providers.base import BaseProvider, ProviderResponse
from app.metrics.logger import get_logger

logger = get_logger("provider_ollama")

class OllamaProvider(BaseProvider):
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3.2:1b"):
        super().__init__(name="local_ollama")
        self.base_url = base_url.rstrip("/")
        self.model = model

    async def generate(self, prompt: str, **kwargs) -> ProviderResponse:
        url = f"{self.base_url}/api/generate"
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": kwargs.get("temperature", 0.7),
                "num_predict": kwargs.get("max_tokens", 512)
            }
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return ProviderResponse(
                content=data.get("response", ""),
                model=self.model,
                provider=self.name,
                tokens_used=data.get("eval_count", 0),
                raw_response=data
            )

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                res = await client.get(f"{self.base_url}/api/tags")
                return res.status_code == 200
        except Exception:
            return False


from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    APP_NAME: str = "Hybrid LLM Gateway"
    ENV: str = "development"
    DEBUG: bool = False
    PORT: int = 8000
    
    # Redis configuration
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    
    # Rate Limiting (Token Bucket defaults)
    RATE_LIMIT_CAPACITY: int = 10
    RATE_LIMIT_REFILL_RATE: float = 2.0  # tokens per second
    
    # Semantic Cache
    CACHE_SIMILARITY_THRESHOLD: float = 0.90
    CACHE_DEFAULT_TTL_SECONDS: int = 3600
    CACHE_VECTOR_DIM: int = 384
    
    # Routing
    ROUTER_TYPE: str = "rules"  # "rules", "learned", "cascade"
    
    # Local & Cloud Backend configs
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3.2:1b"
    CLOUD_API_URL: str = "https://api.openai.com/v1"
    CLOUD_API_KEY: Optional[str] = None
    CLOUD_MODEL: str = "gpt-4o-mini"
    
    # Resilience & Concurrency
    LOCAL_CONCURRENCY_LIMIT: int = 2
    LOCAL_QUEUE_CAPACITY: int = 20
    CLOUD_CONCURRENCY_LIMIT: int = 50
    CLOUD_QUEUE_CAPACITY: int = 100
    BACKEND_TIMEOUT_SECONDS: float = 10.0
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 5
    CIRCUIT_BREAKER_RECOVERY_TIME: float = 15.0

    class Config:
        env_file = ".env"
        extra = "allow"

settings = Settings()


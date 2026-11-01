from contextlib import asynccontextmanager
from fastapi import FastAPI
import redis.asyncio as aioredis

from app.config import settings
from app.metrics.logger import get_logger
from app.ratelimit.token_bucket import TokenBucketRateLimiter
from app.cache.semantic_cache import SemanticCache
from app.cache.embeddings import EmbeddingModel
from app.providers.fake import FakeProvider
from app.providers.local_ollama import OllamaProvider
from app.providers.cloud_openai import CloudOpenAIProvider
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.backpressure import BoundedQueueBackpressure
from app.router.v1_rules import RulesRouter
from app.router.v2_learned import LearnedClassifierRouter
from app.router.v3_cascade import CascadeOutputValidator
from app.api.routes import router as api_router

logger = get_logger("main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Redis client (with graceful error handling)
    redis_client = None
    try:
        redis_client = aioredis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            socket_connect_timeout=2.0
        )
        await redis_client.ping()
        logger.info(f"Connected to Redis at {settings.REDIS_HOST}:{settings.REDIS_PORT}")
    except Exception as e:
        logger.warning(f"Could not connect to Redis: {e}. Using resilient in-memory stores.")
        redis_client = None

    # Components
    app.state.redis = redis_client
    app.state.rate_limiter = TokenBucketRateLimiter(
        redis_client=redis_client,
        capacity=settings.RATE_LIMIT_CAPACITY,
        refill_rate=settings.RATE_LIMIT_REFILL_RATE
    )
    
    app.state.embedder = EmbeddingModel(dim=settings.CACHE_VECTOR_DIM)
    app.state.semantic_cache = SemanticCache(
        redis_client=redis_client,
        threshold=settings.CACHE_SIMILARITY_THRESHOLD,
        ttl_seconds=settings.CACHE_DEFAULT_TTL_SECONDS,
        dim=settings.CACHE_VECTOR_DIM
    )

    # Initialize providers (FakeProvider used by default or for testing, local/cloud configured)
    local_backend = FakeProvider(
        name="local_mock",
        model="llama-3.2-1b-quant",
        base_latency_ms=80.0,
        canned_response="Local response processed quickly on-device."
    )
    cloud_backend = FakeProvider(
        name="cloud_mock",
        model="gpt-4o",
        base_latency_ms=250.0,
        canned_response="Cloud high-reasoning response with full fidelity."
    )

    app.state.providers = {
        "local": local_backend,
        "cloud": cloud_backend
    }

    # Circuit breakers
    app.state.circuit_breakers = {
        "local": CircuitBreaker(
            name="local",
            failure_threshold=settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
            recovery_time_seconds=settings.CIRCUIT_BREAKER_RECOVERY_TIME
        ),
        "cloud": CircuitBreaker(
            name="cloud",
            failure_threshold=settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
            recovery_time_seconds=settings.CIRCUIT_BREAKER_RECOVERY_TIME
        )
    }

    # Bounded Queue Backpressure
    app.state.backpressure = {
        "local": BoundedQueueBackpressure("local", max_concurrency=settings.LOCAL_CONCURRENCY_LIMIT, max_queue_size=settings.LOCAL_QUEUE_CAPACITY),
        "cloud": BoundedQueueBackpressure("cloud", max_concurrency=settings.CLOUD_CONCURRENCY_LIMIT, max_queue_size=settings.CLOUD_QUEUE_CAPACITY)
    }

    # Routers
    if settings.ROUTER_TYPE == "learned":
        app.state.active_router = LearnedClassifierRouter(embedder=app.state.embedder)
    else:
        app.state.active_router = RulesRouter()

    logger.info("Hybrid LLM Gateway initialized successfully.")
    yield

    # Teardown
    if redis_client:
        await redis_client.aclose()
    logger.info("Hybrid LLM Gateway shutdown complete.")

app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(api_router)


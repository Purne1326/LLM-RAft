import time
import asyncio
import os
from typing import Tuple, Optional
import redis.asyncio as aioredis
from app.metrics.logger import get_logger

logger = get_logger("ratelimiter")

class TokenBucketRateLimiter:
    def __init__(
        self,
        redis_client: Optional[aioredis.Redis],
        capacity: int = 10,
        refill_rate: float = 2.0
    ):
        self.redis = redis_client
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.lua_sha: Optional[str] = None
        self._local_buckets: dict[str, tuple[float, float]] = {}  # {key: (tokens, last_time)}
        self._lock = asyncio.Lock()
        
        # Load Lua script content
        lua_path = os.path.join(os.path.dirname(__file__), "token_bucket.lua")
        with open(lua_path, "r", encoding="utf-8") as f:
            self.lua_script = f.read()

    async def _ensure_script(self):
        if self.redis and not self.lua_sha:
            try:
                self.lua_sha = await self.redis.script_load(self.lua_script)
            except Exception as e:
                logger.warning(f"Could not load Lua script to Redis: {e}. Falling back to in-memory rate limiter.")
                self.lua_sha = None

    async def allow_request(self, key: str, requested: int = 1) -> Tuple[bool, float]:
        """
        Atomically checks and decrements tokens for the given key.
        Returns: (allowed: bool, remaining_tokens: float)
        """
        now = time.time()
        bucket_key = f"ratelimit:{key}"

        if self.redis:
            try:
                await self._ensure_script()
                if self.lua_sha:
                    res = await self.redis.evalsha(
                        self.lua_sha,
                        1,
                        bucket_key,
                        self.capacity,
                        self.refill_rate,
                        now,
                        requested
                    )
                    allowed = bool(res[0] == 1)
                    remaining = float(res[1])
                    return allowed, remaining
            except Exception as e:
                logger.error(f"Redis rate limiter error, using in-memory fallback: {e}")

        # In-memory thread-safe fallback
        async with self._lock:
            tokens, last_updated = self._local_buckets.get(bucket_key, (float(self.capacity), now))
            delta = max(0.0, now - last_updated)
            tokens = min(float(self.capacity), tokens + delta * self.refill_rate)
            
            if tokens >= requested:
                tokens -= requested
                self._local_buckets[bucket_key] = (tokens, now)
                return True, tokens
            else:
                self._local_buckets[bucket_key] = (tokens, now)
                return False, tokens


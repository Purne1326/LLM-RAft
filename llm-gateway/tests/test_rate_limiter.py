import pytest
import asyncio
from app.ratelimit.token_bucket import TokenBucketRateLimiter

@pytest.mark.asyncio
async def test_token_bucket_concurrency_limit():
    limiter = TokenBucketRateLimiter(redis_client=None, capacity=10, refill_rate=1.0)
    
    # Fire 100 concurrent requests against a limit of 10
    tasks = [limiter.allow_request("client_burst") for _ in range(100)]
    results = await asyncio.gather(*tasks)
    
    allowed_count = sum(1 for allowed, _ in results if allowed)
    rejected_count = sum(1 for allowed, _ in results if not allowed)
    
    # Exactly 10 must succeed and 90 must be rejected
    assert allowed_count == 10
    assert rejected_count == 90

@pytest.mark.asyncio
async def test_token_bucket_refill():
    limiter = TokenBucketRateLimiter(redis_client=None, capacity=2, refill_rate=10.0)
    
    # Consume all tokens
    allowed1, _ = await limiter.allow_request("refill_client")
    allowed2, _ = await limiter.allow_request("refill_client")
    allowed3, _ = await limiter.allow_request("refill_client")
    
    assert allowed1 is True
    assert allowed2 is True
    assert allowed3 is False
    
    # Wait 0.25 seconds (should refill ~2.5 tokens)
    await asyncio.sleep(0.25)
    allowed4, _ = await limiter.allow_request("refill_client")
    assert allowed4 is True


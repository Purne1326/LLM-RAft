import pytest
from app.cache.semantic_cache import SemanticCache

@pytest.mark.asyncio
async def test_semantic_cache_hit_and_namespace_isolation():
    cache = SemanticCache(redis_client=None, threshold=0.85, ttl_seconds=3600)
    
    client_a = "user_alpha"
    client_b = "user_beta"
    prompt = "Explain how quicksort works in python"
    response_text = "Quicksort is a divide-and-conquer algorithm that selects a pivot..."
    
    # Initial lookup should be a miss
    res = await cache.get(client_a, prompt)
    assert res is None
    
    # Populate cache for Client A
    await cache.set(client_a, prompt, response_text, "local", "llama")
    
    # Client A searches same prompt -> HIT
    res_a = await cache.get(client_a, prompt)
    assert res_a is not None
    assert res_a["response"] == response_text
    
    # Client B searches same prompt -> MISS (namespace isolation)
    res_b = await cache.get(client_b, prompt)
    assert res_b is None

@pytest.mark.asyncio
async def test_sensitive_prompts_not_cached():
    cache = SemanticCache(redis_client=None, threshold=0.85)
    sensitive_prompt = "My api_key is secret_123456789, please summarize it."
    
    assert cache.is_cacheable(sensitive_prompt) is False
    await cache.set("user_a", sensitive_prompt, "dummy answer", "cloud", "gpt-4")
    res = await cache.get("user_a", sensitive_prompt)
    assert res is None


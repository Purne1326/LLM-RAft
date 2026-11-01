import asyncio
import time
import numpy as np
from httpx import AsyncClient, ASGITransport
from app.main import app, lifespan

SAMPLE_PROMPTS = [
    # Frequently repeated queries (to test cache hit rate)
    "What is the capital of France?",
    "Explain Python list comprehension.",
    "What is the difference between TCP and UDP?",
    "How does git rebase work?",
    # Paraphrased queries
    "Tell me the capital city of France.",
    "Can you explain list comprehension in Python?",
    # Complex queries that should route to cloud
    "Write a complete Go implementation of Raft consensus with log compaction.",
    "Design a distributed rate limiter using Redis and sliding window log.",
    "Explain the Byzantine Generals Problem and its proof of impossibility with 3 nodes.",
    # Short local queries
    "Hi!",
    "What is 15 * 12?",
    "Give me 3 synonyms for happy."
]

async def run_benchmark(strategy: str, num_requests: int = 50, concurrency: int = 5):
    transport = ASGITransport(app=app)
    latencies = []
    cloud_count = 0
    cache_hits = 0

    semaphore = asyncio.Semaphore(concurrency)

    async def worker(client: AsyncClient, prompt: str):
        nonlocal cloud_count, cache_hits
        async with semaphore:
            start = time.time()
            try:
                res = await client.post(
                    "/v1/chat",
                    json={"prompt": prompt, "no_cache": ("no_cache" in strategy)},
                    headers={"X-API-Key": f"bench_{strategy}"}
                )
                duration = time.time() - start
                latencies.append(duration * 1000.0)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("cached"):
                        cache_hits += 1
                    if data.get("provider") in ["cloud_mock", "cloud_openai"]:
                        cloud_count += 1
            except Exception:
                pass

    async with lifespan(app):
        start_total = time.time()
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            tasks = []
            for i in range(num_requests):
                prompt = SAMPLE_PROMPTS[i % len(SAMPLE_PROMPTS)]
                tasks.append(worker(client, prompt))
            await asyncio.gather(*tasks)

        total_time = time.time() - start_total
        rps = num_requests / max(0.001, total_time)
        
        p50 = float(np.percentile(latencies, 50)) if latencies else 0.0
        p95 = float(np.percentile(latencies, 95)) if latencies else 0.0
        p99 = float(np.percentile(latencies, 99)) if latencies else 0.0
        cloud_pct = (cloud_count / num_requests) * 100 if num_requests else 0.0
        cache_hit_pct = (cache_hits / num_requests) * 100 if num_requests else 0.0

        print(f"\n==========================================")
        print(f" Strategy: {strategy}")
        print(f" Requests: {num_requests} | Concurrency: {concurrency}")
        print(f" Throughput: {rps:.2f} req/s")
        print(f" Latency p50: {p50:.2f}ms | p95: {p95:.2f}ms | p99: {p99:.2f}ms")
        print(f" Cache Hit Rate: {cache_hit_pct:.1f}%")
        print(f" Cloud Traffic Ratio: {cloud_pct:.1f}%")
        print(f" Estimated Cloud Cost Savings: {100.0 - cloud_pct:.1f}%")
        print(f"==========================================")

if __name__ == "__main__":
    print("Running LLM Gateway Benchmark Harness...")
    asyncio.run(run_benchmark("Rule Router + Semantic Cache", num_requests=60, concurrency=8))

import json
import time
import re
from typing import Optional, Dict, Any, Tuple
import numpy as np
try:
    import faiss
    HAS_FAISS = True
except ImportError:
    HAS_FAISS = False
import redis.asyncio as aioredis
from app.cache.embeddings import EmbeddingModel
from app.metrics.prometheus import CACHE_LOOKUPS
from app.metrics.logger import get_logger

logger = get_logger("semantic_cache")

# Patterns for sensitive or time-sensitive data that should bypass cache
SENSITIVE_PATTERNS = [
    re.compile(r"\b(?:api[_-]?key|password|secret|bearer\s+[a-z0-9_\-\.]+)\b", re.IGNORECASE),
    re.compile(r"\b(?:current\s+time|what\s+time\s+is\s+it|today's\s+weather|stock\s+price)\b", re.IGNORECASE)
]

class ClientIndex:
    def __init__(self, dim: int = 384):
        self.dim = dim
        self.entry_ids: list[str] = []
        if HAS_FAISS:
            self.index = faiss.IndexFlatIP(dim)
            self.vectors = None
        else:
            self.index = None
            self.vectors: Optional[np.ndarray] = None

    def add(self, vector: np.ndarray, entry_id: str):
        if HAS_FAISS:
            self.index.add(vector)
        else:
            if self.vectors is None:
                self.vectors = np.array(vector, dtype=np.float32)
            else:
                self.vectors = np.vstack([self.vectors, vector])
        self.entry_ids.append(entry_id)

    def search(self, vector: np.ndarray, top_k: int = 1) -> Tuple[Optional[str], float]:
        if len(self.entry_ids) == 0:
            return None, 0.0

        if HAS_FAISS:
            distances, indices = self.index.search(vector, top_k)
            score = float(distances[0][0])
            idx = indices[0][0]
            if 0 <= idx < len(self.entry_ids):
                return self.entry_ids[idx], score
            return None, 0.0
        else:
            sims = np.dot(self.vectors, vector.T).flatten()
            best_idx = int(np.argmax(sims))
            score = float(sims[best_idx])
            return self.entry_ids[best_idx], score

class SemanticCache:
    def __init__(
        self,
        redis_client: Optional[aioredis.Redis],
        threshold: float = 0.90,
        ttl_seconds: int = 3600,
        dim: int = 384
    ):
        self.redis = redis_client
        self.threshold = threshold
        self.ttl_seconds = ttl_seconds
        self.dim = dim
        self.embedder = EmbeddingModel(dim=dim)
        self._indices: Dict[str, ClientIndex] = {}
        self._local_kv: Dict[str, Dict[str, Any]] = {}

    def _get_client_index(self, client_id: str) -> ClientIndex:
        if client_id not in self._indices:
            self._indices[client_id] = ClientIndex(dim=self.dim)
        return self._indices[client_id]

    def is_cacheable(self, prompt: str) -> bool:
        for pat in SENSITIVE_PATTERNS:
            if pat.search(prompt):
                return False
        return True

    async def get(self, client_id: str, prompt: str) -> Optional[Dict[str, Any]]:
        if not self.is_cacheable(prompt):
            CACHE_LOOKUPS.labels(result="miss").inc()
            return None

        vector = self.embedder.encode([prompt])
        client_idx = self._get_client_index(client_id)
        entry_id, similarity = client_idx.search(vector, top_k=1)

        if not entry_id or similarity < self.threshold:
            CACHE_LOOKUPS.labels(result="miss").inc()
            return None

        # Fetch payload from Redis or local store
        cache_key = f"cache:{client_id}:{entry_id}"
        data = None
        if self.redis:
            try:
                raw = await self.redis.get(cache_key)
                if raw:
                    data = json.loads(raw)
            except Exception as e:
                logger.warning(f"Redis get failed: {e}")
        
        if not data:
            data = self._local_kv.get(cache_key)

        if data:
            CACHE_LOOKUPS.labels(result="hit").inc()
            logger.info(f"Semantic cache HIT for client {client_id} (score: {similarity:.3f})")
            return {
                "response": data["response"],
                "provider": data["provider"],
                "model": data["model"],
                "similarity_score": similarity
            }

        CACHE_LOOKUPS.labels(result="miss").inc()
        return None

    async def set(self, client_id: str, prompt: str, response: str, provider: str, model: str):
        if not self.is_cacheable(prompt):
            return

        vector = self.embedder.encode([prompt])
        entry_id = f"entry_{int(time.time() * 1000)}_{len(self._get_client_index(client_id).entry_ids)}"
        client_idx = self._get_client_index(client_id)
        client_idx.add(vector, entry_id)

        cache_key = f"cache:{client_id}:{entry_id}"
        payload = {
            "prompt": prompt,
            "response": response,
            "provider": provider,
            "model": model,
            "timestamp": time.time()
        }

        if self.redis:
            try:
                await self.redis.setex(cache_key, self.ttl_seconds, json.dumps(payload))
            except Exception as e:
                logger.warning(f"Redis set failed: {e}")

        self._local_kv[cache_key] = payload


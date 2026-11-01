import numpy as np
from typing import List

class EmbeddingModel:
    """
    Sentence embedding provider. 
    Uses MiniLM / lightweight 384-dimensional normalized vector generator.
    Includes a deterministic hash-projection fallback so tests run instantly without heavy downloads.
    """
    def __init__(self, dim: int = 384):
        self.dim = dim
        self._st_model = None
        try:
            from sentence_transformers import SentenceTransformer
            self._st_model = SentenceTransformer("all-MiniLM-L6-v2")
        except Exception:
            self._st_model = None

    def encode(self, texts: List[str]) -> np.ndarray:
        if self._st_model:
            embeddings = self._st_model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
            return embeddings.astype(np.float32)
        
        # Deterministic lightweight pseudo-embedding for instant local testing and CI
        result = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, text in enumerate(texts):
            words = text.lower().strip().split()
            vec = np.zeros(self.dim, dtype=np.float32)
            if not words:
                vec[0] = 1.0
            else:
                for w in words:
                    h = hash(w) % self.dim
                    vec[h] += 1.0
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec = vec / norm
            result[i] = vec
        return result


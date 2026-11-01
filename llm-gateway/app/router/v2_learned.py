import numpy as np
from sklearn.linear_model import LogisticRegression
from app.router.base import BaseRouter, RoutingDecision
from app.models import ChatRequest
from app.cache.embeddings import EmbeddingModel

class LearnedClassifierRouter(BaseRouter):
    """
    v2: Learned predictive routing using prompt vector embeddings and a trained classifier.
    """
    def __init__(self, embedder: EmbeddingModel):
        self.embedder = embedder
        self.clf = LogisticRegression(class_weight="balanced")
        self._is_trained = False
        self._bootstrap_sample_data()

    def _bootstrap_sample_data(self):
        # Quick seed training data: 0 = local, 1 = cloud
        prompts = [
            # Local (simple, short, conversational, factual)
            "Hello, how are you today?",
            "What is the capital of France?",
            "Tell me a short joke.",
            "Who wrote Hamlet?",
            "Say yes or no.",
            "What is 2 + 2?",
            "Can you greet me in Spanish?",
            "What is the boiling point of water?",
            # Cloud (complex reasoning, coding, analysis, architecture)
            "Implement a distributed consensus algorithm in Go with log compaction.",
            "Explain quantum computing decoherence and its impact on error correction.",
            "Write a Python script to parse AST and refactor class methods into functions.",
            "Design a multi-region highly available microservice system on AWS.",
            "Translate this entire 20-page legal document into formal Japanese.",
            "Prove why Dijkstra's algorithm fails with negative edge weights.",
            "Optimize this complex SQL query with multiple window functions."
        ]
        labels = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1]
        self.train(prompts, labels)

    def train(self, prompts: list[str], labels: list[int]):
        X = self.embedder.encode(prompts)
        y = np.array(labels)
        self.clf.fit(X, y)
        self._is_trained = True

    async def route(self, request: ChatRequest) -> RoutingDecision:
        prompt = request.get_prompt_text()
        if not self._is_trained:
            return RoutingDecision(selected_backend="cloud", reason="classifier not trained yet")

        X = self.embedder.encode([prompt])
        prob_cloud = float(self.clf.predict_proba(X)[0][1])

        if prob_cloud >= 0.50:
            return RoutingDecision(
                selected_backend="cloud",
                reason=f"learned model predicted cloud complexity ({prob_cloud:.2f})",
                confidence=prob_cloud
            )
        else:
            return RoutingDecision(
                selected_backend="local",
                reason=f"learned model predicted local sufficiency ({1.0 - prob_cloud:.2f})",
                confidence=1.0 - prob_cloud
            )


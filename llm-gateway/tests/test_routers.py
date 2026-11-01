import pytest
from app.models import ChatRequest
from app.router.v1_rules import RulesRouter
from app.router.v2_learned import LearnedClassifierRouter
from app.router.v3_cascade import CascadeOutputValidator
from app.cache.embeddings import EmbeddingModel

@pytest.mark.asyncio
async def test_rules_router():
    router = RulesRouter(length_threshold=50)
    
    # Simple query -> local
    req_simple = ChatRequest(prompt="Hello, what time do banks open?")
    dec1 = await router.route(req_simple)
    assert dec1.selected_backend == "local"
    
    # Coding keyword -> cloud
    req_code = ChatRequest(prompt="Write a Python script to optimize quicksort algorithm")
    dec2 = await router.route(req_code)
    assert dec2.selected_backend == "cloud"
    assert "keyword detected" in dec2.reason
    
    # Latency budget -> local
    req_urgent = ChatRequest(prompt="Write an algorithm for Dijkstra", latency_budget_ms=100)
    dec3 = await router.route(req_urgent)
    assert dec3.selected_backend == "local"

@pytest.mark.asyncio
async def test_learned_classifier_router():
    embedder = EmbeddingModel()
    learned_router = LearnedClassifierRouter(embedder=embedder)
    
    req_easy = ChatRequest(prompt="Tell me a simple one-line joke")
    dec_easy = await learned_router.route(req_easy)
    assert dec_easy.selected_backend in ["local", "cloud"]
    assert dec_easy.confidence is not None

def test_cascade_validator():
    # Good output
    ok, _ = CascadeOutputValidator.is_acceptable(
        "Explain photosynthesis",
        "Photosynthesis is the process by which green plants and certain organisms transform light energy into chemical energy."
    )
    assert ok is True
    
    # Refusal
    bad_refusal, msg = CascadeOutputValidator.is_acceptable(
        "Generate a password",
        "I am sorry, but as an AI, I cannot perform that action."
    )
    assert bad_refusal is False
    assert "refused" in msg
    
    # Repetition
    bad_repetitive, msg = CascadeOutputValidator.is_acceptable(
        "Describe a loop",
        "loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop loop"
    )
    assert bad_repetitive is False
    assert "repetition" in msg


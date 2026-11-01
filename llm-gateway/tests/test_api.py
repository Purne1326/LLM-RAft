from fastapi.testclient import TestClient
from app.main import app

def test_health_check():
    with TestClient(app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_metrics_endpoint():
    with TestClient(app) as client:
        response = client.get("/metrics")
    assert response.status_code == 200
    assert b"gateway_requests_total" in response.content

def test_chat_roundtrip():
    with TestClient(app) as client:
        payload = {
            "prompt": "Hello! What is today's schedule?",
            "max_tokens": 128
        }
        headers = {"X-API-Key": "test_key_123"}
        response = client.post("/v1/chat", json=payload, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "id" in data
    assert data["cached"] is False
    assert len(data["response"]) > 0
    assert data["route_decision"] in ["local", "cloud"]

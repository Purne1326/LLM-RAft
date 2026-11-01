import pytest
import time
from fastapi import HTTPException
from app.resilience.circuit_breaker import CircuitBreaker, State
from app.resilience.backpressure import BoundedQueueBackpressure

def test_circuit_breaker_transitions():
    cb = CircuitBreaker("test_breaker", failure_threshold=3, recovery_time_seconds=0.1)
    
    assert cb.state == State.CLOSED
    assert cb.allow_request() is True
    
    # Record failures
    cb.record_failure()
    cb.record_failure()
    assert cb.state == State.CLOSED
    
    # 3rd failure trips breaker
    cb.record_failure()
    assert cb.state == State.OPEN
    assert cb.allow_request() is False
    
    # Wait for recovery time
    time.sleep(0.15)
    assert cb.allow_request() is True
    assert cb.state == State.HALF_OPEN
    
    # Half-open successes close it
    cb.record_success()
    cb.record_success()
    assert cb.state == State.CLOSED

@pytest.mark.asyncio
async def test_backpressure_queue_shedding():
    bp = BoundedQueueBackpressure("test_backend", max_concurrency=1, max_queue_size=2)
    
    # Fill queue to maximum
    await bp.acquire()  # takes active concurrency slot
    
    # Queue waiting
    bp.current_queued = 2
    
    # Next acquisition should be rejected with 503
    with pytest.raises(HTTPException) as exc_info:
        await bp.acquire()
    
    assert exc_info.value.status_code == 503
    assert "Retry-After" in exc_info.value.headers


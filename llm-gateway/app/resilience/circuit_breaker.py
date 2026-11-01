import time
from enum import Enum
from app.metrics.prometheus import CIRCUIT_BREAKER_STATE, BACKEND_FAILURES
from app.metrics.logger import get_logger

logger = get_logger("circuit_breaker")

class State(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreakerOpenException(Exception):
    pass

class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_time_seconds: float = 15.0,
        half_open_success_threshold: int = 2
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_time_seconds = recovery_time_seconds
        self.half_open_success_threshold = half_open_success_threshold

        self.state: State = State.CLOSED
        self.failure_count: int = 0
        self.success_count: int = 0
        self.last_state_change: float = time.time()
        self._update_metric()

    def _update_metric(self):
        state_map = {State.CLOSED: 0, State.HALF_OPEN: 1, State.OPEN: 2}
        CIRCUIT_BREAKER_STATE.labels(backend=self.name).set(state_map[self.state])

    def allow_request(self) -> bool:
        now = time.time()
        if self.state == State.OPEN:
            if now - self.last_state_change > self.recovery_time_seconds:
                logger.info(f"CircuitBreaker [{self.name}] transitioning from OPEN to HALF_OPEN")
                self.state = State.HALF_OPEN
                self.success_count = 0
                self.last_state_change = now
                self._update_metric()
                return True
            return False
        return True

    def record_success(self):
        if self.state == State.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.half_open_success_threshold:
                logger.info(f"CircuitBreaker [{self.name}] recovered! Transitioning to CLOSED")
                self.state = State.CLOSED
                self.failure_count = 0
                self.success_count = 0
                self.last_state_change = time.time()
                self._update_metric()
        elif self.state == State.CLOSED:
            self.failure_count = 0

    def record_failure(self):
        now = time.time()
        self.failure_count += 1
        BACKEND_FAILURES.labels(backend=self.name, failure_type="error").inc()

        if self.state == State.HALF_OPEN:
            logger.warning(f"CircuitBreaker [{self.name}] failed in HALF_OPEN. Re-opening!")
            self.state = State.OPEN
            self.last_state_change = now
            self._update_metric()
        elif self.state == State.CLOSED and self.failure_count >= self.failure_threshold:
            logger.warning(f"CircuitBreaker [{self.name}] hit failure threshold ({self.failure_count}). Trip to OPEN!")
            self.state = State.OPEN
            self.last_state_change = now
            self._update_metric()


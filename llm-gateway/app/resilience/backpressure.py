import asyncio
from fastapi import HTTPException
from app.metrics.prometheus import QUEUE_DEPTH
from app.metrics.logger import get_logger

logger = get_logger("backpressure")

class BoundedQueueBackpressure:
    """
    Guards backend capacity with a concurrency semaphore and bounded wait queue.
    If queue is full, instantly sheds load with 503 and Retry-After header.
    """
    def __init__(self, backend_name: str, max_concurrency: int = 2, max_queue_size: int = 10):
        self.backend_name = backend_name
        self.max_concurrency = max_concurrency
        self.max_queue_size = max_queue_size
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.current_queued: int = 0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            if self.current_queued >= self.max_queue_size:
                logger.warning(f"Backend [{self.backend_name}] queue full ({self.current_queued}/{self.max_queue_size}). Shedding load!")
                raise HTTPException(
                    status_code=503,
                    detail=f"Backend {self.backend_name} overloaded",
                    headers={"Retry-After": "2"}
                )
            self.current_queued += 1
            QUEUE_DEPTH.labels(backend=self.backend_name).set(self.current_queued)

        try:
            await self.semaphore.acquire()
        finally:
            async with self._lock:
                self.current_queued = max(0, self.current_queued - 1)
                QUEUE_DEPTH.labels(backend=self.backend_name).set(self.current_queued)

    def release(self):
        self.semaphore.release()


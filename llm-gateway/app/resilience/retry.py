import asyncio
import random
from typing import Callable, Any
from app.metrics.logger import get_logger

logger = get_logger("retry")

async def retry_with_backoff(
    func: Callable[[], Any],
    max_retries: int = 3,
    base_delay: float = 0.2,
    max_delay: float = 2.0,
    jitter: bool = True
) -> Any:
    """
    Executes func with exponential backoff plus full jitter.
    """
    attempt = 0
    while True:
        try:
            return await func()
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Exceeded max retries ({max_retries}). Final exception: {e}")
                raise
            
            # Exponential backoff: min(max_delay, base_delay * 2^(attempt - 1))
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            if jitter:
                delay = random.uniform(0, delay)
            
            logger.warning(f"Retry attempt {attempt}/{max_retries} failed ({e}). Sleeping {delay:.3f}s...")
            await asyncio.sleep(delay)


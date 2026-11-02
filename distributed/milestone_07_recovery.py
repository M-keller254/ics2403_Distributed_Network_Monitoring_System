"""Retry policy and recovery helpers for transient network failures."""
import random
import time


def retry(operation, attempts=3, base_delay=0.25, max_delay=2.0, sleep=time.sleep):
    """Retry an idempotent operation with bounded exponential backoff.

    The operation should be safe to repeat. This helper does not provide durable
    queuing or exactly-once delivery.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least one")
    for attempt in range(attempts):
        try:
            return operation()
        except Exception:
            if attempt == attempts - 1:
                raise
            delay = min(max_delay, base_delay * (2 ** attempt))
            sleep(delay * random.uniform(0.8, 1.2))

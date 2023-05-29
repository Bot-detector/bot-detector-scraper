import time
import logging

logger = logging.getLogger(__name__)


def timer(func):
    async def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        logger.debug(f"{func.__name__} took {run_time:.4f} seconds")
        return result

    return wrapper

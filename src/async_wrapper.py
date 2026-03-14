"""FleaMarketAI v2 — Async validation wrapper around existing validators.

Phase 1: Wraps synchronous validators with async + rate limiting.
No database schema changes needed.
"""

import asyncio
import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Tuple

from . import validate as sync_validate
from .ratelimit import RateLimiter

log = logging.getLogger(__name__)

# Thread pool for running sync validators
_executor = ThreadPoolExecutor(max_workers=5)


class AsyncValidator:
    """Async validator with rate limiting and concurrency control.
    
    Usage:
        async with AsyncValidator(max_concurrent=5, global_rpm=30) as validator:
            is_valid, message = await validator.validate(key, provider)
    """
    
    def __init__(self, max_concurrent: int = 5, global_rpm: int = 30):
        self.max_concurrent = max_concurrent
        self.global_rpm = global_rpm
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(global_rpm=global_rpm)
    
    async def validate(self, key: str, provider: str) -> Tuple[bool, str]:
        """Validate a single key with rate limiting."""
        async with self.semaphore:
            return await validate_with_rate_limit(key, provider, self.rate_limiter)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        shutdown()
        return False


async def validate_with_rate_limit(
    key: str,
    provider: str,
    rate_limiter: RateLimiter
) -> Tuple[bool, str]:
    """Validate a key with rate limiting.
    
    Wraps the synchronous validator in async with proper rate limiting.
    
    Args:
        key: API key to validate
        provider: Provider name (e.g., 'openai', 'anthropic')
        rate_limiter: RateLimiter instance
        
    Returns:
        (is_valid, message) tuple
    """
    # Get the synchronous validator function
    validator_func = getattr(sync_validate, f"validate_{provider}", None)
    if validator_func is None:
        return False, f"No validator for provider: {provider}"
    
    # Wait for rate limiter
    await rate_limiter.acquire(provider)
    
    # Run sync validator in thread pool
    loop = asyncio.get_event_loop()
    try:
        is_valid, message = await loop.run_in_executor(
            _executor,
            functools.partial(validator_func, key)
        )
        
        # Report to rate limiter
        if is_valid:
            rate_limiter.report_success(provider)
        else:
            # Check if it's a real rejection vs an error
            if any(x in message.lower() for x in ['returned 401', 'returned 403', 'invalid', 'unauthorized']):
                rate_limiter.report_success(provider)  # Request succeeded
            elif 'error' in message.lower() or 'timeout' in message.lower():
                rate_limiter.report_failure(provider)
        
        return is_valid, message
        
    except Exception as e:
        log.exception("Error validating %s key", provider)
        rate_limiter.report_failure(provider)
        return False, f"Validation error: {e}"


async def validate_batch(
    keys: list[Tuple[str, str]],
    max_concurrent: int = 5,
    global_rpm: int = 30
) -> list[Tuple[str, str, bool, str]]:
    """Validate a batch of keys with concurrency control.
    
    Args:
        keys: List of (key, provider) tuples
        max_concurrent: Max concurrent validations
        global_rpm: Global rate limit (requests per minute)
        
    Returns:
        List of (key, provider, is_valid, message) tuples
    """
    rate_limiter = RateLimiter(global_rpm=global_rpm)
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def validate_one(key: str, provider: str) -> Tuple[str, str, bool, str]:
        async with semaphore:
            is_valid, message = await validate_with_rate_limit(key, provider, rate_limiter)
            return key, provider, is_valid, message
    
    # Create tasks for all validations
    tasks = [validate_one(key, provider) for key, provider in keys]
    
    # Run all with progress tracking
    results = []
    for i, coro in enumerate(asyncio.as_completed(tasks)):
        result = await coro
        results.append(result)
        if (i + 1) % 10 == 0:
            log.info("Validated %d/%d keys", i + 1, len(keys))
    
    return results


def shutdown():
    """Shutdown the thread pool. Call on exit."""
    _executor.shutdown(wait=False)

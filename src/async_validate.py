"""Async validation with rate limiting and semaphore control.

FleaMarketAI v2 — Phase 1
Replaces synchronous validation with async, rate-limited version.
"""

import asyncio
import logging
from typing import Callable, Tuple

import aiohttp

from .ratelimit import RateLimiter

log = logging.getLogger(__name__)

# Semaphore to limit concurrent validations
MAX_CONCURRENT = 5


class AsyncValidator:
    """Async validator with rate limiting and concurrency control.
    
    Usage:
        validator = AsyncValidator()
        is_valid, message = await validator.validate(key, provider, validate_func)
    """
    
    def __init__(self, max_concurrent: int = MAX_CONCURRENT, global_rpm: int = 30):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(global_rpm=global_rpm)
        self._session: aiohttp.ClientSession | None = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def validate(
        self,
        key: str,
        provider: str,
        validate_func: Callable[[str, aiohttp.ClientSession], Tuple[bool, str]]
    ) -> Tuple[bool, str]:
        """Validate a key with rate limiting and concurrency control.
        
        Args:
            key: The API key to validate
            provider: Provider name (e.g., 'openai', 'anthropic')
            validate_func: Async function that takes (key, session) and returns (is_valid, message)
        
        Returns:
            (is_valid, message) tuple
        """
        async with self.semaphore:
            # Wait for rate limiter
            await self.rate_limiter.acquire(provider)
            
            session = await self._get_session()
            
            try:
                is_valid, message = await validate_func(key, session)
                
                # Report result to rate limiter
                if is_valid:
                    self.rate_limiter.report_success(provider)
                else:
                    # Only report as failure if it's not a validation rejection
                    # (i.e., the key is actually bad, not just a network error)
                    if "invalid" in message.lower() or "unauthorized" in message.lower():
                        self.rate_limiter.report_success(provider)  # Request succeeded, key is bad
                    else:
                        self.rate_limiter.report_failure(provider)
                
                return is_valid, message
                
            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    log.warning("Rate limited by %s", provider)
                    self.rate_limiter.report_failure(provider, is_rate_limit=True)
                    return False, f"Rate limited by {provider}"
                elif e.status >= 500:
                    log.warning("Server error from %s: %d", provider, e.status)
                    self.rate_limiter.report_failure(provider)
                    return False, f"{provider} server error: {e.status}"
                else:
                    self.rate_limiter.report_success(provider)  # Request completed
                    return False, f"{provider} error: {e.status}"
                    
            except asyncio.TimeoutError:
                log.warning("Timeout validating %s key", provider)
                self.rate_limiter.report_failure(provider)
                return False, f"Timeout validating {provider} key"
                
            except Exception as e:
                log.exception("Unexpected error validating %s key", provider)
                self.rate_limiter.report_failure(provider)
                return False, f"Error: {e}"
    
    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# Provider-specific async validators

async def validate_openai(key: str, session: aiohttp.ClientSession) -> Tuple[bool, str]:
    """Validate OpenAI key."""
    url = "https://api.openai.com/v1/models"
    headers = {"Authorization": f"Bearer {key}"}
    
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            model_count = len(data.get("data", []))
            return True, f"Valid - {model_count} models available"
        elif resp.status == 401:
            return False, "Invalid key (401)"
        else:
            return False, f"HTTP {resp.status}"


async def validate_anthropic(key: str, session: aiohttp.ClientSession) -> Tuple[bool, str]:
    """Validate Anthropic key."""
    url = "https://api.anthropic.com/v1/models"
    headers = {
        "x-api-key": key,
        "anthropic-version": "2023-06-01"
    }
    
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            return True, "Valid key"
        elif resp.status == 401:
            return False, "Invalid key (401)"
        else:
            return False, f"HTTP {resp.status}"


async def validate_groq(key: str, session: aiohttp.ClientSession) -> Tuple[bool, str]:
    """Validate Groq key."""
    url = "https://api.groq.com/openai/v1/models"
    headers = {"Authorization": f"Bearer {key}"}
    
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            return True, "Valid key"
        elif resp.status == 401:
            return False, "Invalid key (401)"
        else:
            return False, f"HTTP {resp.status}"


async def validate_gemini(key: str, session: aiohttp.ClientSession) -> Tuple[bool, str]:
    """Validate Gemini key."""
    # Gemini uses a different pattern - test with models list
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={key}"
    
    async with session.get(url) as resp:
        if resp.status == 200:
            return True, "Valid key"
        elif resp.status == 400:
            return False, "Invalid key (400)"
        else:
            return False, f"HTTP {resp.status}"


# Map provider names to validator functions
ASYNC_VALIDATORS = {
    "openai": validate_openai,
    "anthropic": validate_anthropic,
    "groq": validate_groq,
    "gemini": validate_gemini,
}


async def validate_key(key: str, provider: str, max_retries: int = 2) -> Tuple[bool, str]:
    """Convenience function to validate a single key.
    
    Usage:
        is_valid, message = await validate_key("sk-...", "openai")
    """
    validator = AsyncValidator()
    
    provider = provider.lower()
    if provider not in ASYNC_VALIDATORS:
        return False, f"No async validator for provider: {provider}"
    
    validate_func = ASYNC_VALIDATORS[provider]
    
    for attempt in range(max_retries):
        is_valid, message = await validator.validate(key, provider, validate_func)
        
        # Retry on transient failures
        if not is_valid and ("timeout" in message.lower() or "server error" in message.lower()):
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                log.debug("Retrying %s validation in %d seconds (attempt %d)", provider, wait_time, attempt + 1)
                await asyncio.sleep(wait_time)
                continue
        
        break
    
    await validator.close()
    return is_valid, message

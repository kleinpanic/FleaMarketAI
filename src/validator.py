"""FleaMarketAI v2 Validator Service

Phase 2: Continuously processes validation queue with rate limiting.
Runs as a service (always on).
"""

import asyncio
import logging
import logging.handlers
import sys
import traceback
from pathlib import Path

from . import queue, db, async_wrapper, notify

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "validator.log"

_fmt = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
)
_file_handler.setFormatter(_fmt)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_fmt)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(_file_handler)
log.addHandler(_console_handler)

# Also capture ratelimit logs
logging.getLogger("src.ratelimit").setLevel(logging.INFO)
logging.getLogger("src.ratelimit").addHandler(_file_handler)


async def process_job(validator, job) -> bool:
    """Process a single validation job.
    
    Returns True if job completed (success or permanent failure).
    """
    log.info("Validating %s key from %s (job=%d, attempt=%d)", 
             job.provider, job.source_url, job.id, job.attempts + 1)
    
    try:
        # Validate with rate limiting
        is_valid, message = await async_wrapper.validate_with_rate_limit(
            job.key, job.provider, validator
        )
        
        # Record in database
        db.upsert_key(
            job.provider, 
            job.key, 
            job.source_url,
            is_valid=is_valid,
            validation_msg=message
        )
        
        if is_valid:
            log.info("✓ Valid %s key found!", job.provider)
            notify.send_notification(job.provider, job.key, job.source_url, message)
            return True
        else:
            log.debug("✗ Invalid %s key: %s", job.provider, message[:80])
            return True
            
    except Exception as e:
        log.exception("Error validating %s key", job.provider)
        return False


async def validator_loop():
    """Main validator loop - continuously process queue."""
    log.info("=== FleaMarketAI v2 Validator Started ===")
    
    # Initialize
    queue.init_queue_tables()
    db.init_db()
    
    async with async_wrapper.AsyncValidator(max_concurrent=5, global_rpm=30) as validator:
        consecutive_empty = 0
        
        while True:
            try:
                # Check for stuck jobs periodically
                if consecutive_empty % 10 == 0:
                    stuck = queue.requeue_stuck_jobs(max_age_minutes=30)
                    if stuck:
                        log.info("Re-queued %d stuck jobs", stuck)
                
                # Get next job
                job = queue.dequeue(worker_id="validator-1")
                
                if job is None:
                    consecutive_empty += 1
                    
                    # Log stats every 10 empty cycles
                    if consecutive_empty % 10 == 0:
                        stats = queue.get_queue_stats()
                        log.info("Queue status: %d pending, %d processing", 
                                stats['total_pending'], stats['processing'])
                    
                    await asyncio.sleep(5)
                    continue
                
                consecutive_empty = 0
                
                # Process job
                success = await process_job(validator, job)
                
                # Complete job (remove if success, retry if failed)
                queue.complete_job(job.id, success=success)
                
                # Small pause between jobs
                await asyncio.sleep(0.5)
                
            except Exception as e:
                log.error("Error in validator loop: %s", e)
                log.debug(traceback.format_exc())
                await asyncio.sleep(30)


def main():
    """Entry point."""
    try:
        asyncio.run(validator_loop())
    except KeyboardInterrupt:
        log.info("Validator stopped by user")
    except Exception as e:
        log.error("Fatal error: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()

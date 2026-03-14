"""FleaMarketAI v2 Validator Service

Continuously processes the validation queue with rate limiting.
Respects:
- Don't re-validate known-invalid keys
- Rate limits per provider
- Max concurrent validations

Usage:
    python -m src.validator
"""

import asyncio
import logging
import logging.handlers
import sys
import traceback
from pathlib import Path

from . import db, async_validate, notify

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
    maxBytes=5 * 1024 * 1024,  # 5 MB per file
    backupCount=3,
)
_file_handler.setFormatter(_fmt)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_fmt)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(_file_handler)
log.addHandler(_console_handler)

# Also log ratelimit and async_validate
logging.getLogger("src.ratelimit").setLevel(logging.INFO)
logging.getLogger("src.ratelimit").addHandler(_file_handler)
logging.getLogger("src.async_validate").setLevel(logging.DEBUG)
logging.getLogger("src.async_validate").addHandler(_file_handler)


async def process_key(validator: async_validate.AsyncValidator, key_record: dict) -> bool:
    """Process a single key validation.
    
    Args:
        validator: The AsyncValidator instance
        key_record: Key record from database
        
    Returns:
        True if processed successfully, False on error
    """
    key_id = key_record['id']
    provider = key_record['provider']
    
    # Retrieve the actual key (we need to store it temporarily or pass it)
    # For now, we'll need to store keys in a separate lookup or use the original
    # This is a limitation of the hash-based approach
    
    # NOTE: In the current implementation, we don't have the original key
    # This needs to be addressed - either:
    # 1. Store keys encrypted (safer)
    # 2. Pass keys through a secure queue
    # 3. Keep original key in memory only during discovery
    
    # For Phase 1, we'll use a simpler approach: pass keys directly from discoverer
    # to validator via the queue, not storing them in DB
    
    log.warning("Cannot validate key_id=%d: original key not stored (hash-only)", key_id)
    return False


async def validator_loop():
    """Main validator loop.
    
    Continuously:
    1. Check for keys needing validation
    2. Validate with rate limiting
    3. Record results
    4. Sleep if no work
    """
    log.info("=== FleaMarketAI v2 Validator Started ===")
    
    # Initialize database
    db.init_db()
    
    async with async_validate.AsyncValidator(max_concurrent=5, global_rpm=30) as validator:
        while True:
            try:
                # Get keys needing validation
                keys = db.get_keys_needing_validation(limit=10)
                
                if not keys:
                    log.debug("No keys to validate, sleeping...")
                    await asyncio.sleep(10)
                    continue
                
                log.info("Found %d keys to validate", len(keys))
                
                # Process each key
                for key_record in keys:
                    # Skip if shouldn't validate (e.g., recently checked)
                    if not db.should_validate(key_record['id']):
                        log.debug("Skipping key_id=%d (not due for validation)", key_record['id'])
                        continue
                    
                    # Validate
                    success = await process_key(validator, key_record)
                    
                    if not success:
                        log.warning("Failed to process key_id=%d", key_record['id'])
                
                # Small pause between batches
                await asyncio.sleep(1)
                
            except Exception as e:
                log.error("Error in validator loop: %s", e)
                log.debug(traceback.format_exc())
                await asyncio.sleep(30)  # Longer sleep on error


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

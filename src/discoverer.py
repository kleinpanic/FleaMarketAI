"""FleaMarketAI v2 Discoverer

Phase 2: Only discovers and enqueues - no validation.
Runs on a schedule (every 4 hours).
"""

import logging
import logging.handlers
import os
import sys
import traceback
from pathlib import Path

from . import discover as discover_module, queue

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "discoverer.log"

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


def main():
    """Run one discovery cycle and enqueue all found keys."""
    log.info("=== FleaMarketAI v2 Discoverer Started ===")

    # Initialize queue tables
    queue.init_queue_tables()

    try:
        github_token = os.getenv("GITHUB_TOKEN")
        discoveries = discover_module.discover_keys(github_token=github_token)

        if not discoveries:
            log.info("No new candidates found")
            return

        log.info("Discovered %d candidate key(s)", len(discoveries))

        enqueued = 0
        skipped = 0

        for item in discoveries:
            # v2 tuple: (provider, key, source_url, line_num)
            if len(item) == 4:
                provider, key, source_url, line_num = item
            elif len(item) == 3:
                provider, key, source_url = item
                line_num = None
            else:
                log.warning("Skipping malformed discovery tuple: %r", item)
                skipped += 1
                continue

            if queue.enqueue(key, provider, source_url, source_line=line_num, priority=1):
                enqueued += 1
            else:
                skipped += 1

        log.info("Enqueued %d keys (%d already in queue)", enqueued, skipped)
        depth = queue.get_queue_depth()
        log.info("Queue depth: %d keys waiting for validation", depth)

    except Exception as e:
        log.error("Discovery error: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()

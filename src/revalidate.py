"""FleaMarketAI v2 Re-validation Scheduler

Runs daily to re-check valid keys that haven't been validated in 7+ days.
Enqueues them with lower priority (5) than new finds (priority 1).
"""

import logging
import logging.handlers
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from . import db, queue

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "revalidator.log"

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

REVALIDATE_AGE_DAYS = 7
BATCH_SIZE = 50


def main():
    """Run re-validation scheduler."""
    log.info("=== FleaMarketAI v2 Re-validation Scheduler Started ===")

    queue.init_queue_tables()
    db.init_db()

    try:
        cutoff_date = (datetime.utcnow() - timedelta(days=REVALIDATE_AGE_DAYS)).isoformat()

        with db._connect() as conn:
            cur = conn.execute(
                """
                SELECT k.id, k.key_hash, k.provider, k.source_url, k.source_line,
                       k.last_validated, q.key_encrypted
                FROM keys k
                LEFT JOIN validation_queue q
                  ON q.key_hash = k.key_hash AND q.provider = k.provider
                WHERE k.status = 'valid'
                  AND (k.last_validated IS NULL OR k.last_validated < ?)
                ORDER BY k.last_validated ASC
                LIMIT ?
                """,
                (cutoff_date, BATCH_SIZE),
            )
            rows = cur.fetchall()

        if not rows:
            log.info("No keys need re-validation (all checked within %d days)", REVALIDATE_AGE_DAYS)
            return

        log.info("Found %d keys to re-validate", len(rows))

        from .queue import _key_encryption

        enqueued = 0
        skipped = 0
        unavailable = 0

        for row in rows:
            if not row["key_encrypted"]:
                unavailable += 1
                continue

            try:
                key = _key_encryption.decrypt(row["key_encrypted"])
                if queue.enqueue(
                    key=key,
                    provider=row["provider"],
                    source_url=row["source_url"],
                    source_line=row["source_line"],
                    priority=5,
                ):
                    enqueued += 1
                else:
                    skipped += 1
            except Exception as e:
                log.error("Error processing key %d: %s", row["id"], e)
                skipped += 1

        log.info(
            "Re-validation complete: %d enqueued, %d skipped, %d unavailable",
            enqueued,
            skipped,
            unavailable,
        )

        stats = queue.get_queue_stats()
        log.info(
            "Queue status: %d pending, %d processing",
            stats["total_pending"],
            stats["processing"],
        )

    except Exception as e:
        log.error("Re-validation error: %s", e)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()

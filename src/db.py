"""SQLite helper for storing discovered API keys and validation results.

Schema:
    keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider TEXT NOT NULL,
        api_key TEXT NOT NULL,
        source_url TEXT,
        first_seen TIMESTAMP NOT NULL,
        last_validated TIMESTAMP,
        is_valid BOOLEAN,
        validation_msg TEXT,
        UNIQUE (provider, api_key)       -- enforced at DB level
    )
"""

import datetime
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "keys.db"


def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keys (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                provider        TEXT    NOT NULL,
                api_key         TEXT    NOT NULL,
                source_url      TEXT,
                first_seen      TIMESTAMP NOT NULL,
                last_validated  TIMESTAMP,
                is_valid        BOOLEAN,
                validation_msg  TEXT,
                UNIQUE (provider, api_key)
            )
            """
        )
        # Add the UNIQUE constraint to existing DBs that were created without it.
        # This is idempotent – fails silently if the index already exists.
        try:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_provider_key "
                "ON keys (provider, api_key)"
            )
        except Exception:
            pass
        conn.commit()


def upsert_key(provider, api_key, source_url, is_valid=None, validation_msg=None):
    now = datetime.datetime.utcnow().isoformat(timespec="seconds")
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO keys (provider, api_key, source_url, first_seen, last_validated,
                              is_valid, validation_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (provider, api_key) DO UPDATE SET
                source_url      = excluded.source_url,
                last_validated  = excluded.last_validated,
                is_valid        = excluded.is_valid,
                validation_msg  = excluded.validation_msg
            """,
            (provider, api_key, source_url, now, now, is_valid, validation_msg),
        )
        conn.commit()


def get_all_keys():
    with _connect() as conn:
        cur = conn.execute("SELECT * FROM keys ORDER BY first_seen DESC")
        return [dict(row) for row in cur]


def get_valid_keys():
    """Return only confirmed-valid keys, newest first."""
    with _connect() as conn:
        cur = conn.execute(
            "SELECT * FROM keys WHERE is_valid = 1 ORDER BY first_seen DESC"
        )
        return [dict(row) for row in cur]


def delete_false_positives(provider=None, validation_msg_like=None):
    """Purge records that are known false positives.

    Args:
        provider: if given, restrict deletion to this provider.
        validation_msg_like: SQL LIKE pattern matched against validation_msg,
                             e.g. ``'%No validator%'``.
    """
    where_clauses = ["is_valid = 0"]
    params = []
    if provider:
        where_clauses.append("provider = ?")
        params.append(provider)
    if validation_msg_like:
        where_clauses.append("validation_msg LIKE ?")
        params.append(validation_msg_like)
    sql = f"DELETE FROM keys WHERE {' AND '.join(where_clauses)}"
    with _connect() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount

"""SQLite helper for storing discovered API keys and validation results.

FleaMarketAI v2 — Phase 1
- Adds WAL mode for better concurrency
- Adds indexes for performance
- Adds validation history tracking
- Supports key hashing for privacy
- Prevents re-validation of known-invalid keys

Schema:
    keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key_hash TEXT UNIQUE NOT NULL,  -- SHA256(key) for deduplication
        key_preview TEXT,               -- First 8 chars for display
        provider TEXT NOT NULL,
        status TEXT CHECK(status IN ('pending', 'valid', 'invalid', 'expired')),
        source_url TEXT,
        source_line INTEGER,
        first_seen TIMESTAMP,
        last_validated TIMESTAMP,
        validation_count INTEGER DEFAULT 0
    )
    
    validations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key_id INTEGER REFERENCES keys(id),
        validated_at TIMESTAMP,
        result BOOLEAN,
        response_time_ms INTEGER,
        error_message TEXT
    )
"""

import datetime
import hashlib
import logging
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "keys.db"
log = logging.getLogger(__name__)


def _connect():
    """Create database connection with WAL mode enabled."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    
    return conn


def _hash_key(key: str) -> str:
    """Create SHA256 hash of key for deduplication."""
    return hashlib.sha256(key.encode()).hexdigest()


def _preview_key(key: str) -> str:
    """Create preview of key (first 8 chars only)."""
    return key[:8] + "..." if len(key) > 8 else key


def init_db():
    """Initialize database with v2 schema."""
    with _connect() as conn:
        # Main keys table (v2)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_hash TEXT UNIQUE NOT NULL,
                key_preview TEXT,
                provider TEXT NOT NULL,
                status TEXT CHECK(status IN ('pending', 'valid', 'invalid', 'expired')) DEFAULT 'pending',
                source_url TEXT,
                source_line INTEGER,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_validated TIMESTAMP,
                validation_count INTEGER DEFAULT 0
            )
            """
        )
        
        # Validation history
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS validations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_id INTEGER REFERENCES keys(id),
                validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                result BOOLEAN,
                response_time_ms INTEGER,
                error_message TEXT
            )
            """
        )
        
        # Indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_keys_provider ON keys(provider)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_keys_status ON keys(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_keys_last_validated ON keys(last_validated)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_validations_key_id ON validations(key_id)")
        
        conn.commit()
        log.info("Database initialized at %s", DB_PATH)


def key_exists(key: str, provider: str) -> bool:
    """Check if a key already exists in the database.
    
    Args:
        key: The API key to check
        provider: The provider name
        
    Returns:
        True if key exists, False otherwise
    """
    key_hash = _hash_key(key)
    with _connect() as conn:
        cur = conn.execute(
            "SELECT 1 FROM keys WHERE key_hash = ? AND provider = ?",
            (key_hash, provider)
        )
        return cur.fetchone() is not None


def get_key_by_hash(key: str, provider: str) -> Optional[Dict[str, Any]]:
    """Get key record by key hash and provider.
    
    Args:
        key: The API key (will be hashed)
        provider: The provider name
        
    Returns:
        Key record dict or None
    """
    key_hash = _hash_key(key)
    with _connect() as conn:
        cur = conn.execute(
            "SELECT * FROM keys WHERE key_hash = ? AND provider = ?",
            (key_hash, provider)
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_key(
    key: str,
    provider: str,
    source_url: str,
    source_line: Optional[int] = None
) -> Optional[int]:
    """Insert a new key into the database.
    
    Args:
        key: The API key
        provider: The provider name
        source_url: URL where key was found
        source_line: Line number in source (optional)
        
    Returns:
        Key ID if inserted, None if key already exists
    """
    key_hash = _hash_key(key)
    key_preview = _preview_key(key)
    
    with _connect() as conn:
        try:
            cur = conn.execute(
                """
                INSERT INTO keys (key_hash, key_preview, provider, status, source_url, source_line)
                VALUES (?, ?, ?, 'pending', ?, ?)
                """,
                (key_hash, key_preview, provider, source_url, source_line)
            )
            conn.commit()
            log.debug("Inserted new key for %s (id=%d)", provider, cur.lastrowid)
            return cur.lastrowid
        except sqlite3.IntegrityError:
            # Key already exists
            log.debug("Key already exists for %s, skipping", provider)
            return None


def should_validate(key_id: int) -> bool:
    """Check if a key should be validated.
    
    Returns False if:
    - Key is 'invalid' (already confirmed dead)
    - Key is 'expired' (manual review needed)
    - Key was validated in the last 24 hours (pending retry)
    
    Args:
        key_id: The key ID to check
        
    Returns:
        True if key should be validated
    """
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT status, last_validated, validation_count 
            FROM keys 
            WHERE id = ?
            """,
            (key_id,)
        )
        row = cur.fetchone()
        
        if not row:
            return False
        
        status, last_validated, validation_count = row
        
        # Never re-validate known-invalid keys
        if status == 'invalid':
            return False
        
        # Never re-validate expired keys (manual review needed)
        if status == 'expired':
            return False
        
        # For pending keys, always validate
        if status == 'pending':
            return True
        
        # For valid keys, check when last validated
        if status == 'valid' and last_validated:
            last = datetime.datetime.fromisoformat(last_validated)
            age = datetime.datetime.utcnow() - last
            # Re-validate valid keys every 7 days
            if age.days < 7:
                return False
        
        return True


def record_validation(
    key_id: int,
    is_valid: bool,
    response_time_ms: Optional[int] = None,
    error_message: Optional[str] = None
) -> None:
    """Record a validation attempt.
    
    Args:
        key_id: The key ID
        is_valid: Whether validation succeeded
        response_time_ms: Response time in milliseconds
        error_message: Error message if failed
    """
    now = datetime.datetime.utcnow().isoformat()
    
    # Determine new status
    new_status = 'valid' if is_valid else 'invalid'
    
    with _connect() as conn:
        # Insert validation record
        conn.execute(
            """
            INSERT INTO validations (key_id, result, response_time_ms, error_message)
            VALUES (?, ?, ?, ?)
            """,
            (key_id, is_valid, response_time_ms, error_message)
        )
        
        # Update key status
        conn.execute(
            """
            UPDATE keys 
            SET status = ?, 
                last_validated = ?, 
                validation_count = validation_count + 1
            WHERE id = ?
            """,
            (new_status, now, key_id)
        )
        
        conn.commit()


def get_key_by_id(key_id: int) -> Optional[Dict[str, Any]]:
    """Get key details by ID."""
    with _connect() as conn:
        cur = conn.execute(
            "SELECT * FROM keys WHERE id = ?",
            (key_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_keys_needing_validation(limit: int = 100) -> List[Dict[str, Any]]:
    """Get keys that need validation.
    
    Prioritizes:
    1. Pending keys (never validated)
    2. Valid keys that haven't been checked in 7+ days
    
    Args:
        limit: Maximum number of keys to return
        
    Returns:
        List of key records
    """
    seven_days_ago = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()
    
    with _connect() as conn:
        # First: pending keys
        cur = conn.execute(
            """
            SELECT * FROM keys 
            WHERE status = 'pending'
            ORDER BY first_seen ASC
            LIMIT ?
            """,
            (limit,)
        )
        pending = [dict(row) for row in cur]
        
        # If we have room, add valid keys needing re-check
        if len(pending) < limit:
            remaining = limit - len(pending)
            cur = conn.execute(
                """
                SELECT * FROM keys 
                WHERE status = 'valid' 
                  AND (last_validated IS NULL OR last_validated < ?)
                ORDER BY last_validated ASC
                LIMIT ?
                """,
                (seven_days_ago, remaining)
            )
            pending.extend(dict(row) for row in cur)
        
        return pending


def get_valid_keys() -> List[Dict[str, Any]]:
    """Return all currently valid keys."""
    with _connect() as conn:
        cur = conn.execute(
            "SELECT * FROM keys WHERE status = 'valid' ORDER BY last_validated DESC"
        )
        return [dict(row) for row in cur]


def get_stats() -> Dict[str, Any]:
    """Get database statistics."""
    with _connect() as conn:
        # Count by status
        cur = conn.execute(
            "SELECT status, COUNT(*) as count FROM keys GROUP BY status"
        )
        status_counts = {row['status']: row['count'] for row in cur}
        
        # Total validations
        cur = conn.execute("SELECT COUNT(*) as count FROM validations")
        total_validations = cur.fetchone()['count']
        
        # Recent validations (last 24h)
        yesterday = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).isoformat()
        cur = conn.execute(
            "SELECT COUNT(*) as count FROM validations WHERE validated_at > ?",
            (yesterday,)
        )
        recent_validations = cur.fetchone()['count']
        
        return {
            'keys_by_status': status_counts,
            'total_keys': sum(status_counts.values()),
            'total_validations': total_validations,
            'validations_last_24h': recent_validations,
        }


def migrate_v1_to_v2() -> int:
    """Migrate data from v1 schema to v2.
    
    Returns:
        Number of keys migrated
    """
    with _connect() as conn:
        # Check if v1 table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='keys'"
        )
        if not cur.fetchone():
            log.info("No v1 data to migrate")
            return 0
        
        # Check if v1 schema (has 'api_key' column)
        cur = conn.execute("PRAGMA table_info(keys)")
        columns = [row['name'] for row in cur]
        
        if 'key_hash' in columns:
            log.info("Already migrated to v2")
            return 0
        
        # Migrate v1 data
        cur = conn.execute(
            "SELECT provider, api_key, source_url, first_seen, last_validated, is_valid FROM keys"
        )
        migrated = 0
        
        for row in cur:
            key_hash = _hash_key(row['api_key'])
            key_preview = _preview_key(row['api_key'])
            
            # Map v1 is_valid to v2 status
            if row['is_valid'] is None:
                status = 'pending'
            elif row['is_valid']:
                status = 'valid'
            else:
                status = 'invalid'
            
            try:
                conn.execute(
                    """
                    INSERT INTO keys_v2 (key_hash, key_preview, provider, status, source_url, 
                                       first_seen, last_validated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (key_hash, key_preview, row['provider'], status, row['source_url'],
                     row['first_seen'], row['last_validated'])
                )
                migrated += 1
            except sqlite3.IntegrityError:
                pass  # Duplicate
        
        conn.commit()
        log.info("Migrated %d keys from v1 to v2", migrated)
        return migrated


# Backward compatibility: v1 functions that delegate to v2

def upsert_key(provider, api_key, source_url, is_valid=None, validation_msg=None):
    """v1 compatibility: Insert or update a key.
    
    Note: In v2, we never update existing keys via this method.
    Use insert_key() for new keys and record_validation() for validation results.
    """
    key_id = insert_key(api_key, provider, source_url)
    if key_id and is_valid is not None:
        record_validation(key_id, is_valid, error_message=validation_msg)


def get_all_keys():
    """v1 compatibility: Return all keys."""
    with _connect() as conn:
        cur = conn.execute("SELECT * FROM keys ORDER BY first_seen DESC")
        return [dict(row) for row in cur]


def delete_false_positives(provider=None, validation_msg_like=None):
    """v1 compatibility: Delete false positives."""
    # In v2, we don't delete but could mark as 'expired' if needed
    # For now, this is a no-op to maintain compatibility
    log.warning("delete_false_positives is deprecated in v2")
    return 0

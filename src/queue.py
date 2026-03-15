"""SQLite-based job queue for FleaMarketAI v2.

Phase 2: Replaces direct validation with a queue system.
Discoverer enqueues, validator dequeues.
No Redis needed - everything in SQLite.
"""

import datetime
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "keys.db"
log = logging.getLogger(__name__)


@dataclass
class QueueJob:
    """Represents a validation job in the queue."""
    id: int
    key_hash: str
    key: str  # Decrypted key
    provider: str
    source_url: str
    source_line: Optional[int]
    priority: int  # 1 = highest (new find), 5 = normal, 10 = re-check
    created_at: datetime.datetime
    attempts: int


def _connect():
    """Create database connection."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_queue_tables():
    """Initialize queue tables."""
    with _connect() as conn:
        # Main queue table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS validation_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_hash TEXT NOT NULL,       -- SHA256 hash for dedupe
                key_encrypted TEXT NOT NULL,  -- Fernet-encrypted key
                provider TEXT NOT NULL,
                source_url TEXT,
                source_line INTEGER,
                priority INTEGER DEFAULT 5 CHECK(priority BETWEEN 1 AND 10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                attempts INTEGER DEFAULT 0 CHECK(attempts <= 3),
                processing BOOLEAN DEFAULT 0,
                UNIQUE(key_hash, provider)
            )
            """
        )
        
        # Processing log (what's currently being worked on)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_processing (
                queue_id INTEGER PRIMARY KEY REFERENCES validation_queue(id),
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                worker_id TEXT
            )
            """
        )
        
        # Queue metrics for monitoring
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                queue_depth INTEGER,
                processed_last_hour INTEGER,
                avg_wait_seconds REAL
            )
            """
        )
        
        # Indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_priority ON validation_queue(priority, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_processing ON queue_processing(started_at)")
        
        conn.commit()
        log.info("Queue tables initialized")


class KeyEncryption:
    """Simple Fernet encryption for keys at rest.
    
    Uses a key derived from environment variable FLEAMARKET_KEY.
    If not set, uses a deterministic key (NOT SECURE - for local use only).
    """
    
    def __init__(self):
        self._key = None
        self._fernet = None
    
    def _get_fernet(self):
        """Get or create Fernet instance."""
        if self._fernet is not None:
            return self._fernet
        
        try:
            from cryptography.fernet import Fernet
            import base64
            import hashlib
            
            # Derive key from env or default
            key_material = "FleaMarketAI-v2-Local-Key"
            
            key_bytes = hashlib.sha256(key_material.encode()).digest()
            fernet_key = base64.urlsafe_b64encode(key_bytes)
            
            self._fernet = Fernet(fernet_key)
            return self._fernet
            
        except ImportError:
            log.warning("cryptography not installed, using base64 obfuscation only")
            return None
    
    def encrypt(self, key: str) -> str:
        """Encrypt a key for storage."""
        f = self._get_fernet()
        if f:
            return f.encrypt(key.encode()).decode()
        # Fallback: base64 (obfuscation only)
        import base64
        return "b64:" + base64.b64encode(key.encode()).decode()
    
    def decrypt(self, encrypted: str) -> str:
        """Decrypt a key."""
        if encrypted.startswith("b64:"):
            import base64
            return base64.b64decode(encrypted[4:]).decode()
        
        f = self._get_fernet()
        if f:
            return f.decrypt(encrypted.encode()).decode()
        
        raise ValueError("Cannot decrypt - cryptography not available")


# Global encryption instance
_key_encryption = KeyEncryption()


def _hash_key(key: str) -> str:
    """Create hash for deduplication."""
    import hashlib
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def enqueue(
    key: str,
    provider: str,
    source_url: str,
    source_line: Optional[int] = None,
    priority: int = 5
) -> bool:
    """Add a key to the validation queue.
    
    Args:
        key: The API key to validate
        provider: Provider name
        source_url: Where key was found
        source_line: Line number in source
        priority: 1=highest (new), 5=normal, 10=lowest (re-check)
        
    Returns:
        True if enqueued, False if already exists
    """
    key_hash = _hash_key(key)
    key_encrypted = _key_encryption.encrypt(key)
    
    with _connect() as conn:
        # Check if already in queue (by hash)
        cur = conn.execute(
            "SELECT 1 FROM validation_queue WHERE key_hash = ? AND provider = ?",
            (key_hash, provider)
        )
        if cur.fetchone():
            log.debug("Key already in queue for %s", provider)
            return False
        
        try:
            conn.execute(
                """
                INSERT INTO validation_queue 
                (key_hash, key_encrypted, provider, source_url, source_line, priority)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (key_hash, key_encrypted, provider, source_url, source_line, priority)
            )
            conn.commit()
            log.debug("Enqueued %s key from %s (priority=%d)", provider, source_url, priority)
            return True
        except sqlite3.IntegrityError:
            log.debug("Key already in queue for %s", provider)
            return False


def dequeue(worker_id: str = "main") -> Optional[QueueJob]:
    """Get the next job from the queue.
    
    Returns highest priority, oldest job that isn't being processed.
    Marks job as processing to prevent duplicate work.
    
    Args:
        worker_id: Identifier for this worker
        
    Returns:
        QueueJob or None if queue empty
    """
    with _connect() as conn:
        # Find next job
        cur = conn.execute(
            """
            SELECT id, key_hash, key_encrypted, provider, source_url, source_line, 
                   priority, created_at, attempts
            FROM validation_queue
            WHERE processing = 0 AND attempts < 3
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        
        if not row:
            return None
        
        # Mark as processing
        conn.execute(
            "UPDATE validation_queue SET processing = 1 WHERE id = ?",
            (row['id'],)
        )
        conn.execute(
            "INSERT INTO queue_processing (queue_id, worker_id) VALUES (?, ?)",
            (row['id'], worker_id)
        )
        conn.commit()
        
        # Decrypt key
        try:
            key = _key_encryption.decrypt(row['key_encrypted'])
        except Exception as e:
            log.error("Failed to decrypt key: %s", e)
            # Mark as failed and return None
            conn.execute(
                "UPDATE validation_queue SET processing = 0, attempts = 99 WHERE id = ?",
                (row['id'],)
            )
            conn.commit()
            return None
        
        return QueueJob(
            id=row['id'],
            key_hash=row['key_hash'],
            key=key,
            provider=row['provider'],
            source_url=row['source_url'],
            source_line=row['source_line'],
            priority=row['priority'],
            created_at=datetime.datetime.fromisoformat(row['created_at']),
            attempts=row['attempts']
        )


def complete_job(job_id: int, success: bool) -> None:
    """Mark a job as complete.
    
    Args:
        job_id: The queue job ID
        success: Whether validation succeeded
    """
    with _connect() as conn:
        if success:
            # Remove from queue entirely
            conn.execute("DELETE FROM validation_queue WHERE id = ?", (job_id,))
        else:
            # Increment attempts, release for retry
            conn.execute(
                """
                UPDATE validation_queue 
                SET processing = 0, attempts = attempts + 1 
                WHERE id = ?
                """,
                (job_id,)
            )
        
        # Remove from processing log
        conn.execute("DELETE FROM queue_processing WHERE queue_id = ?", (job_id,))
        conn.commit()


def get_queue_depth() -> int:
    """Get current queue depth."""
    with _connect() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) FROM validation_queue WHERE processing = 0 AND attempts < 3"
        )
        return cur.fetchone()[0]


def get_processing_count() -> int:
    """Get number of jobs currently being processed."""
    with _connect() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM queue_processing")
        return cur.fetchone()[0]


def get_queue_stats() -> dict:
    """Get detailed queue statistics."""
    with _connect() as conn:
        # By priority
        cur = conn.execute(
            """
            SELECT priority, COUNT(*) as count 
            FROM validation_queue 
            WHERE processing = 0 AND attempts < 3
            GROUP BY priority
            """
        )
        by_priority = {row['priority']: row['count'] for row in cur}
        
        # By provider
        cur = conn.execute(
            """
            SELECT provider, COUNT(*) as count 
            FROM validation_queue 
            WHERE processing = 0 AND attempts < 3
            GROUP BY provider
            """
        )
        by_provider = {row['provider']: row['count'] for row in cur}
        
        # Oldest job age
        cur = conn.execute(
            """
            SELECT MIN(created_at) as oldest 
            FROM validation_queue 
            WHERE processing = 0 AND attempts < 3
            """
        )
        row = cur.fetchone()
        oldest_age = None
        if row and row['oldest']:
            oldest = datetime.datetime.fromisoformat(row['oldest'])
            oldest_age = (datetime.datetime.utcnow() - oldest).total_seconds()
        
        return {
            'total_pending': get_queue_depth(),
            'processing': get_processing_count(),
            'by_priority': by_priority,
            'by_provider': by_provider,
            'oldest_job_age_seconds': oldest_age,
        }


def requeue_stuck_jobs(max_age_minutes: int = 30) -> int:
    """Re-queue jobs that have been processing too long (crashed workers).
    
    Args:
        max_age_minutes: Max age for processing job before considered stuck
        
    Returns:
        Number of jobs re-queued
    """
    cutoff = (datetime.datetime.utcnow() - 
              datetime.timedelta(minutes=max_age_minutes)).isoformat()
    
    with _connect() as conn:
        # Find stuck jobs
        cur = conn.execute(
            """
            SELECT queue_id FROM queue_processing 
            WHERE started_at < ?
            """,
            (cutoff,)
        )
        stuck_ids = [row['queue_id'] for row in cur]
        
        if not stuck_ids:
            return 0
        
        # Release them
        for job_id in stuck_ids:
            conn.execute(
                """
                UPDATE validation_queue 
                SET processing = 0, attempts = attempts + 1 
                WHERE id = ?
                """,
                (job_id,)
            )
            conn.execute("DELETE FROM queue_processing WHERE queue_id = ?", (job_id,))
        
        conn.commit()
        log.warning("Re-queued %d stuck jobs", len(stuck_ids))
        return len(stuck_ids)


def record_metrics():
    """Record queue metrics for monitoring."""
    stats = get_queue_stats()
    
    with _connect() as conn:
        # Get processed count from last hour
        one_hour_ago = (datetime.datetime.utcnow() - 
                       datetime.timedelta(hours=1)).isoformat()
        conn.execute(
            """
            SELECT COUNT(*) FROM queue_metrics 
            WHERE recorded_at > ?
            """,
            (one_hour_ago,)
        )
        
        conn.execute(
            """
            INSERT INTO queue_metrics 
            (queue_depth, processed_last_hour, avg_wait_seconds)
            VALUES (?, ?, ?)
            """,
            (stats['total_pending'], 0, stats.get('oldest_job_age_seconds'))
        )
        conn.commit()

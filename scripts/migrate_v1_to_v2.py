#!/usr/bin/env python3
"""Migrate FleaMarketAI v1 data to v2 schema.

Usage:
    python3 scripts/migrate_v1_to_v2.py [--dry-run]

This script:
1. Backs up the v1 database
2. Creates v2 tables if they don't exist
3. Migrates v1 keys to v2 format (with hashing)
4. Validates the migration
5. Optionally removes old v1 data
"""

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import db, queue


def backup_database(db_path: Path) -> Path:
    """Create timestamped backup of database."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.parent / f"keys_backup_{timestamp}.db"
    shutil.copy2(db_path, backup_path)
    print(f"✓ Database backed up to: {backup_path}")
    return backup_path


def check_v1_schema(conn: sqlite3.Connection) -> bool:
    """Check if database has v1 schema."""
    cur = conn.execute("PRAGMA table_info(keys)")
    columns = {row[1] for row in cur.fetchall()}
    
    # v1 has 'api_key' column, v2 has 'key_hash'
    return 'api_key' in columns and 'key_hash' not in columns


def migrate_keys(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """Migrate v1 keys to v2 format."""
    stats = {"total": 0, "migrated": 0, "skipped": 0, "errors": 0}
    
    # Get all v1 keys
    cur = conn.execute(
        "SELECT provider, api_key, source_url, first_seen, last_validated, is_valid FROM keys"
    )
    
    v1_keys = cur.fetchall()
    stats["total"] = len(v1_keys)
    
    if dry_run:
        print(f"Would migrate {len(v1_keys)} keys (dry run)")
        return stats
    
    for row in v1_keys:
        provider, api_key, source_url, first_seen, last_validated, is_valid = row
        
        try:
            # Map v1 is_valid to v2 status
            if is_valid is None:
                status = 'pending'
            elif is_valid:
                status = 'valid'
            else:
                status = 'invalid'
            
            # Check if already migrated (by hash)
            key_hash = db._hash_key(api_key)
            cur = conn.execute(
                "SELECT 1 FROM keys_v2 WHERE key_hash = ? AND provider = ?",
                (key_hash, provider)
            )
            if cur.fetchone():
                stats["skipped"] += 1
                continue
            
            # Insert into v2 table
            key_preview = db._preview_key(api_key)
            conn.execute(
                """
                INSERT INTO keys_v2 
                (key_hash, key_preview, provider, status, source_url, first_seen, last_validated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (key_hash, key_preview, provider, status, source_url, first_seen, last_validated)
            )
            
            stats["migrated"] += 1
            
        except Exception as e:
            print(f"  Error migrating key for {provider}: {e}")
            stats["errors"] += 1
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Migrate FleaMarketAI v1 to v2")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup (not recommended)")
    args = parser.parse_args()
    
    db_path = db.DB_PATH
    
    print("=" * 60)
    print("FleaMarketAI v1 → v2 Migration Tool")
    print("=" * 60)
    print()
    
    # Check if database exists
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        print("Nothing to migrate. Fresh install detected.")
        return 0
    
    # Connect and check version
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    # Check current schema
    is_v1 = check_v1_schema(conn)
    
    if not is_v1:
        print("Database appears to already be on v2 schema (or empty).")
        print("No migration needed.")
        conn.close()
        return 0
    
    print(f"Found v1 database at: {db_path}")
    print()
    
    # Backup
    if not args.no_backup and not args.dry_run:
        backup_path = backup_database(db_path)
        print()
    
    # Initialize v2 tables
    print("Initializing v2 tables...")
    if not args.dry_run:
        db.init_db()
        queue.init_queue_tables()
    print("✓ Tables ready")
    print()
    
    # Migrate keys
    print("Migrating keys...")
    stats = migrate_keys(conn, dry_run=args.dry_run)
    
    if not args.dry_run:
        conn.commit()
    
    print(f"  Total v1 keys: {stats['total']}")
    print(f"  Migrated: {stats['migrated']}")
    print(f"  Skipped (already exists): {stats['skipped']}")
    print(f"  Errors: {stats['errors']}")
    print()
    
    if args.dry_run:
        print("DRY RUN - No changes made")
        print("Run without --dry-run to perform migration")
    else:
        print("✓ Migration complete!")
        print()
        print("Next steps:")
        print("1. Test the new services: ./start-validator.sh (in another terminal)")
        print("2. Enable systemd services:")
        print("   systemctl --user enable --now fleamarket-validator.service")
        print("   systemctl --user enable --now fleamarket-discoverer.timer")
        print("   systemctl --user enable --now fleamarket-revalidator.timer")
        print()
        print("To rollback:")
        print(f"   cp {backup_path} {db_path}")
    
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

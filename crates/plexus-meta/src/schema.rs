//! Schema definitions and migration system for the metadata store.
//!
//! Uses a simple versioned migration approach: each migration is a SQL string
//! with a version number. On startup, we check the current version and run
//! any pending migrations in order.

use crate::MetaError;
use rusqlite::Connection;

/// Current schema version. Increment when adding new migrations.
const CURRENT_VERSION: u32 = 1;

/// Run all pending migrations.
pub fn run_migrations(conn: &Connection) -> Result<(), MetaError> {
    // Create the migrations tracking table if it doesn't exist
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _plexus_migrations (
            version  INTEGER PRIMARY KEY,
            applied  TEXT NOT NULL DEFAULT (datetime('now')),
            description TEXT
        );",
    )?;

    let current: u32 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM _plexus_migrations",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if current >= CURRENT_VERSION {
        tracing::debug!(version = current, "metadata schema is up to date");
        return Ok(());
    }

    tracing::info!(
        from = current,
        to = CURRENT_VERSION,
        "running metadata migrations"
    );

    for version in (current + 1)..=CURRENT_VERSION {
        let (sql, desc) = migration(version)?;
        conn.execute_batch(sql)?;
        conn.execute(
            "INSERT INTO _plexus_migrations (version, description) VALUES (?1, ?2)",
            rusqlite::params![version, desc],
        )?;
        tracing::info!(version, desc, "applied migration");
    }

    Ok(())
}

/// Return the SQL and description for a given migration version.
fn migration(version: u32) -> Result<(&'static str, &'static str), MetaError> {
    match version {
        1 => Ok((MIGRATION_V1, "initial schema")),
        _ => Err(MetaError::Migration(format!(
            "unknown migration version: {version}"
        ))),
    }
}

/// V1: Initial schema — SSTable manifest, cluster state, storage config.
const MIGRATION_V1: &str = r#"
-- ════════════════════════════════════════════════════════════════════
-- SSTable Manifest: tracks every SSTable file across all levels
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE sstable_manifest (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name       TEXT NOT NULL UNIQUE,
    level           INTEGER NOT NULL DEFAULT 0,
    file_size       INTEGER NOT NULL,
    entry_count     INTEGER NOT NULL,
    min_key         BLOB NOT NULL,
    max_key         BLOB NOT NULL,
    min_timestamp   INTEGER NOT NULL,
    max_timestamp   INTEGER NOT NULL,
    bloom_offset    INTEGER NOT NULL,
    index_offset    INTEGER NOT NULL,
    checksum        TEXT NOT NULL,
    storage_tier    TEXT NOT NULL DEFAULT 'local',
    s3_key          TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    compacted       INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_sstable_level ON sstable_manifest(level);
CREATE INDEX idx_sstable_tier ON sstable_manifest(storage_tier);
CREATE INDEX idx_sstable_key_range ON sstable_manifest(min_key, max_key);

-- ════════════════════════════════════════════════════════════════════
-- Cluster Nodes: current membership view
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE cluster_nodes (
    node_id         TEXT PRIMARY KEY,
    address         TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'alive',
    role            TEXT NOT NULL DEFAULT 'follower',
    last_heartbeat  TEXT NOT NULL DEFAULT (datetime('now')),
    incarnation     INTEGER NOT NULL DEFAULT 0,
    joined_at       TEXT NOT NULL DEFAULT (datetime('now')),
    metadata        TEXT
);

-- ════════════════════════════════════════════════════════════════════
-- Raft State: persistent Raft consensus state
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE raft_state (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
);

-- Initialize with defaults
INSERT INTO raft_state (key, value) VALUES ('current_term', '0');
INSERT INTO raft_state (key, value) VALUES ('voted_for', '');
INSERT INTO raft_state (key, value) VALUES ('commit_index', '0');

-- ════════════════════════════════════════════════════════════════════
-- Raft Log: persistent Raft log entries
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE raft_log (
    log_index   INTEGER PRIMARY KEY,
    term        INTEGER NOT NULL,
    entry_type  TEXT NOT NULL,
    data        BLOB
);

-- ════════════════════════════════════════════════════════════════════
-- Storage Configuration: detected hardware + tier mappings
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE storage_devices (
    device_path     TEXT PRIMARY KEY,
    device_type     TEXT NOT NULL,
    mount_point     TEXT NOT NULL,
    total_bytes     INTEGER NOT NULL,
    available_bytes INTEGER NOT NULL,
    tier            TEXT NOT NULL,
    detected_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE storage_config (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
);

-- Defaults
INSERT INTO storage_config (key, value) VALUES ('s3_tiering_threshold', '0.8');
INSERT INTO storage_config (key, value) VALUES ('compaction_threads', '2');
INSERT INTO storage_config (key, value) VALUES ('memtable_size_mb', '32');
INSERT INTO storage_config (key, value) VALUES ('wal_max_size_mb', '64');
INSERT INTO storage_config (key, value) VALUES ('block_cache_size_mb', '256');

-- ════════════════════════════════════════════════════════════════════
-- S3 Objects: tracks data moved to cold storage
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE s3_objects (
    s3_key              TEXT PRIMARY KEY,
    sstable_file_name   TEXT NOT NULL,
    bucket              TEXT NOT NULL,
    compressed_size     INTEGER NOT NULL,
    original_size       INTEGER NOT NULL,
    uploaded_at         TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed       TEXT,
    FOREIGN KEY (sstable_file_name) REFERENCES sstable_manifest(file_name)
);

-- ════════════════════════════════════════════════════════════════════
-- Users & Auth
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE users (
    username        TEXT PRIMARY KEY,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'read_write',
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    last_login      TEXT
);

-- Default admin user (password: 'plexus' — MUST be changed on first login)
INSERT INTO users (username, password_hash, role) 
VALUES ('admin', '$argon2_placeholder$', 'admin');

-- ════════════════════════════════════════════════════════════════════
-- Namespaces: logical data groupings
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE namespaces (
    name        TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    config      TEXT
);

INSERT INTO namespaces (name) VALUES ('default');
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_migrations_run_cleanly() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        run_migrations(&conn).unwrap();

        // Verify tables exist
        let count: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_%' ESCAPE '\\'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count >= 7, "expected at least 7 tables, got {count}");

        // Verify critical tables by name
        let expected_tables = [
            "sstable_manifest",
            "storage_config",
            "cluster_nodes",
            "raft_state",
            "users",
            "namespaces",
        ];
        for table_name in &expected_tables {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name = ?1",
                    [table_name],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "expected table '{table_name}' to exist");
        }
    }

    #[test]
    fn test_migrations_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();

        let count_before: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        run_migrations(&conn).unwrap(); // Should not error

        let count_after: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            count_before, count_after,
            "idempotent run should not change table count"
        );
    }
}

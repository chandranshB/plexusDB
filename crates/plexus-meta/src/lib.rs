//! # plexus-meta
//!
//! Embedded SQLite metadata store for PlexusDB.
//!
//! Tracks all internal state that doesn't belong on the hot data path:
//! - SSTable manifest (which files exist, their levels, key ranges)
//! - Cluster membership and Raft state
//! - Storage tier configuration
//! - S3 object mappings for cold data
//! - User/auth records
//!
//! SQLite is used strictly for metadata — user data never touches SQLite.

pub mod schema;
pub mod queries;

use std::path::Path;
use parking_lot::Mutex;
use rusqlite::Connection;

/// Errors from the metadata store.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("migration failed: {0}")]
    Migration(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}

/// The metadata store backed by embedded SQLite.
///
/// All access is serialized through a [`Mutex`] since SQLite's write
/// concurrency is limited. This is acceptable because metadata operations
/// are infrequent compared to data I/O.
pub struct MetaStore {
    conn: Mutex<Connection>,
}

impl MetaStore {
    /// Open or create a metadata store at the given path.
    ///
    /// Runs all pending migrations on first open.
    pub fn open(path: &Path) -> Result<Self, MetaError> {
        let conn = Connection::open(path)?;

        // Performance tuning for embedded use
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -8000;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA temp_store = MEMORY;"
        )?;

        let store = Self {
            conn: Mutex::new(conn),
        };

        store.run_migrations()?;

        tracing::info!(path = %path.display(), "metadata store opened");
        Ok(store)
    }

    /// Open an in-memory metadata store (for testing).
    pub fn open_memory() -> Result<Self, MetaError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;

        let store = Self {
            conn: Mutex::new(conn),
        };
        store.run_migrations()?;
        Ok(store)
    }

    /// Run all schema migrations.
    fn run_migrations(&self) -> Result<(), MetaError> {
        let conn = self.conn.lock();
        schema::run_migrations(&conn)?;
        Ok(())
    }

    /// Get a reference to the connection (locked).
    pub fn conn(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.conn.lock()
    }
}

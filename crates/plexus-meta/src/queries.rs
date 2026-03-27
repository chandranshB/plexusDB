//! Prepared statement wrappers for efficient metadata queries.
//!
//! Each function takes a `&Connection` and performs a specific query.
//! Callers should obtain the connection via `MetaStore::conn()`.

use crate::MetaError;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

// ════════════════════════════════════════════════════════════════════
// SSTable Manifest Queries
// ════════════════════════════════════════════════════════════════════

/// Metadata for a single SSTable file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsTableInfo {
    pub id: i64,
    pub file_name: String,
    pub level: u32,
    pub file_size: u64,
    pub entry_count: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub bloom_offset: u64,
    pub index_offset: u64,
    pub checksum: String,
    pub storage_tier: String,
    pub s3_key: Option<String>,
}

/// Register a new SSTable in the manifest.
pub fn insert_sstable(conn: &Connection, info: &SsTableInfo) -> Result<i64, MetaError> {
    conn.execute(
        "INSERT INTO sstable_manifest 
            (file_name, level, file_size, entry_count, min_key, max_key,
             min_timestamp, max_timestamp, bloom_offset, index_offset,
             checksum, storage_tier, s3_key)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            info.file_name,
            info.level,
            info.file_size,
            info.entry_count,
            info.min_key,
            info.max_key,
            info.min_timestamp,
            info.max_timestamp,
            info.bloom_offset,
            info.index_offset,
            info.checksum,
            info.storage_tier,
            info.s3_key,
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Get all SSTables at a given level, ordered by min_key.
pub fn get_sstables_at_level(conn: &Connection, level: u32) -> Result<Vec<SsTableInfo>, MetaError> {
    let mut stmt = conn.prepare(
        "SELECT id, file_name, level, file_size, entry_count, min_key, max_key,
                min_timestamp, max_timestamp, bloom_offset, index_offset,
                checksum, storage_tier, s3_key
         FROM sstable_manifest
         WHERE level = ?1 AND compacted = 0
         ORDER BY min_key",
    )?;

    let rows = stmt.query_map(params![level], |row| {
        Ok(SsTableInfo {
            id: row.get(0)?,
            file_name: row.get(1)?,
            level: row.get(2)?,
            file_size: row.get(3)?,
            entry_count: row.get(4)?,
            min_key: row.get(5)?,
            max_key: row.get(6)?,
            min_timestamp: row.get(7)?,
            max_timestamp: row.get(8)?,
            bloom_offset: row.get(9)?,
            index_offset: row.get(10)?,
            checksum: row.get(11)?,
            storage_tier: row.get(12)?,
            s3_key: row.get(13)?,
        })
    })?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// Get all active (non-compacted) SSTables across all levels.
pub fn get_all_active_sstables(conn: &Connection) -> Result<Vec<SsTableInfo>, MetaError> {
    let mut stmt = conn.prepare(
        "SELECT id, file_name, level, file_size, entry_count, min_key, max_key,
                min_timestamp, max_timestamp, bloom_offset, index_offset,
                checksum, storage_tier, s3_key
         FROM sstable_manifest
         WHERE compacted = 0
         ORDER BY level, min_key",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok(SsTableInfo {
            id: row.get(0)?,
            file_name: row.get(1)?,
            level: row.get(2)?,
            file_size: row.get(3)?,
            entry_count: row.get(4)?,
            min_key: row.get(5)?,
            max_key: row.get(6)?,
            min_timestamp: row.get(7)?,
            max_timestamp: row.get(8)?,
            bloom_offset: row.get(9)?,
            index_offset: row.get(10)?,
            checksum: row.get(11)?,
            storage_tier: row.get(12)?,
            s3_key: row.get(13)?,
        })
    })?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// Mark SSTables as compacted (they'll be garbage collected later).
pub fn mark_compacted(conn: &Connection, file_names: &[&str]) -> Result<(), MetaError> {
    let tx = conn.unchecked_transaction()?;
    for name in file_names {
        tx.execute(
            "UPDATE sstable_manifest SET compacted = 1 WHERE file_name = ?1",
            params![name],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Delete compacted SSTable records.
pub fn purge_compacted(conn: &Connection) -> Result<usize, MetaError> {
    let count = conn.execute("DELETE FROM sstable_manifest WHERE compacted = 1", [])?;
    Ok(count)
}

/// Count SSTables at each level.
pub fn level_counts(conn: &Connection) -> Result<Vec<(u32, u64)>, MetaError> {
    let mut stmt = conn.prepare(
        "SELECT level, COUNT(*) FROM sstable_manifest WHERE compacted = 0 GROUP BY level ORDER BY level"
    )?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

// ════════════════════════════════════════════════════════════════════
// Storage Config Queries
// ════════════════════════════════════════════════════════════════════

/// Get a storage configuration value.
pub fn get_config(conn: &Connection, key: &str) -> Result<Option<String>, MetaError> {
    let result = conn
        .query_row(
            "SELECT value FROM storage_config WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()?;
    Ok(result)
}

/// Set a storage configuration value.
pub fn set_config(conn: &Connection, key: &str, value: &str) -> Result<(), MetaError> {
    conn.execute(
        "INSERT OR REPLACE INTO storage_config (key, value) VALUES (?1, ?2)",
        params![key, value],
    )?;
    Ok(())
}

// ════════════════════════════════════════════════════════════════════
// Cluster Node Queries
// ════════════════════════════════════════════════════════════════════

/// Node membership record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: String,
    pub address: String,
    pub state: String,
    pub role: String,
    pub incarnation: u64,
}

/// Upsert a cluster node.
pub fn upsert_node(conn: &Connection, node: &NodeRecord) -> Result<(), MetaError> {
    conn.execute(
        "INSERT INTO cluster_nodes (node_id, address, state, role, incarnation)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(node_id) DO UPDATE SET
             address = excluded.address,
             state = excluded.state,
             role = excluded.role,
             incarnation = excluded.incarnation,
             last_heartbeat = datetime('now')",
        params![
            node.node_id,
            node.address,
            node.state,
            node.role,
            node.incarnation
        ],
    )?;
    Ok(())
}

/// Get all alive nodes.
pub fn get_alive_nodes(conn: &Connection) -> Result<Vec<NodeRecord>, MetaError> {
    let mut stmt = conn.prepare(
        "SELECT node_id, address, state, role, incarnation
         FROM cluster_nodes WHERE state = 'alive'",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(NodeRecord {
            node_id: row.get(0)?,
            address: row.get(1)?,
            state: row.get(2)?,
            role: row.get(3)?,
            incarnation: row.get(4)?,
        })
    })?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

// ════════════════════════════════════════════════════════════════════
// Raft State Queries
// ════════════════════════════════════════════════════════════════════

/// Get a Raft state value.
pub fn get_raft_state(conn: &Connection, key: &str) -> Result<String, MetaError> {
    conn.query_row(
        "SELECT value FROM raft_state WHERE key = ?1",
        params![key],
        |row| row.get(0),
    )
    .map_err(|e| MetaError::NotFound(format!("raft state key '{key}': {e}")))
}

/// Set a Raft state value.
pub fn set_raft_state(conn: &Connection, key: &str, value: &str) -> Result<(), MetaError> {
    conn.execute(
        "UPDATE raft_state SET value = ?2 WHERE key = ?1",
        params![key, value],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaStore;

    #[test]
    fn test_sstable_crud() {
        let store = MetaStore::open_memory().unwrap();
        let conn = store.conn();

        let info = SsTableInfo {
            id: 0,
            file_name: "l0_001.sst".to_string(),
            level: 0,
            file_size: 1024 * 1024,
            entry_count: 10000,
            min_key: b"aaa".to_vec(),
            max_key: b"zzz".to_vec(),
            min_timestamp: 1000,
            max_timestamp: 2000,
            bloom_offset: 900000,
            index_offset: 950000,
            checksum: "abc123".to_string(),
            storage_tier: "local".to_string(),
            s3_key: None,
        };

        let id = insert_sstable(&conn, &info).unwrap();
        assert!(id > 0);

        let tables = get_sstables_at_level(&conn, 0).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].file_name, "l0_001.sst");

        mark_compacted(&conn, &["l0_001.sst"]).unwrap();
        let tables = get_sstables_at_level(&conn, 0).unwrap();
        assert_eq!(tables.len(), 0); // compacted tables are excluded
    }

    #[test]
    fn test_config() {
        let store = MetaStore::open_memory().unwrap();
        let conn = store.conn();

        let val = get_config(&conn, "memtable_size_mb").unwrap();
        assert_eq!(val, Some("32".to_string()));

        set_config(&conn, "memtable_size_mb", "64").unwrap();
        let val = get_config(&conn, "memtable_size_mb").unwrap();
        assert_eq!(val, Some("64".to_string()));
    }

    #[test]
    fn test_node_upsert() {
        let store = MetaStore::open_memory().unwrap();
        let conn = store.conn();

        let node = NodeRecord {
            node_id: "node-1".to_string(),
            address: "192.168.1.1:9090".to_string(),
            state: "alive".to_string(),
            role: "leader".to_string(),
            incarnation: 1,
        };

        upsert_node(&conn, &node).unwrap();
        let nodes = get_alive_nodes(&conn).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].role, "leader");
    }
}

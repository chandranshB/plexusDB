//! # plexus-core
//!
//! The LSM-Tree storage engine — the beating heart of PlexusDB.
//!
//! ## Architecture
//!
//! ```text
//! Write Path:  Client → WAL → MemTable → (flush) → SSTable L0
//! Read Path:   Client → MemTable → Bloom → SSTable L0..LN → (cache)
//! Background:  Compaction merges L(n) → L(n+1) for read amplification control
//! ```
//!
//! All writes are sequential appends. All reads check MemTable first,
//! then consult Bloom filters to avoid unnecessary disk I/O.

pub mod wal;
pub mod memtable;
pub mod sstable;
pub mod bloom;
pub mod cache;
pub mod iterator;
pub mod compaction;
pub mod manifest;

use std::path::PathBuf;

/// Errors from the storage engine.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("I/O error: {0}")]
    Io(#[from] plexus_io::traits::IoError),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("SSTable error: {0}")]
    SsTable(String),

    #[error("compaction error: {0}")]
    Compaction(String),

    #[error("key not found")]
    NotFound,

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("corruption detected: {0}")]
    Corruption(String),

    #[error("metadata error: {0}")]
    Meta(#[from] plexus_meta::MetaError),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("engine closed")]
    Closed,
}

/// A timestamped key-value entry.
///
/// The timestamp enables MVCC and conflict resolution in the distributed layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // None = tombstone (delete marker)
    pub timestamp: u64,
    pub namespace: String,
}

impl Entry {
    /// Create a new put entry.
    pub fn put(key: Vec<u8>, value: Vec<u8>, timestamp: u64) -> Self {
        Self {
            key,
            value: Some(value),
            timestamp,
            namespace: "default".to_string(),
        }
    }

    /// Create a delete tombstone entry.
    pub fn delete(key: Vec<u8>, timestamp: u64) -> Self {
        Self {
            key,
            value: None,
            timestamp,
            namespace: "default".to_string(),
        }
    }

    /// Is this a tombstone (delete marker)?
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Serialized size in bytes.
    pub fn encoded_size(&self) -> usize {
        // key_len(2) + key + value_len(4) + value + timestamp(8) + namespace_len(2) + namespace + tombstone(1)
        2 + self.key.len()
            + 4
            + self.value.as_ref().map(|v| v.len()).unwrap_or(0)
            + 8
            + 2
            + self.namespace.len()
            + 1
    }

    /// Encode entry to bytes.
    ///
    /// # Panics
    ///
    /// Panics if the key exceeds 65535 bytes or value exceeds ~4GB.
    /// These limits match the on-disk format field widths (u16 / u32).
    pub fn encode(&self) -> Vec<u8> {
        assert!(
            self.key.len() <= u16::MAX as usize,
            "key too large for encoding: {} bytes (max {})",
            self.key.len(),
            u16::MAX
        );
        if let Some(ref val) = self.value {
            assert!(
                val.len() <= u32::MAX as usize,
                "value too large for encoding: {} bytes (max {})",
                val.len(),
                u32::MAX
            );
        }

        let mut buf = Vec::with_capacity(self.encoded_size());

        // Key
        buf.extend_from_slice(&(self.key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.key);

        // Tombstone flag
        buf.push(if self.is_tombstone() { 1 } else { 0 });

        // Value
        if let Some(ref val) = self.value {
            buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
            buf.extend_from_slice(val);
        } else {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }

        // Timestamp
        buf.extend_from_slice(&self.timestamp.to_le_bytes());

        // Namespace
        buf.extend_from_slice(&(self.namespace.len() as u16).to_le_bytes());
        buf.extend_from_slice(self.namespace.as_bytes());

        buf
    }

    /// Decode entry from bytes. Returns (entry, bytes_consumed).
    pub fn decode(data: &[u8]) -> Result<(Self, usize), EngineError> {
        let mut pos = 0;

        if data.len() < 2 {
            return Err(EngineError::Corruption("entry too short".into()));
        }

        // Key
        let key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + key_len > data.len() {
            return Err(EngineError::Corruption("key extends beyond data".into()));
        }
        let key = data[pos..pos + key_len].to_vec();
        pos += key_len;

        // Tombstone
        if pos >= data.len() {
            return Err(EngineError::Corruption("missing tombstone flag".into()));
        }
        let is_tombstone = data[pos] == 1;
        pos += 1;

        // Value
        if pos + 4 > data.len() {
            return Err(EngineError::Corruption("missing value length".into()));
        }
        let val_len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Always validate and advance past value bytes, even for tombstones.
        // Corrupt data may have tombstone=true with val_len>0; we must
        // still skip the value bytes to keep pos aligned with the stream.
        let value = if val_len > 0 {
            if pos + val_len > data.len() {
                return Err(EngineError::Corruption("value extends beyond data".into()));
            }
            let v = if is_tombstone {
                None // tombstone — discard value bytes
            } else {
                Some(data[pos..pos + val_len].to_vec())
            };
            pos += val_len;
            v
        } else {
            None
        };

        // Timestamp
        if pos + 8 > data.len() {
            return Err(EngineError::Corruption("missing timestamp".into()));
        }
        let timestamp = u64::from_le_bytes([
            data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
            data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
        ]);
        pos += 8;

        // Namespace
        if pos + 2 > data.len() {
            return Err(EngineError::Corruption("missing namespace length".into()));
        }
        let ns_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + ns_len > data.len() {
            return Err(EngineError::Corruption("namespace extends beyond data".into()));
        }
        let namespace = String::from_utf8_lossy(&data[pos..pos + ns_len]).to_string();
        pos += ns_len;

        Ok((
            Entry {
                key,
                value,
                timestamp,
                namespace,
            },
            pos,
        ))
    }
}

/// Configuration for the storage engine.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Base directory for all data files.
    pub data_dir: PathBuf,
    /// Maximum MemTable size before flush (bytes).
    pub memtable_size: usize,
    /// Maximum WAL size before rotation (bytes).
    pub wal_max_size: usize,
    /// Block cache size (bytes).
    pub block_cache_size: usize,
    /// Number of compaction threads.
    pub compaction_threads: usize,
    /// SSTable block size (bytes).
    pub block_size: usize,
    /// Bloom filter false positive rate.
    pub bloom_fp_rate: f64,
    /// Compaction level size ratio.
    pub level_ratio: usize,
    /// Maximum number of levels.
    pub max_levels: usize,
    /// Enable zstd compression for SSTable blocks.
    pub compression: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./plexus-data"),
            memtable_size: 32 * 1024 * 1024,       // 32 MB
            wal_max_size: 64 * 1024 * 1024,          // 64 MB
            block_cache_size: 256 * 1024 * 1024,      // 256 MB
            compaction_threads: 2,
            block_size: 4096,
            bloom_fp_rate: 0.01,                     // 1% false positive
            level_ratio: 10,
            max_levels: 7,
            compression: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_encode_decode_put() {
        let entry = Entry::put(b"hello".to_vec(), b"world".to_vec(), 12345);
        let encoded = entry.encode();
        let (decoded, consumed) = Entry::decode(&encoded).unwrap();
        assert_eq!(decoded.key, b"hello");
        assert_eq!(decoded.value, Some(b"world".to_vec()));
        assert_eq!(decoded.timestamp, 12345);
        assert!(!decoded.is_tombstone());
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_entry_encode_decode_delete() {
        let entry = Entry::delete(b"goodbye".to_vec(), 99999);
        let encoded = entry.encode();
        let (decoded, consumed) = Entry::decode(&encoded).unwrap();
        assert_eq!(decoded.key, b"goodbye");
        assert!(decoded.is_tombstone());
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.timestamp, 99999);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_entry_encode_decode_with_namespace() {
        let mut entry = Entry::put(b"ns_key".to_vec(), b"ns_val".to_vec(), 42);
        entry.namespace = "my_namespace".to_string();
        let encoded = entry.encode();
        let (decoded, consumed) = Entry::decode(&encoded).unwrap();
        assert_eq!(decoded.key, b"ns_key");
        assert_eq!(decoded.value, Some(b"ns_val".to_vec()));
        assert_eq!(decoded.namespace, "my_namespace");
        assert_eq!(decoded.timestamp, 42);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_entry_encode_decode_empty_value() {
        // The on-disk format doesn't distinguish Some(b"") from None —
        // both encode as val_len=0 and decode back as None.
        let entry = Entry::put(b"key".to_vec(), b"".to_vec(), 1);
        let encoded = entry.encode();
        let (decoded, consumed) = Entry::decode(&encoded).unwrap();
        assert_eq!(decoded.key, b"key");
        assert_eq!(decoded.value, None); // empty value round-trips to None
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_entry_consecutive_decode() {
        // Simulate how entries are stored back-to-back in an SSTable block
        let e1 = Entry::put(b"key1".to_vec(), b"value1".to_vec(), 1);
        let e2 = Entry::delete(b"key2".to_vec(), 2);
        let e3 = Entry::put(b"key3".to_vec(), b"value3".to_vec(), 3);

        let mut buf = Vec::new();
        buf.extend_from_slice(&e1.encode());
        buf.extend_from_slice(&e2.encode());
        buf.extend_from_slice(&e3.encode());

        let mut pos = 0;
        let (d1, consumed) = Entry::decode(&buf[pos..]).unwrap();
        pos += consumed;
        assert_eq!(d1.key, b"key1");
        assert_eq!(d1.value, Some(b"value1".to_vec()));

        let (d2, consumed) = Entry::decode(&buf[pos..]).unwrap();
        pos += consumed;
        assert_eq!(d2.key, b"key2");
        assert!(d2.is_tombstone());

        let (d3, consumed) = Entry::decode(&buf[pos..]).unwrap();
        pos += consumed;
        assert_eq!(d3.key, b"key3");
        assert_eq!(d3.value, Some(b"value3".to_vec()));

        assert_eq!(pos, buf.len(), "all bytes should be consumed");
    }
}

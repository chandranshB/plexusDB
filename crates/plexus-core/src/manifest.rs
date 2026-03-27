//! Manifest — tracks the current set of active SSTables and their levels.
//!
//! The manifest is the "table of contents" for the storage engine. It records
//! which SSTable files exist, what level they belong to, and their key ranges.
//! It delegates persistence to the SQLite metadata store.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use plexus_meta::MetaStore;
use plexus_meta::queries::{self, SsTableInfo};

use crate::EngineError;

/// The manifest tracks all active SSTables.
pub struct Manifest {
    /// Metadata store (SQLite).
    meta: Arc<MetaStore>,
    /// Cached level structure (rebuilt on changes).
    levels: RwLock<Vec<Vec<SsTableEntry>>>,
    /// Data directory.
    data_dir: PathBuf,
    /// Maximum number of levels.
    max_levels: usize,
}

/// An SSTable entry in the manifest.
#[derive(Debug, Clone)]
pub struct SsTableEntry {
    pub file_name: String,
    pub path: PathBuf,
    pub level: u32,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub file_size: u64,
    pub entry_count: u64,
}

impl Manifest {
    /// Create a new manifest backed by the given metadata store.
    pub fn new(meta: Arc<MetaStore>, data_dir: &Path, max_levels: usize) -> Result<Self, EngineError> {
        let manifest = Self {
            meta,
            levels: RwLock::new(vec![Vec::new(); max_levels]),
            data_dir: data_dir.to_path_buf(),
            max_levels,
        };

        manifest.reload()?;
        Ok(manifest)
    }

    /// Reload the manifest from the metadata store.
    pub fn reload(&self) -> Result<(), EngineError> {
        let conn = self.meta.conn();
        let all = queries::get_all_active_sstables(&conn)?;

        let mut levels = vec![Vec::new(); self.max_levels];

        for info in all {
            let level = info.level as usize;
            if level < self.max_levels {
                levels[level].push(SsTableEntry {
                    path: self.data_dir.join(&info.file_name),
                    file_name: info.file_name,
                    level: info.level,
                    min_key: info.min_key,
                    max_key: info.max_key,
                    file_size: info.file_size,
                    entry_count: info.entry_count,
                });
            }
        }

        *self.levels.write() = levels;
        Ok(())
    }

    /// Register a new SSTable in the manifest.
    pub fn add_sstable(&self, info: &SsTableInfo) -> Result<(), EngineError> {
        {
            let conn = self.meta.conn();
            queries::insert_sstable(&conn, info)?;
        } // MutexGuard dropped here — avoids deadlock in reload()
        self.reload()?;
        Ok(())
    }

    /// Mark SSTables as compacted (pending deletion).
    pub fn mark_compacted(&self, file_names: &[&str]) -> Result<(), EngineError> {
        {
            let conn = self.meta.conn();
            queries::mark_compacted(&conn, file_names)?;
        } // MutexGuard dropped here — avoids deadlock in reload()
        self.reload()?;
        Ok(())
    }

    /// Get all SSTables at a given level.
    pub fn get_level(&self, level: u32) -> Vec<SsTableEntry> {
        let levels = self.levels.read();
        levels
            .get(level as usize)
            .cloned()
            .unwrap_or_default()
    }

    /// Get the number of SSTables at each level.
    pub fn level_counts(&self) -> Vec<(u32, usize)> {
        let levels = self.levels.read();
        levels
            .iter()
            .enumerate()
            .filter(|(_, v)| !v.is_empty())
            .map(|(i, v)| (i as u32, v.len()))
            .collect()
    }

    /// Get total size at a given level.
    pub fn level_size(&self, level: u32) -> u64 {
        let levels = self.levels.read();
        levels
            .get(level as usize)
            .map(|l| l.iter().map(|e| e.file_size).sum())
            .unwrap_or(0)
    }

    /// Get SSTables whose key range overlaps with [min_key, max_key].
    pub fn get_overlapping(
        &self,
        level: u32,
        min_key: &[u8],
        max_key: &[u8],
    ) -> Vec<SsTableEntry> {
        self.get_level(level)
            .into_iter()
            .filter(|sst| {
                // Ranges overlap if: sst.min <= max_key AND sst.max >= min_key
                sst.min_key.as_slice() <= max_key && sst.max_key.as_slice() >= min_key
            })
            .collect()
    }

    /// Total number of active SSTables across all levels.
    pub fn total_sstables(&self) -> usize {
        let levels = self.levels.read();
        levels.iter().map(|l| l.len()).sum()
    }

    /// Total data size across all levels.
    pub fn total_size(&self) -> u64 {
        let levels = self.levels.read();
        levels.iter().flat_map(|l| l.iter()).map(|e| e.file_size).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_empty() {
        let meta = Arc::new(MetaStore::open_memory().unwrap());
        let manifest = Manifest::new(meta, Path::new("/tmp/test"), 7).unwrap();

        assert_eq!(manifest.total_sstables(), 0);
        assert_eq!(manifest.total_size(), 0);
        assert!(manifest.get_level(0).is_empty());
    }

    #[test]
    fn test_manifest_add_and_query() {
        let meta = Arc::new(MetaStore::open_memory().unwrap());
        let manifest = Manifest::new(meta, Path::new("/tmp/test"), 7).unwrap();

        let info = SsTableInfo {
            id: 0,
            file_name: "l0_001.sst".to_string(),
            level: 0,
            file_size: 1024,
            entry_count: 100,
            min_key: b"aaa".to_vec(),
            max_key: b"zzz".to_vec(),
            min_timestamp: 1,
            max_timestamp: 100,
            bloom_offset: 900,
            index_offset: 950,
            checksum: "abc".to_string(),
            storage_tier: "local".to_string(),
            s3_key: None,
        };

        manifest.add_sstable(&info).unwrap();

        assert_eq!(manifest.total_sstables(), 1);
        let level0 = manifest.get_level(0);
        assert_eq!(level0.len(), 1);
        assert_eq!(level0[0].file_name, "l0_001.sst");
    }
}

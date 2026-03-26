//! Leveled compaction engine.
//!
//! Compaction merges smaller SSTables into larger ones to:
//! 1. Reduce read amplification (fewer files to check)
//! 2. Reclaim space from deleted/overwritten entries
//! 3. Maintain sequential disk layout for HDD throughput
//!
//! ## Strategy: Leveled Compaction
//!
//! - **L0**: Unsorted flush output from MemTable (may overlap)
//! - **L1+**: Sorted, non-overlapping key ranges. Each level is ~10x larger.
//! - When a level exceeds its size limit, overlapping SSTables are merged
//!   into the next level.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::iterator::{MergeIterator, VecSource, EntrySource};
use crate::sstable::reader::SsTableReader;
use crate::sstable::writer::{SsTableWriter, SsTableWriterConfig, SsTableBuildResult};
use crate::{EngineConfig, EngineError};

/// Compaction job — describes what SSTables to merge.
#[derive(Debug)]
pub struct CompactionJob {
    /// Level being compacted from.
    pub from_level: u32,
    /// Level being compacted to.
    pub to_level: u32,
    /// SSTable files to merge.
    pub input_files: Vec<PathBuf>,
    /// Output directory for the new SSTable.
    pub output_dir: PathBuf,
}

/// Result of a compaction job.
#[derive(Debug)]
pub struct CompactionResult {
    /// New SSTable files created.
    pub output_files: Vec<SsTableBuildResult>,
    /// Input files that should be deleted.
    pub input_files: Vec<PathBuf>,
    /// Number of entries written.
    pub entries_written: u64,
    /// Number of entries discarded (tombstoned / superseded).
    pub entries_discarded: u64,
}

/// Compaction engine that runs compaction jobs.
pub struct CompactionEngine {
    config: EngineConfig,
    /// Flag to signal shutdown.
    shutdown: Arc<AtomicBool>,
}

impl CompactionEngine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal the compaction engine to shut down.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Determine the max size for a given level.
    pub fn level_max_size(&self, level: u32) -> u64 {
        if level == 0 {
            // L0 triggers compaction based on file count, not size
            return u64::MAX;
        }
        // L1 = 10MB, L2 = 100MB, L3 = 1GB, etc.
        let base = 10 * 1024 * 1024u64; // 10MB
        base * (self.config.level_ratio as u64).pow(level - 1)
    }

    /// Maximum number of L0 files before triggering compaction.
    pub fn l0_compaction_trigger(&self) -> usize {
        4
    }

    /// Execute a compaction job.
    pub fn compact(&self, job: &CompactionJob) -> Result<CompactionResult, EngineError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(EngineError::Compaction("engine is shutting down".into()));
        }

        tracing::info!(
            from = job.from_level,
            to = job.to_level,
            files = job.input_files.len(),
            "starting compaction"
        );

        // Read all input SSTables
        let mut sources: Vec<Box<dyn EntrySource>> = Vec::new();
        let mut total_input_entries = 0u64;

        for path in &job.input_files {
            let reader = SsTableReader::open(path)?;
            total_input_entries += reader.entry_count();
            let entries = reader.iter()?;
            sources.push(Box::new(VecSource::new(entries)));
        }

        // Merge all sources
        let mut merger = MergeIterator::new(sources);

        // Write output SSTable(s)
        let output_path = job.output_dir.join(format!(
            "l{}_{}.sst",
            job.to_level,
            chrono::Utc::now().timestamp_micros()
        ));

        let writer_config = SsTableWriterConfig {
            expected_entries: total_input_entries as usize,
            ..SsTableWriterConfig::from(&self.config)
        };

        let mut writer = SsTableWriter::new(&output_path, writer_config)?;
        let mut entries_written = 0u64;

        while let Some(entry) = merger.next() {
            // During compaction to higher levels, we can drop tombstones
            // if we're sure there are no older versions in lower levels.
            // For safety, we keep tombstones in L0→L1 compaction.
            if entry.is_tombstone() && job.to_level >= 2 {
                // TODO: Check if key exists in lower levels before dropping
                // For now, keep tombstones to be safe
            }

            writer.add(&entry)?;
            entries_written += 1;
        }

        let result = writer.finish()?;

        let entries_discarded = total_input_entries.saturating_sub(entries_written);

        tracing::info!(
            from = job.from_level,
            to = job.to_level,
            input_files = job.input_files.len(),
            entries_written,
            entries_discarded,
            output_size = result.file_size,
            "compaction complete"
        );

        Ok(CompactionResult {
            output_files: vec![result],
            input_files: job.input_files.clone(),
            entries_written,
            entries_discarded,
        })
    }

    /// Plan compaction for L0 → L1.
    ///
    /// Selects all L0 files and any overlapping L1 files.
    pub fn plan_l0_compaction(
        &self,
        l0_files: &[PathBuf],
        l1_files: &[PathBuf],
        output_dir: &Path,
    ) -> Option<CompactionJob> {
        if l0_files.len() < self.l0_compaction_trigger() {
            return None;
        }

        let mut input_files = l0_files.to_vec();
        // For L0→L1, include all L1 files (they might overlap)
        input_files.extend_from_slice(l1_files);

        Some(CompactionJob {
            from_level: 0,
            to_level: 1,
            input_files,
            output_dir: output_dir.to_path_buf(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Entry;
    use crate::sstable::writer::SsTableWriterConfig;
    use tempfile::TempDir;

    fn write_test_sstable(dir: &Path, name: &str, entries: Vec<Entry>) -> PathBuf {
        let path = dir.join(name);
        let config = SsTableWriterConfig::default();
        let mut writer = SsTableWriter::new(&path, config).unwrap();
        for entry in &entries {
            writer.add(entry).unwrap();
        }
        writer.finish().unwrap();
        path
    }

    #[test]
    fn test_basic_compaction() {
        let tmp = TempDir::new().unwrap();
        let input_dir = tmp.path().join("input");
        let output_dir = tmp.path().join("output");
        std::fs::create_dir_all(&input_dir).unwrap();
        std::fs::create_dir_all(&output_dir).unwrap();

        // Create two overlapping SSTables
        let sst1 = write_test_sstable(
            &input_dir,
            "sst1.sst",
            vec![
                Entry::put(b"a".to_vec(), b"1".to_vec(), 1),
                Entry::put(b"c".to_vec(), b"3".to_vec(), 1),
            ],
        );

        let sst2 = write_test_sstable(
            &input_dir,
            "sst2.sst",
            vec![
                Entry::put(b"b".to_vec(), b"2".to_vec(), 2),
                Entry::put(b"c".to_vec(), b"3_new".to_vec(), 2),
            ],
        );

        let engine = CompactionEngine::new(EngineConfig::default());
        let job = CompactionJob {
            from_level: 0,
            to_level: 1,
            input_files: vec![sst1, sst2],
            output_dir: output_dir.clone(),
        };

        let result = engine.compact(&job).unwrap();

        assert_eq!(result.entries_written, 3); // a, b, c (deduplicated)
        assert_eq!(result.output_files.len(), 1);

        // Verify the output
        let reader = SsTableReader::open(&result.output_files[0].path).unwrap();
        let entries = reader.iter().unwrap();
        assert_eq!(entries.len(), 3);
        // "c" should have the newer value
        let c_entry = entries.iter().find(|e| e.key == b"c").unwrap();
        assert_eq!(c_entry.value, Some(b"3_new".to_vec()));
    }
}

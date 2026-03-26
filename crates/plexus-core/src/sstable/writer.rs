//! SSTable writer — builds an SSTable file from sorted entries.
//!
//! Usage:
//! ```ignore
//! let mut writer = SsTableWriter::new(path, config)?;
//! for entry in sorted_entries {
//!     writer.add(entry)?;
//! }
//! let info = writer.finish()?;
//! ```

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use xxhash_rust::xxh3::xxh3_64;

use crate::bloom::BloomFilter;
use crate::sstable::format::*;
use crate::{Entry, EngineConfig, EngineError};

/// Builds an SSTable file from sorted entries.
pub struct SsTableWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    config: SsTableWriterConfig,
    /// Current data block being built.
    current_block: Vec<u8>,
    /// Index entries for completed blocks.
    index: Vec<IndexEntry>,
    /// Bloom filter accumulating all keys.
    bloom: BloomFilter,
    /// First key of the current block.
    block_first_key: Option<Vec<u8>>,
    /// Last key of the current block.
    block_last_key: Option<Vec<u8>>,
    /// Current write offset in the file.
    offset: u64,
    /// Total entry count.
    entry_count: u64,
    /// Blake3 hasher for the entire file.
    hasher: blake3::Hasher,
    /// Min/max keys across the entire SSTable.
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    /// Min/max timestamps.
    min_timestamp: u64,
    max_timestamp: u64,
}

/// Configuration for SSTable writing.
#[derive(Debug, Clone)]
pub struct SsTableWriterConfig {
    pub block_size: usize,
    pub compression: Compression,
    pub bloom_fp_rate: f64,
    pub expected_entries: usize,
}

impl Default for SsTableWriterConfig {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            compression: Compression::Zstd,
            bloom_fp_rate: 0.01,
            expected_entries: 10000,
        }
    }
}

impl From<&EngineConfig> for SsTableWriterConfig {
    fn from(config: &EngineConfig) -> Self {
        Self {
            block_size: config.block_size,
            compression: if config.compression {
                Compression::Zstd
            } else {
                Compression::None
            },
            bloom_fp_rate: config.bloom_fp_rate,
            expected_entries: 10000,
        }
    }
}

/// Result of finishing an SSTable write.
#[derive(Debug)]
pub struct SsTableBuildResult {
    pub path: PathBuf,
    pub file_size: u64,
    pub entry_count: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub bloom_offset: u64,
    pub index_offset: u64,
    pub checksum: String,
    pub block_count: usize,
}

impl SsTableWriter {
    /// Create a new SSTable writer.
    pub fn new(path: &Path, config: SsTableWriterConfig) -> Result<Self, EngineError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| EngineError::SsTable(format!("cannot create dir: {e}")))?;
        }

        let file = File::create(path)
            .map_err(|e| EngineError::SsTable(format!("cannot create SSTable: {e}")))?;

        let bloom = BloomFilter::new(config.expected_entries, config.bloom_fp_rate);

        Ok(Self {
            writer: BufWriter::with_capacity(256 * 1024, file), // 256KB buffer
            path: path.to_path_buf(),
            config,
            current_block: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            index: Vec::new(),
            bloom,
            block_first_key: None,
            block_last_key: None,
            offset: 0,
            entry_count: 0,
            hasher: blake3::Hasher::new(),
            min_key: None,
            max_key: None,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
        })
    }

    /// Add a sorted entry to the SSTable.
    ///
    /// Entries MUST be added in sorted key order.
    pub fn add(&mut self, entry: &Entry) -> Result<(), EngineError> {
        // Track min/max
        if self.min_key.is_none() {
            self.min_key = Some(entry.key.clone());
        }
        self.max_key = Some(entry.key.clone());
        self.min_timestamp = self.min_timestamp.min(entry.timestamp);
        self.max_timestamp = self.max_timestamp.max(entry.timestamp);

        // Add to bloom filter
        self.bloom.insert(&entry.key);

        // Encode entry
        let encoded = entry.encode();

        // Check if current block is full
        if !self.current_block.is_empty()
            && self.current_block.len() + encoded.len() > self.config.block_size
        {
            self.flush_block()?;
        }

        // Track block key range
        if self.block_first_key.is_none() {
            self.block_first_key = Some(entry.key.clone());
        }
        self.block_last_key = Some(entry.key.clone());

        self.current_block.extend_from_slice(&encoded);
        self.entry_count += 1;

        Ok(())
    }

    /// Flush the current data block to disk.
    fn flush_block(&mut self) -> Result<(), EngineError> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Compress block
        let compressed = match self.config.compression {
            Compression::Zstd => {
                zstd::encode_all(self.current_block.as_slice(), 3)
                    .map_err(|e| EngineError::SsTable(format!("compression failed: {e}")))?
            }
            Compression::None => self.current_block.clone(),
        };

        let checksum = xxh3_64(&compressed);

        // Write compressed block
        self.writer
            .write_all(&compressed)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        self.hasher.update(&compressed);

        // Record index entry
        self.index.push(IndexEntry {
            first_key: self.block_first_key.take().unwrap_or_default(),
            last_key: self.block_last_key.take().unwrap_or_default(),
            offset: self.offset,
            length: compressed.len() as u32,
            uncompressed_length: self.current_block.len() as u32,
            checksum,
        });

        self.offset += compressed.len() as u64;
        self.current_block.clear();

        Ok(())
    }

    /// Finish writing the SSTable. Writes meta block, index block, and footer.
    pub fn finish(mut self) -> Result<SsTableBuildResult, EngineError> {
        // Flush remaining data block
        self.flush_block()?;

        let block_count = self.index.len();

        // ── Write Meta Block (Bloom Filter) ──
        let meta_offset = self.offset;
        let bloom_data = self.bloom.to_bytes();
        self.writer
            .write_all(&bloom_data)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        self.hasher.update(&bloom_data);
        self.offset += bloom_data.len() as u64;

        // ── Write Index Block ──
        let index_offset = self.offset;
        let mut index_data = Vec::new();
        // Number of index entries
        index_data.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for entry in &self.index {
            index_data.extend_from_slice(&entry.encode());
        }
        self.writer
            .write_all(&index_data)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        self.hasher.update(&index_data);
        self.offset += index_data.len() as u64;

        // ── Write Footer ──
        let hash = self.hasher.finalize();
        let mut checksum_bytes = [0u8; 8];
        checksum_bytes.copy_from_slice(&hash.as_bytes()[..8]);

        let footer = Footer {
            magic: *MAGIC,
            version: FORMAT_VERSION,
            meta_offset,
            index_offset,
            entry_count: self.entry_count,
            checksum: checksum_bytes,
        };

        let footer_data = footer.encode();
        self.writer
            .write_all(&footer_data)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        self.offset += FOOTER_SIZE as u64;

        // Flush everything
        self.writer
            .flush()
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| EngineError::SsTable(e.to_string()))?;

        let checksum_hex = hex::encode(checksum_bytes);

        tracing::info!(
            path = %self.path.display(),
            entries = self.entry_count,
            blocks = block_count,
            size = self.offset,
            "SSTable written"
        );

        Ok(SsTableBuildResult {
            path: self.path,
            file_size: self.offset,
            entry_count: self.entry_count,
            min_key: self.min_key.unwrap_or_default(),
            max_key: self.max_key.unwrap_or_default(),
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            bloom_offset: meta_offset,
            index_offset,
            checksum: checksum_hex,
            block_count,
        })
    }
}

// We need the hex crate for checksum encoding — small inline implementation
mod hex {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    pub fn encode(data: [u8; 8]) -> String {
        let mut s = String::with_capacity(16);
        for b in data {
            s.push(HEX_CHARS[(b >> 4) as usize] as char);
            s.push(HEX_CHARS[(b & 0x0f) as usize] as char);
        }
        s
    }
}

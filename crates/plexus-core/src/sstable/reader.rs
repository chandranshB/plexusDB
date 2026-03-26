//! SSTable reader — reads entries from an SSTable file.
//!
//! Supports:
//! - Point lookups via bloom filter + binary search on index
//! - Range scans via sequential block iteration
//! - Integrity verification via block checksums

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use xxhash_rust::xxh3::xxh3_64;

use crate::bloom::BloomFilter;
use crate::sstable::format::*;
use crate::{Entry, EngineError};

/// Reads entries from an SSTable file on disk.
pub struct SsTableReader {
    path: PathBuf,
    footer: Footer,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    compression: Compression,
}

impl SsTableReader {
    /// Open an SSTable file for reading.
    pub fn open(path: &Path) -> Result<Self, EngineError> {
        let mut file = File::open(path)
            .map_err(|e| EngineError::SsTable(format!("cannot open {}: {e}", path.display())))?;

        let file_size = file.metadata()
            .map_err(|e| EngineError::SsTable(e.to_string()))?
            .len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(EngineError::SsTable("file too small for SSTable".into()));
        }

        // ── Read Footer ──
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        let footer = Footer::decode(&footer_buf)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;

        // ── Read Bloom Filter (Meta Block) ──
        let bloom_size = footer.index_offset - footer.meta_offset;
        file.seek(SeekFrom::Start(footer.meta_offset))
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        let mut bloom_data = vec![0u8; bloom_size as usize];
        file.read_exact(&mut bloom_data)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        let bloom = BloomFilter::from_bytes(&bloom_data)
            .map_err(|e| EngineError::SsTable(format!("invalid bloom filter: {e}")))?;

        // ── Read Index Block ──
        let index_size = (file_size - FOOTER_SIZE as u64) - footer.index_offset;
        file.seek(SeekFrom::Start(footer.index_offset))
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        let mut index_data = vec![0u8; index_size as usize];
        file.read_exact(&mut index_data)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;

        let index = decode_index(&index_data)?;

        // Detect compression from first block (if any) — default to Zstd
        let compression = Compression::Zstd;

        tracing::debug!(
            path = %path.display(),
            entries = footer.entry_count,
            blocks = index.len(),
            "SSTable opened"
        );

        Ok(Self {
            path: path.to_path_buf(),
            footer,
            index,
            bloom,
            compression,
        })
    }

    /// Check if a key might exist in this SSTable (bloom filter check).
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    /// Get a value by key.
    ///
    /// Returns `None` if the key is not found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>, EngineError> {
        // Step 1: Bloom filter check (fast negative)
        if !self.may_contain(key) {
            return Ok(None);
        }

        // Step 2: Binary search the index for the right block
        let block_idx = self.find_block(key);
        if block_idx.is_none() {
            return Ok(None);
        }
        let block_idx = block_idx.unwrap();

        // Step 3: Read and decompress the block
        let entries = self.read_block(block_idx)?;

        // Step 4: Binary search within the block
        // Since entries may have multiple versions of same key, find latest
        let mut result: Option<Entry> = None;
        for entry in entries {
            if entry.key == key {
                match &result {
                    None => result = Some(entry),
                    Some(existing) if entry.timestamp > existing.timestamp => {
                        result = Some(entry);
                    }
                    _ => {}
                }
            }
        }

        Ok(result)
    }

    /// Read and decompress a data block by index.
    pub fn read_block(&self, block_idx: usize) -> Result<Vec<Entry>, EngineError> {
        if block_idx >= self.index.len() {
            return Err(EngineError::SsTable("block index out of range".into()));
        }

        let idx_entry = &self.index[block_idx];

        let mut file = File::open(&self.path)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;

        file.seek(SeekFrom::Start(idx_entry.offset))
            .map_err(|e| EngineError::SsTable(e.to_string()))?;

        let mut compressed = vec![0u8; idx_entry.length as usize];
        file.read_exact(&mut compressed)
            .map_err(|e| EngineError::SsTable(e.to_string()))?;

        // Verify checksum
        let actual_checksum = xxh3_64(&compressed);
        if actual_checksum != idx_entry.checksum {
            return Err(EngineError::ChecksumMismatch {
                expected: format!("{:016x}", idx_entry.checksum),
                actual: format!("{:016x}", actual_checksum),
            });
        }

        // Decompress
        let data = match self.compression {
            Compression::Zstd => {
                zstd::decode_all(compressed.as_slice())
                    .map_err(|e| EngineError::SsTable(format!("decompression failed: {e}")))?
            }
            Compression::None => compressed,
        };

        // Decode entries
        let mut entries = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            match Entry::decode(&data[pos..]) {
                Ok((entry, consumed)) => {
                    entries.push(entry);
                    pos += consumed;
                }
                Err(_) => break,
            }
        }

        Ok(entries)
    }

    /// Find which block might contain the given key.
    fn find_block(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() {
            return None;
        }

        // Binary search: find the last block whose first_key <= key
        let mut lo = 0;
        let mut hi = self.index.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.index[mid].first_key.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if lo == 0 {
            // Key is before all blocks — check if first block contains it
            if key <= self.index[0].last_key.as_slice() {
                return Some(0);
            }
            return None;
        }

        let idx = lo - 1;
        // Verify key falls within block range
        if key >= self.index[idx].first_key.as_slice()
            && key <= self.index[idx].last_key.as_slice()
        {
            Some(idx)
        } else {
            None
        }
    }

    /// Iterate all entries in order (for compaction merge).
    pub fn iter(&self) -> Result<Vec<Entry>, EngineError> {
        let mut all_entries = Vec::new();
        for i in 0..self.index.len() {
            let block_entries = self.read_block(i)?;
            all_entries.extend(block_entries);
        }
        Ok(all_entries)
    }

    /// Number of entries in this SSTable.
    pub fn entry_count(&self) -> u64 {
        self.footer.entry_count
    }

    /// File path of this SSTable.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the index entries (for debugging / metrics).
    pub fn index_entries(&self) -> &[IndexEntry] {
        &self.index
    }

    /// Min key in this SSTable (from first block's first_key).
    pub fn min_key(&self) -> Option<&[u8]> {
        self.index.first().map(|e| e.first_key.as_slice())
    }

    /// Max key in this SSTable (from last block's last_key).
    pub fn max_key(&self) -> Option<&[u8]> {
        self.index.last().map(|e| e.last_key.as_slice())
    }
}

/// Decode the index block into a Vec of IndexEntry.
fn decode_index(data: &[u8]) -> Result<Vec<IndexEntry>, EngineError> {
    if data.len() < 4 {
        return Err(EngineError::SsTable("index block too small".into()));
    }

    let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(count);
    let mut pos = 4;

    for _ in 0..count {
        if pos >= data.len() {
            break;
        }
        let (entry, consumed) = IndexEntry::decode(&data[pos..])
            .map_err(|e| EngineError::SsTable(e.to_string()))?;
        entries.push(entry);
        pos += consumed;
    }

    Ok(entries)
}

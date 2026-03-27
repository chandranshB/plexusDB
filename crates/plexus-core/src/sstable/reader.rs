//! SSTable reader — reads entries from an SSTable file.
//!
//! Supports:
//! - Point lookups via bloom filter + binary search on index
//! - Range scans via sequential block iteration
//! - Integrity verification via block checksums

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

use crate::bloom::BloomFilter;
use crate::sstable::format::*;
use crate::{EngineError, Entry};

/// Reads entries from an SSTable file on disk.
pub struct SsTableReader {
    path: PathBuf,
    footer: Footer,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    compression: Compression,
    /// Cached file handle — avoids re-opening the file for every block read.
    file: Mutex<File>,
}

impl SsTableReader {
    /// Open an SSTable file for reading.
    pub fn open(path: &Path) -> Result<Self, EngineError> {
        let mut file = File::open(path)
            .map_err(|e| EngineError::SsTable(format!("cannot open {}: {e}", path.display())))?;

        let file_size = file
            .metadata()
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
        let footer =
            Footer::decode(&footer_buf).map_err(|e| EngineError::SsTable(e.to_string()))?;

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

        // Read compression type from footer
        let compression = footer.compression;

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
            file: Mutex::new(file),
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

        let mut file = self.file.lock();

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
            Compression::Zstd => zstd::decode_all(compressed.as_slice())
                .map_err(|e| EngineError::SsTable(format!("decompression failed: {e}")))?,
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
        if key >= self.index[idx].first_key.as_slice() && key <= self.index[idx].last_key.as_slice()
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
        let (entry, consumed) =
            IndexEntry::decode(&data[pos..]).map_err(|e| EngineError::SsTable(e.to_string()))?;
        entries.push(entry);
        pos += consumed;
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::format::Compression;
    use crate::sstable::writer::{SsTableWriter, SsTableWriterConfig};
    use crate::Entry;
    use tempfile::TempDir;

    fn write_sst(
        dir: &std::path::Path,
        name: &str,
        entries: Vec<Entry>,
        compression: Compression,
    ) -> std::path::PathBuf {
        let path = dir.join(name);
        let config = SsTableWriterConfig {
            compression,
            block_size: 256, // small blocks to exercise multi-block paths
            ..SsTableWriterConfig::default()
        };
        let mut writer = SsTableWriter::new(&path, config).unwrap();
        for e in &entries {
            writer.add(e).unwrap();
        }
        writer.finish().unwrap();
        path
    }

    fn make_entries(count: usize) -> Vec<Entry> {
        (0..count)
            .map(|i| {
                Entry::put(
                    format!("key_{i:06}").into_bytes(),
                    format!("value_{i:06}").into_bytes(),
                    i as u64 + 1,
                )
            })
            .collect()
    }

    #[test]
    fn test_open_and_read_all() {
        let tmp = TempDir::new().unwrap();
        let entries = make_entries(50);
        let path = write_sst(tmp.path(), "test.sst", entries.clone(), Compression::Zstd);

        let reader = SsTableReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 50);

        let read_entries = reader.iter().unwrap();
        assert_eq!(read_entries.len(), 50);

        for (orig, read) in entries.iter().zip(read_entries.iter()) {
            assert_eq!(orig.key, read.key);
            assert_eq!(orig.value, read.value);
            assert_eq!(orig.timestamp, read.timestamp);
        }
    }

    #[test]
    fn test_point_lookup_hit() {
        let tmp = TempDir::new().unwrap();
        let entries = make_entries(100);
        let path = write_sst(tmp.path(), "lookup.sst", entries, Compression::Zstd);

        let reader = SsTableReader::open(&path).unwrap();

        let result = reader.get(b"key_000042").unwrap();
        assert!(result.is_some());
        let entry = result.unwrap();
        assert_eq!(entry.key, b"key_000042");
        assert_eq!(entry.value, Some(b"value_000042".to_vec()));
        assert_eq!(entry.timestamp, 43);
    }

    #[test]
    fn test_point_lookup_miss() {
        let tmp = TempDir::new().unwrap();
        let entries = make_entries(50);
        let path = write_sst(tmp.path(), "miss.sst", entries, Compression::Zstd);

        let reader = SsTableReader::open(&path).unwrap();

        // Key that was never inserted
        let result = reader.get(b"zzz_not_here").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_bloom_filter_negative() {
        let tmp = TempDir::new().unwrap();
        let entries = make_entries(100);
        let path = write_sst(tmp.path(), "bloom.sst", entries, Compression::Zstd);

        let reader = SsTableReader::open(&path).unwrap();

        // A key that definitely doesn't exist should return false from bloom
        // (with very high probability — 1% FP rate means this should pass)
        let definitely_absent = b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        // may_contain can return true (false positive), but get() must return None
        let result = reader.get(definitely_absent).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_open_no_compression() {
        let tmp = TempDir::new().unwrap();
        let entries = make_entries(30);
        let path = write_sst(tmp.path(), "nocomp.sst", entries.clone(), Compression::None);

        let reader = SsTableReader::open(&path).unwrap();
        let read_entries = reader.iter().unwrap();
        assert_eq!(read_entries.len(), 30);
    }

    #[test]
    fn test_min_max_keys() {
        let tmp = TempDir::new().unwrap();
        let entries = make_entries(50);
        let path = write_sst(tmp.path(), "minmax.sst", entries, Compression::Zstd);

        let reader = SsTableReader::open(&path).unwrap();
        assert_eq!(reader.min_key(), Some(b"key_000000".as_slice()));
        assert_eq!(reader.max_key(), Some(b"key_000049".as_slice()));
    }

    #[test]
    fn test_tombstone_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let entries = vec![
            Entry::put(b"a".to_vec(), b"1".to_vec(), 1),
            Entry::delete(b"b".to_vec(), 2),
            Entry::put(b"c".to_vec(), b"3".to_vec(), 3),
        ];
        let path = write_sst(tmp.path(), "tomb.sst", entries, Compression::Zstd);

        let reader = SsTableReader::open(&path).unwrap();
        let all = reader.iter().unwrap();
        assert_eq!(all.len(), 3);

        let b_entry = reader.get(b"b").unwrap().unwrap();
        assert!(b_entry.is_tombstone());
    }

    #[test]
    fn test_open_nonexistent_file_errors() {
        let result = SsTableReader::open(std::path::Path::new("/nonexistent/path/file.sst"));
        assert!(result.is_err());
    }

    #[test]
    fn test_checksum_verification() {
        let tmp = TempDir::new().unwrap();
        // Use many entries and no compression so data blocks are large and easy to target
        let entries = make_entries(200);
        let path = write_sst(tmp.path(), "checksum.sst", entries, Compression::None);

        // Read the file to find where data blocks end (before bloom filter)
        let original = std::fs::read(&path).unwrap();
        let file_size = original.len();

        // Open the reader to get the meta_offset (start of bloom filter)
        let reader = SsTableReader::open(&path).unwrap();
        let meta_offset = reader
            .index_entries()
            .first()
            .map(|_| {
                // Corrupt a byte well within the first data block
                // The first block starts at offset 0
                file_size / 4
            })
            .unwrap_or(file_size / 4);

        let mut corrupted = original.clone();
        // Flip a byte in the middle of the first data block
        corrupted[meta_offset] ^= 0xFF;
        std::fs::write(&path, &corrupted).unwrap();

        // Re-open with corrupted data
        let reader2 = SsTableReader::open(&path).unwrap();
        // At least one block read should fail due to checksum mismatch
        let mut found_error = false;
        for i in 0..reader2.index_entries().len() {
            if reader2.read_block(i).is_err() {
                found_error = true;
                break;
            }
        }
        assert!(
            found_error,
            "corrupted block should fail checksum verification"
        );
    }
}

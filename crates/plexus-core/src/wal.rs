//! Write-Ahead Log (WAL) for crash recovery.
//!
//! Every write is appended to the WAL before being applied to the MemTable.
//! On crash, the WAL is replayed to reconstruct the MemTable state.
//!
//! ## On-Disk Format
//!
//! ```text
//! ┌────────────────────────────────────────────┐
//! │ Record 0                                   │
//! │ ┌──────────┬──────────┬──────────────────┐ │
//! │ │ len (4B) │ crc (4B) │ entry (variable) │ │
//! │ └──────────┴──────────┴──────────────────┘ │
//! │ Record 1                                   │
//! │ ┌──────────┬──────────┬──────────────────┐ │
//! │ │ len (4B) │ crc (4B) │ entry (variable) │ │
//! │ └──────────┴──────────┴──────────────────┘ │
//! │ ...                                        │
//! └────────────────────────────────────────────┘
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crc32fast::Hasher as CrcHasher;
use parking_lot::Mutex;

use crate::{Entry, EngineError};

/// WAL record header size: length (4) + CRC32 (4) = 8 bytes.
const RECORD_HEADER_SIZE: usize = 8;

/// Write-Ahead Log.
///
/// Thread-safe via internal `Mutex`. Writes are buffered and fsynced
/// on configurable intervals or on explicit `sync()`.
pub struct Wal {
    /// Inner state protected by mutex.
    inner: Mutex<WalInner>,
    /// Current WAL file size (atomic for lock-free reads).
    size: AtomicU64,
    /// Maximum WAL size before rotation.
    max_size: u64,
    /// WAL directory.
    dir: PathBuf,
    /// Current WAL sequence number.
    sequence: AtomicU64,
}

struct WalInner {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl Wal {
    /// Create or open a WAL in the given directory.
    pub fn open(dir: &Path, max_size: u64) -> Result<Self, EngineError> {
        fs::create_dir_all(dir).map_err(|e| EngineError::Wal(e.to_string()))?;

        // Find the latest WAL file or create a new one
        let (path, seq) = find_or_create_wal(dir)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| EngineError::Wal(e.to_string()))?;

        let file_size = file.metadata()
            .map_err(|e| EngineError::Wal(e.to_string()))?
            .len();

        tracing::info!(
            path = %path.display(),
            size = file_size,
            sequence = seq,
            "WAL opened"
        );

        Ok(Self {
            inner: Mutex::new(WalInner {
                writer: BufWriter::with_capacity(64 * 1024, file), // 64KB write buffer
                path,
            }),
            size: AtomicU64::new(file_size),
            max_size,
            dir: dir.to_path_buf(),
            sequence: AtomicU64::new(seq),
        })
    }

    /// Append an entry to the WAL.
    ///
    /// Returns the byte offset at which the record was written.
    pub fn append(&self, entry: &Entry) -> Result<u64, EngineError> {
        let data = entry.encode();
        let crc = compute_crc(&data);

        let record_size = RECORD_HEADER_SIZE + data.len();

        let mut inner = self.inner.lock();

        let offset = self.size.load(Ordering::Relaxed);

        // Write header: [length: u32][crc: u32]
        inner.writer
            .write_all(&(data.len() as u32).to_le_bytes())
            .map_err(|e| EngineError::Wal(e.to_string()))?;
        inner.writer
            .write_all(&crc.to_le_bytes())
            .map_err(|e| EngineError::Wal(e.to_string()))?;

        // Write entry data
        inner.writer
            .write_all(&data)
            .map_err(|e| EngineError::Wal(e.to_string()))?;

        self.size.fetch_add(record_size as u64, Ordering::Relaxed);

        Ok(offset)
    }

    /// Flush the write buffer and sync to disk.
    pub fn sync(&self) -> Result<(), EngineError> {
        let mut inner = self.inner.lock();
        inner.writer.flush().map_err(|e| EngineError::Wal(e.to_string()))?;
        inner.writer.get_ref().sync_data().map_err(|e| EngineError::Wal(e.to_string()))?;
        Ok(())
    }

    /// Current WAL file size.
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    /// Check if the WAL needs rotation.
    pub fn needs_rotation(&self) -> bool {
        self.size() >= self.max_size
    }

    /// Rotate to a new WAL file.
    ///
    /// Returns the path of the old (sealed) WAL file.
    pub fn rotate(&self) -> Result<PathBuf, EngineError> {
        let new_seq = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let new_path = self.dir.join(format!("wal_{:06}.log", new_seq));

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| EngineError::Wal(e.to_string()))?;

        let mut inner = self.inner.lock();

        // Flush and sync the old WAL
        inner.writer.flush().map_err(|e| EngineError::Wal(e.to_string()))?;
        inner.writer.get_ref().sync_data().map_err(|e| EngineError::Wal(e.to_string()))?;

        let old_path = inner.path.clone();

        // Switch to new WAL
        inner.writer = BufWriter::with_capacity(64 * 1024, new_file);
        inner.path = new_path;

        self.size.store(0, Ordering::Relaxed);

        tracing::info!(
            old = %old_path.display(),
            new_seq = new_seq,
            "WAL rotated"
        );

        Ok(old_path)
    }

    /// Replay a WAL file and return all entries.
    ///
    /// Entries with CRC mismatches are skipped (logged as warnings).
    pub fn replay(path: &Path) -> Result<Vec<Entry>, EngineError> {
        let mut file = File::open(path)
            .map_err(|e| EngineError::Wal(format!("cannot open WAL {}: {e}", path.display())))?;

        let file_size = file.metadata()
            .map_err(|e| EngineError::Wal(e.to_string()))?
            .len();

        let mut entries = Vec::new();
        let mut pos = 0u64;

        tracing::info!(path = %path.display(), size = file_size, "replaying WAL");

        while pos + RECORD_HEADER_SIZE as u64 <= file_size {
            // Read header
            let mut header = [0u8; RECORD_HEADER_SIZE];
            file.seek(SeekFrom::Start(pos))
                .map_err(|e| EngineError::Wal(e.to_string()))?;

            if file.read_exact(&mut header).is_err() {
                break; // Partial header at end of file
            }

            let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

            if len == 0 || pos + RECORD_HEADER_SIZE as u64 + len as u64 > file_size {
                tracing::warn!(pos, len, "truncated WAL record, stopping replay");
                break;
            }

            // Read entry data
            let mut data = vec![0u8; len];
            if file.read_exact(&mut data).is_err() {
                break;
            }

            // Verify CRC
            let actual_crc = compute_crc(&data);
            if actual_crc != expected_crc {
                tracing::warn!(
                    pos,
                    expected = expected_crc,
                    actual = actual_crc,
                    "CRC mismatch, skipping record"
                );
                pos += RECORD_HEADER_SIZE as u64 + len as u64;
                continue;
            }

            // Decode entry
            match Entry::decode(&data) {
                Ok((entry, _)) => {
                    entries.push(entry);
                }
                Err(e) => {
                    tracing::warn!(pos, error = %e, "failed to decode WAL entry, skipping");
                }
            }

            pos += RECORD_HEADER_SIZE as u64 + len as u64;
        }

        tracing::info!(
            path = %path.display(),
            entries = entries.len(),
            "WAL replay complete"
        );

        Ok(entries)
    }

    /// Delete a sealed WAL file after its entries have been flushed to SSTable.
    pub fn delete_sealed(path: &Path) -> Result<(), EngineError> {
        fs::remove_file(path).map_err(|e| EngineError::Wal(e.to_string()))?;
        tracing::debug!(path = %path.display(), "deleted sealed WAL");
        Ok(())
    }

    /// Get the current WAL file path.
    pub fn current_path(&self) -> PathBuf {
        self.inner.lock().path.clone()
    }
}

/// Compute CRC32c checksum.
fn compute_crc(data: &[u8]) -> u32 {
    let mut hasher = CrcHasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Find the latest WAL file in the directory, or create a new one.
fn find_or_create_wal(dir: &Path) -> Result<(PathBuf, u64), EngineError> {
    let mut max_seq = 0u64;

    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(seq_str) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    max_seq = max_seq.max(seq);
                }
            }
        }
    }

    // Use the latest existing WAL or create sequence 0
    let path = dir.join(format!("wal_{:06}.log", max_seq));
    Ok((path, max_seq))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_append_and_replay() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        let wal = Wal::open(&wal_dir, 1024 * 1024).unwrap();

        // Write some entries
        let e1 = Entry::put(b"key1".to_vec(), b"value1".to_vec(), 1);
        let e2 = Entry::put(b"key2".to_vec(), b"value2".to_vec(), 2);
        let e3 = Entry::delete(b"key1".to_vec(), 3);

        wal.append(&e1).unwrap();
        wal.append(&e2).unwrap();
        wal.append(&e3).unwrap();
        wal.sync().unwrap();

        // Replay
        let path = wal.current_path();
        let entries = Wal::replay(&path).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"key1");
        assert_eq!(entries[1].key, b"key2");
        assert!(entries[2].is_tombstone());
    }

    #[test]
    fn test_wal_rotation() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        let wal = Wal::open(&wal_dir, 100).unwrap(); // Very small max size

        // Write enough to trigger rotation check
        for i in 0..20 {
            let entry = Entry::put(
                format!("key_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
                i as u64,
            );
            wal.append(&entry).unwrap();
        }
        wal.sync().unwrap();

        assert!(wal.needs_rotation());

        let old_path = wal.rotate().unwrap();
        assert!(old_path.exists());
        assert_eq!(wal.size(), 0);
    }
}

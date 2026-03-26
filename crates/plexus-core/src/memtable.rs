//! In-memory sorted table (MemTable) using a concurrent skip list.
//!
//! The MemTable is the first destination for all writes. It provides:
//! - O(log n) insert and lookup
//! - Lock-free concurrent reads (via `crossbeam-skiplist`)
//! - Ordered iteration for flushing to SSTable
//!
//! When the MemTable reaches its size threshold, it is "frozen" (made
//! read-only) and a new active MemTable is created. A background thread
//! then flushes the frozen MemTable to an SSTable on disk.

use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use crossbeam_skiplist::SkipMap;

use crate::Entry;

/// A composite key for the skip list that sorts by (key, reverse timestamp).
///
/// Sorting by reverse timestamp ensures that the latest version of a key
/// appears first during iteration, enabling efficient point lookups.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MemKey {
    key: Vec<u8>,
    /// Stored as `u64::MAX - timestamp` so that newer entries sort first.
    reverse_ts: u64,
}

impl MemKey {
    fn new(key: Vec<u8>, timestamp: u64) -> Self {
        Self {
            key,
            reverse_ts: u64::MAX - timestamp,
        }
    }
}

/// Value stored in the skip list.
#[derive(Debug, Clone)]
struct MemValue {
    value: Option<Vec<u8>>,
    namespace: String,
}

/// In-memory sorted table backed by a lock-free skip list.
#[allow(dead_code)]
pub struct MemTable {
    /// The underlying skip list.
    map: SkipMap<MemKey, MemValue>,
    /// Approximate size in bytes (for flush threshold).
    size: AtomicU64,
    /// Number of entries.
    count: AtomicU64,
    /// Whether this MemTable is frozen (read-only).
    frozen: AtomicBool,
    /// Creation timestamp.
    created_at: u64,
}

impl MemTable {
    /// Create a new empty MemTable.
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicU64::new(0),
            count: AtomicU64::new(0),
            frozen: AtomicBool::new(false),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
        }
    }

    /// Insert an entry into the MemTable.
    ///
    /// Returns an error if the MemTable is frozen.
    pub fn put(&self, entry: Entry) -> Result<(), &'static str> {
        if self.frozen.load(Ordering::Relaxed) {
            return Err("MemTable is frozen");
        }

        let entry_size = entry.encoded_size() as u64;

        let mem_key = MemKey::new(entry.key, entry.timestamp);
        let mem_val = MemValue {
            value: entry.value,
            namespace: entry.namespace,
        };

        self.map.insert(mem_key, mem_val);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Look up the latest value for a key.
    ///
    /// Returns `None` if the key doesn't exist. Returns `Some(None)` if
    /// the key was deleted (tombstone).
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        // Create a search key with the highest possible timestamp
        let search = MemKey::new(key.to_vec(), u64::MAX);

        // Find the first entry >= search key (which will be our key with
        // the highest timestamp due to reverse-ts ordering)
        let iter = self.map.range(search..);

        for entry in iter {
            let mem_key = entry.key();
            if mem_key.key == key {
                let mem_val = entry.value();
                return Some(Entry {
                    key: mem_key.key.clone(),
                    value: mem_val.value.clone(),
                    timestamp: u64::MAX - mem_key.reverse_ts,
                    namespace: mem_val.namespace.clone(),
                });
            }
            // Past our key range
            if mem_key.key.as_slice() > key {
                break;
            }
        }

        None
    }

    /// Freeze this MemTable (make it read-only).
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::Release);
        tracing::debug!(
            size = self.size(),
            count = self.count(),
            "MemTable frozen"
        );
    }

    /// Is this MemTable frozen?
    #[inline]
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Relaxed)
    }

    /// Approximate size in bytes.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    /// Number of entries.
    #[inline]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Check if the MemTable should be flushed (exceeds threshold).
    #[inline]
    pub fn should_flush(&self, threshold: usize) -> bool {
        self.size() >= threshold as u64
    }

    /// Iterate all entries in sorted order for flushing to SSTable.
    ///
    /// Entries are yielded in (key, timestamp_desc) order.
    pub fn iter(&self) -> impl Iterator<Item = Entry> + '_ {
        self.map.iter().map(|entry| {
            let mem_key = entry.key();
            let mem_val = entry.value();
            Entry {
                key: mem_key.key.clone(),
                value: mem_val.value.clone(),
                timestamp: u64::MAX - mem_key.reverse_ts,
                namespace: mem_val.namespace.clone(),
            }
        })
    }

    /// Drain all entries as a sorted Vec (for SSTable flush).
    pub fn drain_sorted(&self) -> Vec<Entry> {
        self.iter().collect()
    }

    /// Check if the MemTable is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mt = MemTable::new();

        mt.put(Entry::put(b"key1".to_vec(), b"value1".to_vec(), 1)).unwrap();
        mt.put(Entry::put(b"key2".to_vec(), b"value2".to_vec(), 2)).unwrap();

        let result = mt.get(b"key1").unwrap();
        assert_eq!(result.value, Some(b"value1".to_vec()));

        let result = mt.get(b"key2").unwrap();
        assert_eq!(result.value, Some(b"value2".to_vec()));

        assert!(mt.get(b"key3").is_none());
    }

    #[test]
    fn test_latest_version_wins() {
        let mt = MemTable::new();

        mt.put(Entry::put(b"key1".to_vec(), b"old".to_vec(), 1)).unwrap();
        mt.put(Entry::put(b"key1".to_vec(), b"new".to_vec(), 2)).unwrap();

        let result = mt.get(b"key1").unwrap();
        assert_eq!(result.value, Some(b"new".to_vec()));
        assert_eq!(result.timestamp, 2);
    }

    #[test]
    fn test_tombstone() {
        let mt = MemTable::new();

        mt.put(Entry::put(b"key1".to_vec(), b"value1".to_vec(), 1)).unwrap();
        mt.put(Entry::delete(b"key1".to_vec(), 2)).unwrap();

        let result = mt.get(b"key1").unwrap();
        assert!(result.is_tombstone());
        assert_eq!(result.timestamp, 2);
    }

    #[test]
    fn test_freeze_prevents_writes() {
        let mt = MemTable::new();
        mt.put(Entry::put(b"key1".to_vec(), b"value1".to_vec(), 1)).unwrap();
        mt.freeze();

        let result = mt.put(Entry::put(b"key2".to_vec(), b"value2".to_vec(), 2));
        assert!(result.is_err());
    }

    #[test]
    fn test_sorted_iteration() {
        let mt = MemTable::new();

        mt.put(Entry::put(b"c".to_vec(), b"3".to_vec(), 1)).unwrap();
        mt.put(Entry::put(b"a".to_vec(), b"1".to_vec(), 1)).unwrap();
        mt.put(Entry::put(b"b".to_vec(), b"2".to_vec(), 1)).unwrap();

        let entries = mt.drain_sorted();
        assert_eq!(entries[0].key, b"a");
        assert_eq!(entries[1].key, b"b");
        assert_eq!(entries[2].key, b"c");
    }

    #[test]
    fn test_size_tracking() {
        let mt = MemTable::new();
        assert_eq!(mt.size(), 0);
        assert_eq!(mt.count(), 0);

        mt.put(Entry::put(b"key".to_vec(), b"value".to_vec(), 1)).unwrap();
        assert!(mt.size() > 0);
        assert_eq!(mt.count(), 1);
    }
}

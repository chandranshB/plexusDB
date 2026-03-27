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

use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::Entry;

/// A composite key for the skip list that sorts by (namespace, key, reverse timestamp).
///
/// Including namespace in the key ensures complete isolation between namespaces —
/// `(ns_a, "key")` and `(ns_b, "key")` are entirely separate entries.
/// Sorting by reverse timestamp within a (namespace, key) pair ensures the
/// latest version appears first during iteration.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MemKey {
    namespace: String,
    key: Vec<u8>,
    /// Stored as `u64::MAX - timestamp` so that newer entries sort first.
    reverse_ts: u64,
}

impl MemKey {
    fn new(namespace: String, key: Vec<u8>, timestamp: u64) -> Self {
        Self {
            namespace,
            key,
            // wrapping_sub prevents panic in debug on u64::MAX timestamps
            reverse_ts: u64::MAX.wrapping_sub(timestamp),
        }
    }
}

/// Value stored in the skip list.
#[derive(Debug, Clone)]
struct MemValue {
    value: Option<Vec<u8>>,
}

/// In-memory sorted table backed by a lock-free skip list.
pub struct MemTable {
    /// The underlying skip list.
    map: SkipMap<MemKey, MemValue>,
    /// Approximate size in bytes (for flush threshold).
    size: AtomicU64,
    /// Number of entries.
    count: AtomicU64,
    /// Whether this MemTable is frozen (read-only).
    frozen: AtomicBool,
}

impl MemTable {
    /// Create a new empty MemTable.
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            size: AtomicU64::new(0),
            count: AtomicU64::new(0),
            frozen: AtomicBool::new(false),
        }
    }

    /// Insert an entry into the MemTable.
    ///
    /// Returns an error if the MemTable is frozen.
    pub fn put(&self, entry: Entry) -> Result<(), &'static str> {
        if self.frozen.load(Ordering::Acquire) {
            return Err("MemTable is frozen");
        }

        let entry_size = entry.encoded_size() as u64;

        let mem_key = MemKey::new(entry.namespace, entry.key, entry.timestamp);
        let mem_val = MemValue { value: entry.value };

        self.map.insert(mem_key, mem_val);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Look up the latest value for a key within a namespace.
    ///
    /// Returns `None` if the key doesn't exist in that namespace.
    /// Returns `Some(Entry { value: None, .. })` if the key was deleted (tombstone).
    pub fn get(&self, key: &[u8], namespace: &str) -> Option<Entry> {
        // Search key: (namespace, key, reverse_ts=0) is the smallest key for this
        // (namespace, key) pair. The first entry at or after it is the latest version
        // (highest timestamp = smallest reverse_ts).
        let search = MemKey::new(namespace.to_string(), key.to_vec(), u64::MAX);

        if let Some(entry) = self.map.range(search..).next() {
            let mem_key = entry.key();
            // Confirm we're still on the right (namespace, key) pair
            if mem_key.namespace == namespace && mem_key.key == key {
                let mem_val = entry.value();
                return Some(Entry {
                    key: mem_key.key.clone(),
                    value: mem_val.value.clone(),
                    timestamp: u64::MAX.wrapping_sub(mem_key.reverse_ts),
                    namespace: mem_key.namespace.clone(),
                });
            }
        }

        None
    }

    /// Freeze this MemTable (make it read-only).
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::Release);
        tracing::debug!(size = self.size(), count = self.count(), "MemTable frozen");
    }

    /// Is this MemTable frozen?
    #[inline]
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
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
    /// Entries are yielded in (namespace, key, timestamp_desc) order.
    pub fn iter(&self) -> impl Iterator<Item = Entry> + '_ {
        self.map.iter().map(|entry| {
            let mem_key = entry.key();
            let mem_val = entry.value();
            Entry {
                key: mem_key.key.clone(),
                value: mem_val.value.clone(),
                timestamp: u64::MAX.wrapping_sub(mem_key.reverse_ts),
                namespace: mem_key.namespace.clone(),
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

        mt.put(Entry::put(b"key1".to_vec(), b"value1".to_vec(), 1))
            .unwrap();
        mt.put(Entry::put(b"key2".to_vec(), b"value2".to_vec(), 2))
            .unwrap();

        let result = mt.get(b"key1", "default").unwrap();
        assert_eq!(result.value, Some(b"value1".to_vec()));

        let result = mt.get(b"key2", "default").unwrap();
        assert_eq!(result.value, Some(b"value2".to_vec()));

        assert!(mt.get(b"key3", "default").is_none());
    }

    #[test]
    fn test_latest_version_wins() {
        let mt = MemTable::new();

        mt.put(Entry::put(b"key1".to_vec(), b"old".to_vec(), 1))
            .unwrap();
        mt.put(Entry::put(b"key1".to_vec(), b"new".to_vec(), 2))
            .unwrap();

        let result = mt.get(b"key1", "default").unwrap();
        assert_eq!(result.value, Some(b"new".to_vec()));
        assert_eq!(result.timestamp, 2);
    }

    #[test]
    fn test_tombstone() {
        let mt = MemTable::new();

        mt.put(Entry::put(b"key1".to_vec(), b"value1".to_vec(), 1))
            .unwrap();
        mt.put(Entry::delete(b"key1".to_vec(), 2)).unwrap();

        let result = mt.get(b"key1", "default").unwrap();
        assert!(result.is_tombstone());
        assert_eq!(result.timestamp, 2);
    }

    #[test]
    fn test_namespace_isolation_in_memtable() {
        let mt = MemTable::new();

        let mut e_a = Entry::put(b"key".to_vec(), b"ns_a".to_vec(), 1);
        e_a.namespace = "ns_a".to_string();
        let mut e_b = Entry::put(b"key".to_vec(), b"ns_b".to_vec(), 2);
        e_b.namespace = "ns_b".to_string();

        mt.put(e_a).unwrap();
        mt.put(e_b).unwrap();

        assert_eq!(
            mt.get(b"key", "ns_a").unwrap().value,
            Some(b"ns_a".to_vec())
        );
        assert_eq!(
            mt.get(b"key", "ns_b").unwrap().value,
            Some(b"ns_b".to_vec())
        );
        assert!(mt.get(b"key", "default").is_none());
    }

    #[test]
    fn test_freeze_prevents_writes() {
        let mt = MemTable::new();
        mt.put(Entry::put(b"key1".to_vec(), b"value1".to_vec(), 1))
            .unwrap();
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
        // All in default namespace — sorted by (namespace, key)
        assert_eq!(entries[0].key, b"a");
        assert_eq!(entries[1].key, b"b");
        assert_eq!(entries[2].key, b"c");
    }

    #[test]
    fn test_size_tracking() {
        let mt = MemTable::new();
        assert_eq!(mt.size(), 0);
        assert_eq!(mt.count(), 0);

        mt.put(Entry::put(b"key".to_vec(), b"value".to_vec(), 1))
            .unwrap();
        assert!(mt.size() > 0);
        assert_eq!(mt.count(), 1);
    }
}

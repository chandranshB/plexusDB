//! LRU block cache for frequently accessed SSTable data blocks.
//!
//! The cache keeps hot data blocks in RAM to avoid redundant disk reads.
//! This is especially critical when PlexusDB manages its own memory via
//! `O_DIRECT` (bypassing the kernel page cache).

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cache key: (SSTable file name, block index).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub file_name: String,
    pub block_index: usize,
}

/// A cached data block.
#[derive(Debug, Clone)]
struct CacheEntry {
    data: Vec<u8>,
    size: usize,
    /// For LRU eviction — higher = more recently used.
    last_access: u64,
}

/// Thread-safe LRU block cache.
pub struct BlockCache {
    inner: Mutex<CacheInner>,
    /// Maximum cache size in bytes.
    max_size: usize,
    /// Monotonically increasing access counter.
    access_counter: AtomicU64,
    // Metrics
    hits: AtomicU64,
    misses: AtomicU64,
}

struct CacheInner {
    entries: HashMap<CacheKey, CacheEntry>,
    current_size: usize,
}

impl BlockCache {
    /// Create a new block cache with the given maximum size in bytes.
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                entries: HashMap::new(),
                current_size: 0,
            }),
            max_size,
            access_counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Look up a block in the cache.
    pub fn get(&self, key: &CacheKey) -> Option<Vec<u8>> {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.entries.get_mut(key) {
            entry.last_access = self.access_counter.fetch_add(1, Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.data.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a block into the cache. Evicts LRU entries if necessary.
    pub fn insert(&self, key: CacheKey, data: Vec<u8>) {
        let size = data.len();
        let access = self.access_counter.fetch_add(1, Ordering::Relaxed);

        let mut inner = self.inner.lock();

        // Remove existing entry if present
        if let Some(old) = inner.entries.remove(&key) {
            inner.current_size -= old.size;
        }

        // Evict LRU entries until we have room
        while inner.current_size + size > self.max_size && !inner.entries.is_empty() {
            // Find the LRU entry
            let lru_key = inner
                .entries
                .iter()
                .min_by_key(|(_, v)| v.last_access)
                .map(|(k, _)| k.clone());

            if let Some(lru_key) = lru_key {
                if let Some(evicted) = inner.entries.remove(&lru_key) {
                    inner.current_size -= evicted.size;
                }
            } else {
                break;
            }
        }

        // Insert new entry (if it fits)
        if size <= self.max_size {
            inner.entries.insert(
                key,
                CacheEntry {
                    data,
                    size,
                    last_access: access,
                },
            );
            inner.current_size += size;
        }
    }

    /// Invalidate a specific block.
    pub fn invalidate(&self, key: &CacheKey) {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.entries.remove(key) {
            inner.current_size -= entry.size;
        }
    }

    /// Invalidate all blocks for a given SSTable file.
    pub fn invalidate_file(&self, file_name: &str) {
        let mut inner = self.inner.lock();
        let keys: Vec<CacheKey> = inner
            .entries
            .keys()
            .filter(|k| k.file_name == file_name)
            .cloned()
            .collect();

        for key in keys {
            if let Some(entry) = inner.entries.remove(&key) {
                inner.current_size -= entry.size;
            }
        }
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.entries.clear();
        inner.current_size = 0;
    }

    /// Cache hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Current cache size in bytes.
    pub fn current_size(&self) -> usize {
        self.inner.lock().current_size
    }

    /// Number of cached blocks.
    pub fn entry_count(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Cache statistics.
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock();
        CacheStats {
            max_size: self.max_size,
            current_size: inner.current_size,
            entry_count: inner.entries.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            hit_rate: self.hit_rate(),
        }
    }
}

/// Cache statistics for monitoring.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub max_size: usize,
    pub current_size: usize,
    pub entry_count: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let cache = BlockCache::new(1024 * 1024);

        let key = CacheKey {
            file_name: "test.sst".to_string(),
            block_index: 0,
        };

        cache.insert(key.clone(), vec![1, 2, 3, 4]);
        let result = cache.get(&key);
        assert_eq!(result, Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_lru_eviction() {
        let cache = BlockCache::new(100); // tiny cache

        // Insert entries that exceed max size
        for i in 0..10 {
            let key = CacheKey {
                file_name: "test.sst".to_string(),
                block_index: i,
            };
            cache.insert(key, vec![0u8; 20]);
        }

        // Cache should have evicted oldest entries
        assert!(cache.current_size() <= 100);
    }

    #[test]
    fn test_invalidate_file() {
        let cache = BlockCache::new(1024 * 1024);

        for i in 0..5 {
            cache.insert(
                CacheKey {
                    file_name: "a.sst".to_string(),
                    block_index: i,
                },
                vec![0u8; 10],
            );
            cache.insert(
                CacheKey {
                    file_name: "b.sst".to_string(),
                    block_index: i,
                },
                vec![0u8; 10],
            );
        }

        assert_eq!(cache.entry_count(), 10);
        cache.invalidate_file("a.sst");
        assert_eq!(cache.entry_count(), 5);
    }
}

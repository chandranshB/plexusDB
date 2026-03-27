//! Merge iterator for combining entries from multiple sorted sources.
//!
//! Used during:
//! - **Reads**: Merge MemTable + multiple SSTable levels
//! - **Compaction**: Merge overlapping SSTables into a single sorted stream
//!
//! The iterator uses a binary heap (min-heap) to efficiently merge K sorted
//! streams in O(N log K) time.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::Entry;

/// A source of sorted entries.
pub trait EntrySource {
    /// Return the next entry, or None if exhausted.
    fn next_entry(&mut self) -> Option<Entry>;
}

/// Wrapper for the heap that implements reverse ordering (min-heap).
struct HeapEntry {
    entry: Entry,
    source_index: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key && self.entry.timestamp == other.entry.timestamp
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior (BinaryHeap is a max-heap)
        match other.entry.key.cmp(&self.entry.key) {
            Ordering::Equal => {
                // For same key, prefer higher timestamp (newer) and lower source_index
                match self.entry.timestamp.cmp(&other.entry.timestamp) {
                    Ordering::Equal => other.source_index.cmp(&self.source_index),
                    ord => ord,
                }
            }
            ord => ord,
        }
    }
}

/// Merges multiple sorted entry sources into a single sorted stream.
///
/// When multiple sources contain the same key, the entry with the highest
/// timestamp is returned (latest write wins). Duplicate older versions are
/// silently discarded.
pub struct MergeIterator {
    sources: Vec<Box<dyn EntrySource>>,
    heap: BinaryHeap<HeapEntry>,
    initialized: bool,
}

impl MergeIterator {
    /// Create a new merge iterator from multiple sources.
    pub fn new(sources: Vec<Box<dyn EntrySource>>) -> Self {
        Self {
            sources,
            heap: BinaryHeap::new(),
            initialized: false,
        }
    }

    /// Initialize the heap with the first entry from each source.
    fn init(&mut self) {
        for (i, source) in self.sources.iter_mut().enumerate() {
            if let Some(entry) = source.next_entry() {
                self.heap.push(HeapEntry {
                    entry,
                    source_index: i,
                });
            }
        }
        self.initialized = true;
    }

    /// Collect all remaining entries into a Vec.
    pub fn collect_all(&mut self) -> Vec<Entry> {
        let mut result = Vec::new();
        for entry in self.by_ref() {
            result.push(entry);
        }
        result
    }
}

impl Iterator for MergeIterator {
    type Item = Entry;

    /// Get the next entry in sorted order.
    ///
    /// Automatically deduplicates: for the same key, only the newest version
    /// is returned.
    fn next(&mut self) -> Option<Entry> {
        if !self.initialized {
            self.init();
        }

        let heap_entry = self.heap.pop()?;
        let current = heap_entry.entry;
        let source_idx = heap_entry.source_index;

        // Advance the source that provided this entry
        if let Some(next) = self.sources[source_idx].next_entry() {
            self.heap.push(HeapEntry {
                entry: next,
                source_index: source_idx,
            });
        }

        // Skip older versions of the same key
        while let Some(top) = self.heap.peek() {
            if top.entry.key == current.key {
                let old = self.heap.pop().unwrap();
                // Advance that source too
                if let Some(next) = self.sources[old.source_index].next_entry() {
                    self.heap.push(HeapEntry {
                        entry: next,
                        source_index: old.source_index,
                    });
                }
            } else {
                break;
            }
        }

        Some(current)
    }
}

/// An entry source backed by a Vec (for testing and compaction).
pub struct VecSource {
    entries: Vec<Entry>,
    position: usize,
}

impl VecSource {
    pub fn new(entries: Vec<Entry>) -> Self {
        Self {
            entries,
            position: 0,
        }
    }
}

impl EntrySource for VecSource {
    fn next_entry(&mut self) -> Option<Entry> {
        if self.position < self.entries.len() {
            let entry = self.entries[self.position].clone();
            self.position += 1;
            Some(entry)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_two_sources() {
        let src1 = VecSource::new(vec![
            Entry::put(b"a".to_vec(), b"1".to_vec(), 1),
            Entry::put(b"c".to_vec(), b"3".to_vec(), 1),
            Entry::put(b"e".to_vec(), b"5".to_vec(), 1),
        ]);

        let src2 = VecSource::new(vec![
            Entry::put(b"b".to_vec(), b"2".to_vec(), 1),
            Entry::put(b"d".to_vec(), b"4".to_vec(), 1),
            Entry::put(b"f".to_vec(), b"6".to_vec(), 1),
        ]);

        let mut merger = MergeIterator::new(vec![Box::new(src1), Box::new(src2)]);
        let result = merger.collect_all();

        let keys: Vec<&[u8]> = result.iter().map(|e| e.key.as_slice()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c", b"d", b"e", b"f"]);
    }

    #[test]
    fn test_deduplication_newest_wins() {
        let src1 = VecSource::new(vec![
            Entry::put(b"key".to_vec(), b"old".to_vec(), 1),
        ]);

        let src2 = VecSource::new(vec![
            Entry::put(b"key".to_vec(), b"new".to_vec(), 2),
        ]);

        let mut merger = MergeIterator::new(vec![Box::new(src1), Box::new(src2)]);
        let result = merger.collect_all();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, Some(b"new".to_vec()));
        assert_eq!(result[0].timestamp, 2);
    }

    #[test]
    fn test_tombstone_preserved() {
        let src1 = VecSource::new(vec![
            Entry::put(b"key".to_vec(), b"value".to_vec(), 1),
        ]);

        let src2 = VecSource::new(vec![
            Entry::delete(b"key".to_vec(), 2),
        ]);

        let mut merger = MergeIterator::new(vec![Box::new(src1), Box::new(src2)]);
        let result = merger.collect_all();

        assert_eq!(result.len(), 1);
        assert!(result[0].is_tombstone());
    }

    #[test]
    fn test_empty_sources() {
        let src1 = VecSource::new(vec![]);
        let src2 = VecSource::new(vec![]);

        let mut merger = MergeIterator::new(vec![Box::new(src1), Box::new(src2)]);
        let result = merger.collect_all();

        assert!(result.is_empty());
    }
}

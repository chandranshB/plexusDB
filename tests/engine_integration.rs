//! Integration tests for the PlexusDB storage engine.
//!
//! These tests exercise the full stack: WAL → MemTable → SSTable → compaction,
//! including crash recovery, namespace isolation, and concurrent access.

use plexus_core::{Engine, EngineConfig};
use tempfile::TempDir;

fn open_engine(dir: &std::path::Path) -> std::sync::Arc<Engine> {
    Engine::open(EngineConfig {
        data_dir: dir.to_path_buf(),
        memtable_size: 512 * 1024, // 512 KB — small so tests trigger flushes
        block_cache_size: 4 * 1024 * 1024,
        ..EngineConfig::default()
    })
    .expect("engine open failed")
}

// ── Basic put / get / delete ──────────────────────────────────────────────────

#[test]
fn test_put_get_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine
        .put(b"hello".to_vec(), b"world".to_vec(), "default")
        .unwrap();
    let val = engine.get(b"hello", "default").unwrap();
    assert_eq!(val, Some(b"world".to_vec()));
}

#[test]
fn test_get_missing_key_returns_none() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());
    assert_eq!(engine.get(b"nope", "default").unwrap(), None);
}

#[test]
fn test_overwrite_returns_latest_value() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine
        .put(b"k".to_vec(), b"v1".to_vec(), "default")
        .unwrap();
    engine
        .put(b"k".to_vec(), b"v2".to_vec(), "default")
        .unwrap();
    engine
        .put(b"k".to_vec(), b"v3".to_vec(), "default")
        .unwrap();

    assert_eq!(engine.get(b"k", "default").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn test_delete_makes_key_invisible() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine
        .put(b"key".to_vec(), b"val".to_vec(), "default")
        .unwrap();
    assert!(engine.get(b"key", "default").unwrap().is_some());

    engine.delete(b"key".to_vec(), "default").unwrap();
    assert_eq!(engine.get(b"key", "default").unwrap(), None);
}

#[test]
fn test_delete_nonexistent_key_is_ok() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());
    // Should not error — tombstone is written regardless
    engine.delete(b"ghost".to_vec(), "default").unwrap();
    assert_eq!(engine.get(b"ghost", "default").unwrap(), None);
}

// ── Namespace isolation ───────────────────────────────────────────────────────

#[test]
fn test_namespace_isolation() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine
        .put(b"key".to_vec(), b"ns_a_value".to_vec(), "ns_a")
        .unwrap();
    engine
        .put(b"key".to_vec(), b"ns_b_value".to_vec(), "ns_b")
        .unwrap();

    assert_eq!(
        engine.get(b"key", "ns_a").unwrap(),
        Some(b"ns_a_value".to_vec())
    );
    assert_eq!(
        engine.get(b"key", "ns_b").unwrap(),
        Some(b"ns_b_value".to_vec())
    );
    // "default" namespace should not see either
    assert_eq!(engine.get(b"key", "default").unwrap(), None);
}

#[test]
fn test_delete_in_one_namespace_does_not_affect_another() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine.put(b"k".to_vec(), b"a".to_vec(), "ns_a").unwrap();
    engine.put(b"k".to_vec(), b"b".to_vec(), "ns_b").unwrap();

    engine.delete(b"k".to_vec(), "ns_a").unwrap();

    assert_eq!(engine.get(b"k", "ns_a").unwrap(), None);
    assert_eq!(engine.get(b"k", "ns_b").unwrap(), Some(b"b".to_vec()));
}

// ── Scan ─────────────────────────────────────────────────────────────────────

#[test]
fn test_scan_bounded_range() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    for i in 0u8..10 {
        engine.put(vec![i], vec![i * 10], "default").unwrap();
    }

    // Scan [3, 7)
    let results = engine.scan(&[3], &[7], 0, "default").unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].0, vec![3]);
    assert_eq!(results[3].0, vec![6]);
}

#[test]
fn test_scan_unbounded_returns_all() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    for i in 0u8..5 {
        engine.put(vec![i], vec![i], "default").unwrap();
    }

    let results = engine.scan(&[], &[], 0, "default").unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_scan_limit() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    for i in 0u8..20 {
        engine.put(vec![i], vec![i], "default").unwrap();
    }

    let results = engine.scan(&[], &[], 5, "default").unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_scan_excludes_tombstones() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine.put(b"a".to_vec(), b"1".to_vec(), "default").unwrap();
    engine.put(b"b".to_vec(), b"2".to_vec(), "default").unwrap();
    engine.put(b"c".to_vec(), b"3".to_vec(), "default").unwrap();
    engine.delete(b"b".to_vec(), "default").unwrap();

    let results = engine.scan(&[], &[], 0, "default").unwrap();
    let keys: Vec<_> = results.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a", b"c"]);
}

#[test]
fn test_scan_namespace_isolation() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine.put(b"k1".to_vec(), b"a".to_vec(), "ns_a").unwrap();
    engine.put(b"k2".to_vec(), b"b".to_vec(), "ns_b").unwrap();
    engine.put(b"k3".to_vec(), b"c".to_vec(), "ns_a").unwrap();

    let ns_a = engine.scan(&[], &[], 0, "ns_a").unwrap();
    assert_eq!(ns_a.len(), 2);
    assert!(ns_a.iter().all(|(_, v)| v == b"a" || v == b"c"));

    let ns_b = engine.scan(&[], &[], 0, "ns_b").unwrap();
    assert_eq!(ns_b.len(), 1);
}

// ── Input validation ──────────────────────────────────────────────────────────

#[test]
fn test_empty_key_is_rejected() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());
    assert!(engine.put(vec![], b"v".to_vec(), "default").is_err());
    assert!(engine.delete(vec![], "default").is_err());
}

#[test]
fn test_oversized_key_is_rejected() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());
    let big_key = vec![0u8; u16::MAX as usize + 1];
    assert!(engine.put(big_key, b"v".to_vec(), "default").is_err());
}

// ── WAL crash recovery ────────────────────────────────────────────────────────

#[test]
fn test_wal_recovery_after_restart() {
    let tmp = TempDir::new().unwrap();

    // Write data and sync WAL, but do NOT flush to SSTable
    {
        let engine = open_engine(tmp.path());
        engine
            .put(b"persistent".to_vec(), b"value".to_vec(), "default")
            .unwrap();
        engine
            .put(b"also_here".to_vec(), b"yes".to_vec(), "default")
            .unwrap();
        engine.sync().unwrap(); // fsync WAL — simulates clean shutdown without flush
                                // Drop engine without calling shutdown() — simulates crash after sync
    }

    // Reopen — should recover from WAL
    {
        let engine = open_engine(tmp.path());
        assert_eq!(
            engine.get(b"persistent", "default").unwrap(),
            Some(b"value".to_vec()),
            "WAL recovery should restore written keys"
        );
        assert_eq!(
            engine.get(b"also_here", "default").unwrap(),
            Some(b"yes".to_vec())
        );
    }
}

#[test]
fn test_data_survives_clean_shutdown() {
    let tmp = TempDir::new().unwrap();

    {
        let engine = open_engine(tmp.path());
        for i in 0u32..100 {
            let key = format!("key_{i:04}").into_bytes();
            let val = format!("val_{i}").into_bytes();
            engine.put(key, val, "default").unwrap();
        }
        engine.shutdown().unwrap();
    }

    {
        let engine = open_engine(tmp.path());
        for i in 0u32..100 {
            let key = format!("key_{i:04}").into_bytes();
            let expected = format!("val_{i}").into_bytes();
            assert_eq!(
                engine.get(&key, "default").unwrap(),
                Some(expected),
                "key_{i} missing after restart"
            );
        }
    }
}

// ── Flush to SSTable ──────────────────────────────────────────────────────────

#[test]
fn test_data_readable_after_flush_to_sstable() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    // Write enough to trigger a flush
    for i in 0u32..5000 {
        let key = format!("k{i:06}").into_bytes();
        engine.put(key, vec![0u8; 100], "default").unwrap();
    }

    // Force flush — synchronously waits until all data is on disk
    engine.flush().unwrap();

    // All keys should still be readable from SSTable
    for i in 0u32..5000 {
        let key = format!("k{i:06}").into_bytes();
        assert!(
            engine.get(&key, "default").unwrap().is_some(),
            "k{i} missing after flush"
        );
    }
}

#[test]
fn test_delete_visible_after_flush() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine
        .put(b"gone".to_vec(), b"here".to_vec(), "default")
        .unwrap();
    engine.flush().unwrap();

    // Key is in SSTable now
    assert!(
        engine.get(b"gone", "default").unwrap().is_some(),
        "key should exist after flush"
    );

    // Delete it (tombstone goes to active MemTable)
    engine.delete(b"gone".to_vec(), "default").unwrap();

    // Flush the tombstone to SSTable as well — this ensures the tombstone
    // is durably on disk and shadows the original value in all levels
    engine.flush().unwrap();

    // After flushing the tombstone, the key must be gone
    assert_eq!(engine.get(b"gone", "default").unwrap(), None);
}

#[test]
fn test_delete_shadows_sstable_without_flush() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    // Write and flush to SSTable
    engine.put(b"k".to_vec(), b"v".to_vec(), "default").unwrap();
    engine.flush().unwrap();
    assert!(engine.get(b"k", "default").unwrap().is_some());

    // Delete — tombstone in MemTable
    engine.delete(b"k".to_vec(), "default").unwrap();

    // Should be gone immediately (MemTable tombstone shadows SSTable)
    assert_eq!(engine.get(b"k", "default").unwrap(), None);
}

// ── Metrics ───────────────────────────────────────────────────────────────────

#[test]
fn test_metrics_track_operations() {
    let tmp = TempDir::new().unwrap();
    let engine = open_engine(tmp.path());

    engine.put(b"a".to_vec(), b"1".to_vec(), "default").unwrap();
    engine.put(b"b".to_vec(), b"2".to_vec(), "default").unwrap();
    engine.get(b"a", "default").unwrap();
    engine.get(b"missing", "default").unwrap();
    engine.delete(b"b".to_vec(), "default").unwrap();

    let m = engine.metrics();
    assert_eq!(m.writes_total, 2);
    assert_eq!(m.reads_total, 2);
    assert_eq!(m.deletes_total, 1);
    assert!(m.memtable_size_bytes > 0);
}

// ── Concurrent access ─────────────────────────────────────────────────────────

#[test]
fn test_concurrent_writes_and_reads() {
    use std::sync::Arc;
    use std::thread;

    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(open_engine(tmp.path()));

    let writers: Vec<_> = (0..4)
        .map(|t| {
            let e = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0u32..250 {
                    let key = format!("t{t}_k{i:04}").into_bytes();
                    e.put(key, vec![t as u8; 32], "default").unwrap();
                }
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }

    // All 1000 keys should be readable
    for t in 0u32..4 {
        for i in 0u32..250 {
            let key = format!("t{t}_k{i:04}").into_bytes();
            assert!(
                engine.get(&key, "default").unwrap().is_some(),
                "t{t}_k{i} missing"
            );
        }
    }
}

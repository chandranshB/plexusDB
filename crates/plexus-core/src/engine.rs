//! Top-level storage engine.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use plexus_meta::MetaStore;

use crate::cache::{BlockCache, CacheStats};
use crate::compaction::CompactionEngine;
use crate::manifest::Manifest;
use crate::memtable::MemTable;
use crate::sstable::reader::SsTableReader;
use crate::sstable::writer::{SsTableBuildResult, SsTableWriter, SsTableWriterConfig};
use crate::wal::Wal;
use crate::{EngineConfig, EngineError, Entry};

/// Return type for [`Engine::scan`].
pub type ScanResult = Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError>;

/// Maximum number of frozen MemTables allowed in the queue before writes are
/// back-pressured. Prevents unbounded memory growth if the background flush
/// thread falls behind.
const MAX_FROZEN_QUEUE: usize = 8;

/// Maximum allowed namespace length in bytes.
const MAX_NAMESPACE_LEN: usize = 256;

#[derive(Debug, Clone)]
pub struct EngineMetrics {
    pub writes_total: u64,
    pub reads_total: u64,
    pub deletes_total: u64,
    pub memtable_size_bytes: u64,
    pub memtable_entries: u64,
    pub wal_size_bytes: u64,
    pub sstable_count: usize,
    pub total_disk_bytes: u64,
    pub cache: CacheStats,
    pub level_counts: Vec<(u32, usize)>,
}

pub struct Engine {
    config: EngineConfig,
    active: RwLock<Arc<MemTable>>,
    frozen: RwLock<VecDeque<Arc<MemTable>>>,
    flush_signal: Arc<(StdMutex<bool>, Condvar)>,
    wal: Arc<Wal>,
    manifest: Arc<Manifest>,
    cache: Arc<BlockCache>,
    compaction: Arc<CompactionEngine>,
    #[allow(dead_code)]
    meta: Arc<MetaStore>,
    timestamp: AtomicU64,
    writes: AtomicU64,
    reads: AtomicU64,
    deletes: AtomicU64,
    shutdown: Arc<AtomicBool>,
    /// Incremented by the background thread each time it finishes a flush batch.
    flush_generation: AtomicU64,
    /// Number of MemTables currently being flushed by the background thread.
    flushing_count: AtomicU64,
}

impl Engine {
    pub fn open(config: EngineConfig) -> Result<Arc<Self>, EngineError> {
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| EngineError::Wal(format!("cannot create data dir: {e}")))?;
        let wal_dir = config.data_dir.join("wal");
        let meta_path = config.data_dir.join("meta.db");
        let sst_dir = config.data_dir.join("sst");
        std::fs::create_dir_all(&sst_dir)
            .map_err(|e| EngineError::SsTable(format!("cannot create sst dir: {e}")))?;
        let meta = Arc::new(MetaStore::open(&meta_path).map_err(EngineError::Meta)?);
        let wal = Arc::new(Wal::open(&wal_dir, config.wal_max_size as u64)?);
        let manifest = Arc::new(Manifest::new(
            Arc::clone(&meta),
            &sst_dir,
            config.max_levels,
        )?);
        let cache = Arc::new(BlockCache::new(config.block_cache_size));
        let compaction = Arc::new(CompactionEngine::new(config.clone()));
        let active = Arc::new(MemTable::new());
        // Replay ALL WAL files in the directory (not just the current one).
        // After a rotation, sealed WAL files may contain entries that were never
        // flushed to SSTable. We must replay them in sequence-number order.
        let mut wal_files = Wal::list_all(&wal_dir)?;
        wal_files.sort(); // wal_000000.log < wal_000001.log — lexicographic = numeric order
        let mut max_ts = 0u64;
        let mut total_recovered = 0usize;
        for wal_path in &wal_files {
            let recovered = Wal::replay(wal_path)?;
            total_recovered += recovered.len();
            for entry in recovered {
                max_ts = max_ts.max(entry.timestamp);
                active
                    .put(entry)
                    .map_err(|e| EngineError::Wal(e.to_string()))?;
            }
        }
        tracing::info!(entries = total_recovered, max_ts, wal_files = wal_files.len(), "WAL recovery complete");
        let flush_signal = Arc::new((StdMutex::new(false), Condvar::new()));
        let engine = Arc::new(Self {
            config,
            active: RwLock::new(active),
            frozen: RwLock::new(VecDeque::new()),
            flush_signal: Arc::clone(&flush_signal),
            wal,
            manifest,
            cache,
            compaction,
            meta,
            timestamp: AtomicU64::new(max_ts + 1),
            writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            shutdown: Arc::new(AtomicBool::new(false)),
            flush_generation: AtomicU64::new(0),
            flushing_count: AtomicU64::new(0),
        });
        let bg = Arc::clone(&engine);
        std::thread::Builder::new()
            .name("plexus-bg".into())
            .spawn(move || bg.background_loop())
            .map_err(|e| EngineError::Wal(format!("cannot spawn background thread: {e}")))?;
        tracing::info!(data_dir = %engine.config.data_dir.display(), sstables = engine.manifest.total_sstables(), "engine opened");
        Ok(engine)
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, namespace: &str) -> Result<u64, EngineError> {
        if key.is_empty() {
            return Err(EngineError::Wal("key must not be empty".into()));
        }
        if key.len() > u16::MAX as usize {
            return Err(EngineError::Wal(format!(
                "key too large: {} bytes (max {})",
                key.len(),
                u16::MAX
            )));
        }
        if value.len() > u32::MAX as usize {
            return Err(EngineError::Wal(format!(
                "value too large: {} bytes (max {})",
                value.len(),
                u32::MAX
            )));
        }
        if namespace.is_empty() || namespace.len() > MAX_NAMESPACE_LEN {
            return Err(EngineError::Wal(format!(
                "namespace must be 1–{MAX_NAMESPACE_LEN} bytes"
            )));
        }
        // Back-pressure: if the frozen queue is full, wait briefly for the
        // background thread to drain it rather than growing memory unboundedly.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if self.frozen.read().len() < MAX_FROZEN_QUEUE {
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err(EngineError::Wal(
                    "write rejected: frozen queue full (flush backlog)".into(),
                ));
            }
            // Wake the background thread and yield
            let (lock, cvar) = &*self.flush_signal;
            *lock.lock().unwrap() = true;
            cvar.notify_one();
            std::thread::sleep(Duration::from_millis(1));
        }
        let ts = self.next_timestamp();
        let entry = Entry {
            key,
            value: Some(value),
            timestamp: ts,
            namespace: namespace.to_string(),
        };
        self.wal.append(&entry)?;
        self.active
            .read()
            .put(entry)
            .map_err(|e| EngineError::Wal(e.to_string()))?;
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.maybe_rotate_memtable();
        Ok(ts)
    }

    pub fn delete(&self, key: Vec<u8>, namespace: &str) -> Result<u64, EngineError> {
        if key.is_empty() {
            return Err(EngineError::Wal("key must not be empty".into()));
        }
        if key.len() > u16::MAX as usize {
            return Err(EngineError::Wal(format!(
                "key too large: {} bytes (max {})",
                key.len(),
                u16::MAX
            )));
        }
        if namespace.is_empty() || namespace.len() > MAX_NAMESPACE_LEN {
            return Err(EngineError::Wal(format!(
                "namespace must be 1–{MAX_NAMESPACE_LEN} bytes"
            )));
        }
        let ts = self.next_timestamp();
        let entry = Entry {
            key,
            value: None,
            timestamp: ts,
            namespace: namespace.to_string(),
        };
        self.wal.append(&entry)?;
        self.active
            .read()
            .put(entry)
            .map_err(|e| EngineError::Wal(e.to_string()))?;
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.maybe_rotate_memtable();
        Ok(ts)
    }

    pub fn get(&self, key: &[u8], namespace: &str) -> Result<Option<Vec<u8>>, EngineError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        // Check active MemTable — namespace is part of the skip list key
        if let Some(entry) = self.active.read().get(key, namespace) {
            return Ok(if entry.is_tombstone() {
                None
            } else {
                entry.value
            });
        }
        // Check frozen MemTables (newest first)
        for mt in self.frozen.read().iter().rev() {
            if let Some(entry) = mt.get(key, namespace) {
                return Ok(if entry.is_tombstone() {
                    None
                } else {
                    entry.value
                });
            }
        }
        // Check SSTables level by level.
        // L0 files may overlap in key range and time — we must check ALL of them
        // and pick the entry with the highest timestamp (newest write wins).
        // L1+ files are non-overlapping — the first match is authoritative.
        for level in 0..self.config.max_levels as u32 {
            let sstables = self.manifest.get_level(level);
            if sstables.is_empty() {
                continue;
            }

            if level == 0 {
                // L0: scan all files, collect the best (highest-timestamp) match
                let mut best: Option<Entry> = None;
                for sst in &sstables {
                    if key < sst.min_key.as_slice() || key > sst.max_key.as_slice() {
                        continue;
                    }
                    let reader = SsTableReader::open(&sst.path)?;
                    if !reader.may_contain(key) {
                        continue;
                    }
                    if let Some(entry) = reader.get(key)? {
                        if entry.namespace == namespace {
                            match &best {
                                None => best = Some(entry),
                                Some(b) if entry.timestamp > b.timestamp => best = Some(entry),
                                _ => {}
                            }
                        }
                    }
                }
                if let Some(entry) = best {
                    return Ok(if entry.is_tombstone() {
                        None
                    } else {
                        entry.value
                    });
                }
            } else {
                // L1+: non-overlapping, first match is authoritative
                for sst in &sstables {
                    if key < sst.min_key.as_slice() || key > sst.max_key.as_slice() {
                        continue;
                    }
                    let reader = SsTableReader::open(&sst.path)?;
                    if !reader.may_contain(key) {
                        continue;
                    }
                    if let Some(entry) = reader.get(key)? {
                        if entry.namespace == namespace {
                            return Ok(if entry.is_tombstone() {
                                None
                            } else {
                                entry.value
                            });
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn scan(&self, start: &[u8], end: &[u8], limit: usize, namespace: &str) -> ScanResult {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let effective_end: Option<&[u8]> = if end.is_empty() { None } else { Some(end) };
        let mut candidates: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();
        let mut merge = |entry: Entry| {
            if entry.key.as_slice() < start {
                return;
            }
            if let Some(e) = effective_end {
                if entry.key.as_slice() >= e {
                    return;
                }
            }
            // Strict namespace isolation — only match the requested namespace
            if entry.namespace != namespace {
                return;
            }
            candidates
                .entry(entry.key.clone())
                .and_modify(|ex| {
                    if entry.timestamp > ex.timestamp {
                        *ex = entry.clone();
                    }
                })
                .or_insert(entry);
        };
        for entry in self.active.read().iter() {
            merge(entry);
        }
        for mt in self.frozen.read().iter() {
            for entry in mt.iter() {
                merge(entry);
            }
        }
        for level in 0..self.config.max_levels as u32 {
            for sst in self.manifest.get_level(level) {
                // Skip SSTables whose key range doesn't overlap the scan range
                if !start.is_empty() && sst.max_key.as_slice() < start {
                    continue;
                }
                if let Some(e) = effective_end {
                    if sst.min_key.as_slice() >= e {
                        continue;
                    }
                }
                for entry in SsTableReader::open(&sst.path)?.iter()? {
                    merge(entry);
                }
            }
        }
        let mut results = Vec::new();
        for (key, entry) in candidates {
            if entry.is_tombstone() {
                continue;
            }
            if let Some(value) = entry.value {
                results.push((key, value));
                if limit > 0 && results.len() >= limit {
                    break;
                }
            }
        }
        Ok(results)
    }

    pub fn sync(&self) -> Result<(), EngineError> {
        self.wal.sync()
    }

    /// Return the data directory path for this engine instance.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.config.data_dir
    }

    pub fn flush(&self) -> Result<(), EngineError> {
        // Rotate the active MemTable into the frozen queue
        self.rotate_memtable();

        // Signal the background thread to wake up and flush
        let (lock, cvar) = &*self.flush_signal;
        *lock.lock().unwrap() = true;
        cvar.notify_one();

        // Wait for the background thread to drain the frozen queue completely.
        // We poll with a short sleep rather than a condvar to keep the logic simple
        // and avoid introducing another synchronization primitive.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let queue_empty = self.frozen.read().is_empty();
            let none_in_flight = self.flushing_count.load(Ordering::Acquire) == 0;
            if queue_empty && none_in_flight {
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err(EngineError::Wal("flush timed out after 30s".into()));
            }
            std::thread::sleep(Duration::from_millis(2));
        }

        self.wal.sync()
    }

    pub fn metrics(&self) -> EngineMetrics {
        let active = self.active.read();
        EngineMetrics {
            writes_total: self.writes.load(Ordering::Relaxed),
            reads_total: self.reads.load(Ordering::Relaxed),
            deletes_total: self.deletes.load(Ordering::Relaxed),
            memtable_size_bytes: active.size(),
            memtable_entries: active.count(),
            wal_size_bytes: self.wal.size(),
            sstable_count: self.manifest.total_sstables(),
            total_disk_bytes: self.manifest.total_size(),
            cache: self.cache.stats(),
            level_counts: self.manifest.level_counts(),
        }
    }

    pub fn shutdown(&self) -> Result<(), EngineError> {
        tracing::info!("engine shutting down");
        self.compaction.shutdown();

        // Flush all pending data BEFORE signaling the background thread to stop.
        // This ensures the background thread can still process the frozen queue.
        self.flush()?;

        // Now signal the background thread to exit.
        self.shutdown.store(true, Ordering::Release);
        let (lock, cvar) = &*self.flush_signal;
        *lock.lock().unwrap() = true;
        cvar.notify_all();

        self.wal.sync()?;
        tracing::info!("engine shutdown complete");
        Ok(())
    }

    fn next_timestamp(&self) -> u64 {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        self.timestamp
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
                Some(prev.max(wall) + 1)
            })
            .unwrap_or_else(|v| v)
    }

    fn maybe_rotate_memtable(&self) {
        if self.active.read().should_flush(self.config.memtable_size) {
            self.rotate_memtable();
        }
    }

    fn rotate_memtable(&self) {
        let new_active = Arc::new(MemTable::new());
        let old = {
            let mut active = self.active.write();
            let old = Arc::clone(&*active);
            old.freeze();
            *active = new_active;
            old
        };
        if !old.is_empty() {
            self.frozen.write().push_back(old);
            let (lock, cvar) = &*self.flush_signal;
            *lock.lock().unwrap() = true;
            cvar.notify_one();
        }
    }

    fn flush_frozen(&self) -> Result<(), EngineError> {
        loop {
            let mt = match self.frozen.write().pop_front() {
                Some(m) => m,
                None => break,
            };
            // Skip empty memtables — nothing to flush
            if mt.is_empty() {
                continue;
            }
            self.flushing_count.fetch_add(1, Ordering::AcqRel);
            let result = self.flush_memtable_to_sst(&mt);
            // Always decrement — even on error — to prevent flush() from
            // waiting forever if flush_memtable_to_sst returns Err.
            self.flushing_count.fetch_sub(1, Ordering::AcqRel);
            let result = result?;
            tracing::info!(entries = result.entry_count, size = result.file_size, path = %result.path.display(), "flushed MemTable to SSTable");
            let file_name = format!(
                "L0/{}",
                result
                    .path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
            );
            self.manifest
                .add_sstable(&plexus_meta::queries::SsTableInfo {
                    id: 0,
                    file_name,
                    level: 0,
                    file_size: result.file_size,
                    entry_count: result.entry_count,
                    min_key: result.min_key,
                    max_key: result.max_key,
                    min_timestamp: result.min_timestamp,
                    max_timestamp: result.max_timestamp,
                    bloom_offset: result.bloom_offset,
                    index_offset: result.index_offset,
                    checksum: result.checksum,
                    storage_tier: "local".to_string(),
                    s3_key: None,
                })?;
        }
        Ok(())
    }

    fn flush_memtable_to_sst(&self, mt: &MemTable) -> Result<SsTableBuildResult, EngineError> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        let sst_dir = self.config.data_dir.join("sst").join("L0");
        std::fs::create_dir_all(&sst_dir)
            .map_err(|e| EngineError::SsTable(format!("cannot create L0 dir: {e}")))?;
        let path = sst_dir.join(format!("l0_{ts}.sst"));
        let mut writer = SsTableWriter::new(
            &path,
            SsTableWriterConfig {
                expected_entries: mt.count() as usize,
                ..SsTableWriterConfig::from(&self.config)
            },
        )?;
        for entry in mt.iter() {
            writer.add(&entry)?;
        }
        writer.finish()
    }

    fn maybe_compact(&self) {
        // ── L0 → L1 ──────────────────────────────────────────────────────────
        let l0: Vec<PathBuf> = self
            .manifest
            .get_level(0)
            .into_iter()
            .map(|e| e.path)
            .collect();
        if l0.len() >= self.compaction.l0_compaction_trigger() {
            let l1: Vec<PathBuf> = self
                .manifest
                .get_level(1)
                .into_iter()
                .map(|e| e.path)
                .collect();
            let sst_dir = self.config.data_dir.join("sst").join("L1");
            if let Err(e) = std::fs::create_dir_all(&sst_dir) {
                tracing::error!(error = %e, "cannot create L1 dir");
                return;
            }
            let job = match self.compaction.plan_l0_compaction(&l0, &l1, &sst_dir) {
                Some(j) => j,
                None => return,
            };
            match self.compaction.compact(&job) {
                Ok(result) => {
                    // Two-phase commit: register new SSTables in manifest FIRST,
                    // then delete the input files. If we crash between these two
                    // steps, the input files are still present and the manifest
                    // will be consistent on recovery.
                    self.register_compaction_output(&result, 1);
                    self.remove_compacted_files(&result);
                    tracing::info!(
                        written = result.entries_written,
                        discarded = result.entries_discarded,
                        "L0->L1 compaction complete"
                    );
                }                Err(e) => tracing::error!(error = %e, "L0->L1 compaction failed"),
            }
        }

        // ── L1 → L6 (leveled compaction) ─────────────────────────────────────
        for level in 1..(self.config.max_levels as u32 - 1) {
            let level_size = self.manifest.level_size(level);
            let max_size = self.compaction.level_max_size(level);
            if level_size <= max_size {
                continue;
            }
            let to_level = level + 1;
            let sst_dir = self
                .config
                .data_dir
                .join("sst")
                .join(format!("L{to_level}"));
            if let Err(e) = std::fs::create_dir_all(&sst_dir) {
                tracing::error!(error = %e, level = to_level, "cannot create SST dir");
                continue;
            }
            // Pick the oldest SSTable at this level to compact
            let level_files = self.manifest.get_level(level);
            let candidate = match level_files.first() {
                Some(f) => f.clone(),
                None => continue,
            };
            // Find overlapping files in the next level
            let next_files =
                self.manifest
                    .get_overlapping(to_level, &candidate.min_key, &candidate.max_key);
            let mut input_files = vec![candidate.path.clone()];
            input_files.extend(next_files.iter().map(|f| f.path.clone()));

            let job = crate::compaction::CompactionJob {
                from_level: level,
                to_level,
                input_files,
                output_dir: sst_dir,
            };
            match self.compaction.compact(&job) {
                Ok(result) => {
                    // Two-phase commit: register new SSTables in manifest FIRST,
                    // then delete the input files. If we crash between these two
                    // steps, the input files are still present and the manifest
                    // will be consistent on recovery.
                    self.register_compaction_output(&result, to_level);
                    self.remove_compacted_files(&result);
                    tracing::info!(
                        from = level,
                        to = to_level,
                        written = result.entries_written,
                        discarded = result.entries_discarded,
                        "leveled compaction complete"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, from = level, to = to_level, "leveled compaction failed")
                }
            }
            // Only compact one level per background tick to avoid starvation
            break;
        }
    }

    /// Register compacted output SSTables in the manifest.
    fn register_compaction_output(
        &self,
        result: &crate::compaction::CompactionResult,
        to_level: u32,
    ) {
        for build in &result.output_files {
            // Skip empty SSTables (e.g. all tombstones were dropped)
            if build.entry_count == 0 {
                let _ = std::fs::remove_file(&build.path);
                continue;
            }
            let file_name = format!(
                "L{to_level}/{}",
                build.path.file_name().unwrap_or_default().to_string_lossy()
            );
            if let Err(e) = self
                .manifest
                .add_sstable(&plexus_meta::queries::SsTableInfo {
                    id: 0,
                    file_name,
                    level: to_level,
                    file_size: build.file_size,
                    entry_count: build.entry_count,
                    min_key: build.min_key.clone(),
                    max_key: build.max_key.clone(),
                    min_timestamp: build.min_timestamp,
                    max_timestamp: build.max_timestamp,
                    bloom_offset: build.bloom_offset,
                    index_offset: build.index_offset,
                    checksum: build.checksum.clone(),
                    storage_tier: "local".to_string(),
                    s3_key: None,
                })
            {
                tracing::error!(error = %e, "failed to register compacted SSTable");
            }
        }
    }

    /// Remove input files from manifest and disk after successful compaction.
    fn remove_compacted_files(&self, result: &crate::compaction::CompactionResult) {
        let names: Vec<String> = result
            .input_files
            .iter()
            .filter_map(|p| {
                let f = p.file_name()?.to_str()?;
                let par = p.parent()?.file_name()?.to_str()?;
                Some(format!("{par}/{f}"))
            })
            .collect();
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        if let Err(e) = self.manifest.mark_compacted(&refs) {
            tracing::error!(error = %e, "mark_compacted failed");
            return;
        }
        // Invalidate block cache for deleted SSTables, then remove from disk
        for path in &result.input_files {
            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            self.cache.invalidate_file(file_name);
            let _ = std::fs::remove_file(path);
        }
    }

    fn background_loop(&self) {
        tracing::info!("background thread started");
        let (lock, cvar) = &*self.flush_signal;
        let mut consecutive_errors = 0u32;
        loop {
            {
                let g = lock.lock().unwrap();
                let _ = cvar.wait_timeout(g, Duration::from_millis(200));
            }
            if self.shutdown.load(Ordering::Acquire) {
                break;
            }

            match self.flush_frozen() {
                Ok(()) => {
                    consecutive_errors = 0;
                    self.flush_generation.fetch_add(1, Ordering::Release);
                }
                Err(e) => {
                    consecutive_errors += 1;
                    tracing::error!(error = %e, consecutive = consecutive_errors, "background flush failed");
                    if consecutive_errors >= 10 {
                        tracing::error!("too many consecutive flush failures — background thread pausing for 5s");
                        std::thread::sleep(Duration::from_secs(5));
                        consecutive_errors = 0;
                    }
                    continue;
                }
            }

            self.maybe_compact();

            if self.wal.needs_rotation() {
                match self.wal.rotate() {
                    Ok(old) => tracing::debug!(path = %old.display(), "WAL rotated"),
                    Err(e) => tracing::error!(error = %e, "WAL rotation failed"),
                }
            }
        }
        tracing::info!("background thread stopped");
    }
}

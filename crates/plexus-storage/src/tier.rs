//! Tiered storage routing — decides which storage tier a component uses.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use super::detect::{StorageDevice, StorageKind};
use super::config::StorageConfig;

/// Storage tier classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    /// Hot tier (SSD/NVMe) — WAL, index, bloom filters.
    Hot,
    /// Warm tier (HDD or SSD overflow) — SSTable data blocks.
    Warm,
    /// Cold tier (S3) — frozen/archived data.
    Cold,
}

/// Storage mode based on detected hardware.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageMode {
    /// Both SSD and HDD available — optimal tiering.
    Hybrid,
    /// SSD only — optimize for parallelism.
    SsdOnly,
    /// HDD only — optimize for sequential I/O.
    HddOnly,
}

impl std::fmt::Display for StorageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageMode::Hybrid => write!(f, "hybrid"),
            StorageMode::SsdOnly => write!(f, "ssd_only"),
            StorageMode::HddOnly => write!(f, "hdd_only"),
        }
    }
}

/// Routes data components to appropriate storage tiers.
pub struct TierRouter {
    mode: StorageMode,
    hot_path: PathBuf,
    warm_path: PathBuf,
    config: StorageConfig,
}

impl TierRouter {
    /// Create a tier router based on detected storage devices.
    pub fn new(devices: &[StorageDevice], config: StorageConfig) -> Self {
        let has_ssd = devices.iter().any(|d| {
            matches!(d.kind, StorageKind::SSD | StorageKind::NVMe)
        });
        let has_hdd = devices.iter().any(|d| d.kind == StorageKind::HDD);

        let mode = match (has_ssd, has_hdd) {
            (true, true) => StorageMode::Hybrid,
            (true, false) => StorageMode::SsdOnly,
            (false, true) => StorageMode::HddOnly,
            (false, false) => StorageMode::SsdOnly, // default safe
        };

        // Determine paths based on mode
        let (hot_path, warm_path) = match mode {
            StorageMode::Hybrid => {
                let ssd = devices.iter().find(|d| {
                    matches!(d.kind, StorageKind::SSD | StorageKind::NVMe)
                }).unwrap();
                let hdd = devices.iter().find(|d| d.kind == StorageKind::HDD).unwrap();
                (
                    ssd.mount_point.join(&config.data_subdir),
                    hdd.mount_point.join(&config.data_subdir),
                )
            }
            _ => {
                let base = config.data_dir.clone();
                (base.join("hot"), base.join("warm"))
            }
        };

        tracing::info!(
            mode = %mode,
            hot = %hot_path.display(),
            warm = %warm_path.display(),
            "tier router initialized"
        );

        Self { mode, hot_path, warm_path, config }
    }

    /// Get the storage mode.
    pub fn mode(&self) -> StorageMode {
        self.mode
    }

    /// Get the path for the WAL directory.
    pub fn wal_dir(&self) -> PathBuf {
        self.hot_path.join("wal")
    }

    /// Get the path for bloom filter storage.
    pub fn bloom_dir(&self) -> PathBuf {
        self.hot_path.join("bloom")
    }

    /// Get the path for the metadata database.
    pub fn meta_path(&self) -> PathBuf {
        self.hot_path.join("meta.db")
    }

    /// Get the path for SSTable files at a given level.
    pub fn sstable_dir(&self, level: u32) -> PathBuf {
        match self.mode {
            StorageMode::Hybrid => {
                if level <= 1 {
                    // L0-L1 on SSD for fast compaction reads
                    self.hot_path.join("sst").join(format!("L{level}"))
                } else {
                    // L2+ on HDD for sequential bulk storage
                    self.warm_path.join("sst").join(format!("L{level}"))
                }
            }
            StorageMode::SsdOnly => {
                self.hot_path.join("sst").join(format!("L{level}"))
            }
            StorageMode::HddOnly => {
                self.warm_path.join("sst").join(format!("L{level}"))
            }
        }
    }

    /// Get recommended I/O parallelism for the current mode.
    pub fn recommended_parallelism(&self) -> usize {
        match self.mode {
            StorageMode::Hybrid => 4,   // moderate parallelism
            StorageMode::SsdOnly => 8,  // high parallelism
            StorageMode::HddOnly => 1,  // single-threaded sequential
        }
    }

    /// Get recommended compaction threads.
    pub fn recommended_compaction_threads(&self) -> usize {
        match self.mode {
            StorageMode::Hybrid => 2,
            StorageMode::SsdOnly => 4,
            StorageMode::HddOnly => 1,
        }
    }

    /// Check if a device is approaching capacity.
    pub fn should_tier_to_s3(&self, device: &StorageDevice) -> bool {
        device.usage_percent >= self.config.s3_threshold
    }

    /// Create all necessary directories.
    pub fn create_directories(&self) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(self.wal_dir())?;
        std::fs::create_dir_all(self.bloom_dir())?;
        if let Some(parent) = self.meta_path().parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create SSTable directories for each level
        for level in 0..7 {
            std::fs::create_dir_all(self.sstable_dir(level))?;
        }

        Ok(())
    }
}

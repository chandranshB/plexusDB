//! Tiered storage routing — decides which storage tier a component uses.

use super::config::StorageConfig;
use super::detect::{StorageDevice, StorageKind};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
        let has_ssd = devices
            .iter()
            .any(|d| matches!(d.kind, StorageKind::SSD | StorageKind::NVMe));
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
                let ssd = devices
                    .iter()
                    .find(|d| matches!(d.kind, StorageKind::SSD | StorageKind::NVMe))
                    .unwrap();
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

        Self {
            mode,
            hot_path,
            warm_path,
            config,
        }
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
            StorageMode::SsdOnly => self.hot_path.join("sst").join(format!("L{level}")),
            StorageMode::HddOnly => self.warm_path.join("sst").join(format!("L{level}")),
        }
    }

    /// Get recommended I/O parallelism for the current mode.
    pub fn recommended_parallelism(&self) -> usize {
        match self.mode {
            StorageMode::Hybrid => 4,  // moderate parallelism
            StorageMode::SsdOnly => 8, // high parallelism
            StorageMode::HddOnly => 1, // single-threaded sequential
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::detect::{StorageDevice, StorageKind};
    use std::path::PathBuf;

    fn ssd_device(mount: &str) -> StorageDevice {
        StorageDevice {
            device_path: "/dev/sda".into(),
            mount_point: PathBuf::from(mount),
            kind: StorageKind::SSD,
            total_bytes: 500 * 1024 * 1024 * 1024,
            available_bytes: 200 * 1024 * 1024 * 1024,
            usage_percent: 0.6,
        }
    }

    fn hdd_device(mount: &str) -> StorageDevice {
        StorageDevice {
            device_path: "/dev/sdb".into(),
            mount_point: PathBuf::from(mount),
            kind: StorageKind::HDD,
            total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            available_bytes: 1024 * 1024 * 1024 * 1024,
            usage_percent: 0.5,
        }
    }

    fn nvme_device(mount: &str) -> StorageDevice {
        StorageDevice {
            device_path: "/dev/nvme0n1".into(),
            mount_point: PathBuf::from(mount),
            kind: StorageKind::NVMe,
            total_bytes: 1024 * 1024 * 1024 * 1024,
            available_bytes: 800 * 1024 * 1024 * 1024,
            usage_percent: 0.2,
        }
    }

    fn config() -> StorageConfig {
        StorageConfig {
            data_dir: PathBuf::from("/tmp/plexus-test"),
            ..StorageConfig::default()
        }
    }

    #[test]
    fn test_ssd_only_mode() {
        let devices = vec![ssd_device("/")];
        let router = TierRouter::new(&devices, config());
        assert_eq!(router.mode(), StorageMode::SsdOnly);
    }

    #[test]
    fn test_hdd_only_mode() {
        let devices = vec![hdd_device("/")];
        let router = TierRouter::new(&devices, config());
        assert_eq!(router.mode(), StorageMode::HddOnly);
    }

    #[test]
    fn test_hybrid_mode() {
        let devices = vec![ssd_device("/"), hdd_device("/mnt/data")];
        let router = TierRouter::new(&devices, config());
        assert_eq!(router.mode(), StorageMode::Hybrid);
    }

    #[test]
    fn test_nvme_treated_as_ssd_for_mode() {
        let devices = vec![nvme_device("/")];
        let router = TierRouter::new(&devices, config());
        assert_eq!(router.mode(), StorageMode::SsdOnly);
    }

    #[test]
    fn test_empty_devices_defaults_to_ssd_only() {
        let router = TierRouter::new(&[], config());
        assert_eq!(router.mode(), StorageMode::SsdOnly);
    }

    #[test]
    fn test_parallelism_recommendations() {
        let ssd_router = TierRouter::new(&[ssd_device("/")], config());
        let hdd_router = TierRouter::new(&[hdd_device("/")], config());
        let hybrid_router = TierRouter::new(&[ssd_device("/"), hdd_device("/mnt/data")], config());

        assert!(ssd_router.recommended_parallelism() > hdd_router.recommended_parallelism());
        assert_eq!(hdd_router.recommended_parallelism(), 1);
        assert!(hybrid_router.recommended_parallelism() >= 1);
    }

    #[test]
    fn test_compaction_threads() {
        let ssd_router = TierRouter::new(&[ssd_device("/")], config());
        let hdd_router = TierRouter::new(&[hdd_device("/")], config());

        assert!(
            ssd_router.recommended_compaction_threads()
                > hdd_router.recommended_compaction_threads()
        );
        assert_eq!(hdd_router.recommended_compaction_threads(), 1);
    }

    #[test]
    fn test_sstable_dir_hybrid_routing() {
        let devices = vec![ssd_device("/"), hdd_device("/mnt/data")];
        let router = TierRouter::new(&devices, config());

        // L0 and L1 should be on hot (SSD) path
        let l0 = router.sstable_dir(0);
        let l1 = router.sstable_dir(1);
        // L2+ should be on warm (HDD) path
        let l2 = router.sstable_dir(2);

        // In hybrid mode, L0/L1 are on SSD mount, L2+ on HDD mount
        assert!(l0.to_string_lossy().contains("L0"));
        assert!(l1.to_string_lossy().contains("L1"));
        assert!(l2.to_string_lossy().contains("L2"));
    }

    #[test]
    fn test_should_tier_to_s3() {
        let cfg = StorageConfig {
            s3_threshold: 0.8,
            ..config()
        };
        let devices = vec![ssd_device("/")];
        let router = TierRouter::new(&devices, cfg);

        let low_usage = StorageDevice {
            usage_percent: 0.5,
            ..ssd_device("/")
        };
        let high_usage = StorageDevice {
            usage_percent: 0.9,
            ..ssd_device("/")
        };

        assert!(!router.should_tier_to_s3(&low_usage));
        assert!(router.should_tier_to_s3(&high_usage));
    }

    #[test]
    fn test_storage_mode_display() {
        assert_eq!(StorageMode::Hybrid.to_string(), "hybrid");
        assert_eq!(StorageMode::SsdOnly.to_string(), "ssd_only");
        assert_eq!(StorageMode::HddOnly.to_string(), "hdd_only");
    }

    #[test]
    fn test_create_directories() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cfg = StorageConfig {
            data_dir: tmp.path().to_path_buf(),
            ..StorageConfig::default()
        };
        let devices = vec![ssd_device("/")];
        let router = TierRouter::new(&devices, cfg);
        router.create_directories().unwrap();

        assert!(router.wal_dir().exists());
        assert!(router.bloom_dir().exists());
        assert!(router.sstable_dir(0).exists());
        assert!(router.sstable_dir(6).exists());
    }
}

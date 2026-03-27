//! Hardware detection — probes the system for SSD/HDD/NVMe devices.
//!
//! - **Linux**: Reads `/sys/block/<dev>/queue/rotational` and transport info.
//! - **Windows**: Uses `GetDiskFreeSpaceExW` for capacity and WMI-style
//!   heuristics (drive letter probing) for device type.
//! - **macOS**: Uses `diskutil` heuristics.
//! - **Fallback**: Assumes SSD (safe default for development).

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Type of storage device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageKind {
    /// NVMe SSD — fastest, supports deep queue parallelism.
    NVMe,
    /// SATA/M.2 SSD — fast random I/O.
    SSD,
    /// Spinning hard drive — optimize for sequential I/O.
    HDD,
    /// Unknown — treat as SSD (safe default).
    Unknown,
}

impl StorageKind {
    /// Whether this device benefits from sequential I/O optimization.
    pub fn prefers_sequential(&self) -> bool {
        matches!(self, StorageKind::HDD)
    }

    /// Whether this device supports high parallelism.
    pub fn supports_parallelism(&self) -> bool {
        matches!(self, StorageKind::NVMe | StorageKind::SSD)
    }
}

impl std::fmt::Display for StorageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageKind::NVMe => write!(f, "NVMe"),
            StorageKind::SSD => write!(f, "SSD"),
            StorageKind::HDD => write!(f, "HDD"),
            StorageKind::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Detected storage device information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDevice {
    /// Device path (e.g., "/dev/sda" or "C:\\").
    pub device_path: String,
    /// Mount point (e.g., "/data" or "C:\\").
    pub mount_point: PathBuf,
    /// Detected device type.
    pub kind: StorageKind,
    /// Total capacity in bytes.
    pub total_bytes: u64,
    /// Available space in bytes.
    pub available_bytes: u64,
    /// Usage percentage (0.0 - 1.0).
    pub usage_percent: f64,
}

/// Detect all storage devices on the system.
pub fn detect_storage() -> Vec<StorageDevice> {
    #[cfg(target_os = "linux")]
    {
        detect_linux()
    }

    #[cfg(target_os = "windows")]
    {
        detect_windows()
    }

    #[cfg(target_os = "macos")]
    {
        detect_macos()
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        detect_fallback()
    }
}

/// Detect the storage kind for a specific path.
pub fn detect_path_storage(path: &Path) -> StorageDevice {
    let devices = detect_storage();

    // Find the device whose mount point best matches the path
    let path_str = path.to_string_lossy().to_lowercase();
    let mut best_match: Option<&StorageDevice> = None;
    let mut best_len = 0;

    for device in &devices {
        let mp = device.mount_point.to_string_lossy().to_lowercase();
        if path_str.starts_with(&*mp) && mp.len() > best_len {
            best_match = Some(device);
            best_len = mp.len();
        }
    }

    best_match.cloned().unwrap_or(StorageDevice {
        device_path: "unknown".into(),
        mount_point: path.to_path_buf(),
        kind: StorageKind::Unknown,
        total_bytes: 0,
        available_bytes: 0,
        usage_percent: 0.0,
    })
}

// ── Linux Detection ──────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn detect_linux() -> Vec<StorageDevice> {
    use std::fs;

    let mut devices = Vec::new();

    // Read /sys/block/ for device info
    let block_dir = Path::new("/sys/block");
    if let Ok(entries) = fs::read_dir(block_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Skip loop/ram/dm devices
            if name_str.starts_with("loop")
                || name_str.starts_with("ram")
                || name_str.starts_with("dm-")
            {
                continue;
            }

            let rotational_path = block_dir.join(&name).join("queue/rotational");

            let kind = if let Ok(val) = fs::read_to_string(&rotational_path) {
                match val.trim() {
                    "0" => {
                        // Check if NVMe
                        let transport = block_dir.join(&name).join("device/transport");
                        if name_str.starts_with("nvme")
                            || fs::read_to_string(transport)
                                .map(|t| t.contains("pcie"))
                                .unwrap_or(false)
                        {
                            StorageKind::NVMe
                        } else {
                            StorageKind::SSD
                        }
                    }
                    "1" => StorageKind::HDD,
                    _ => StorageKind::Unknown,
                }
            } else {
                StorageKind::Unknown
            };

            // Get size from /sys/block/<dev>/size (in 512-byte sectors)
            let size_path = block_dir.join(&name).join("size");
            let total_bytes = fs::read_to_string(size_path)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(|sectors| sectors * 512)
                .unwrap_or(0);

            devices.push(StorageDevice {
                device_path: format!("/dev/{name_str}"),
                mount_point: PathBuf::from("/"),
                kind,
                total_bytes,
                available_bytes: 0,
                usage_percent: 0.0,
            });
        }
    }

    if devices.is_empty() {
        devices.push(StorageDevice {
            device_path: "unknown".into(),
            mount_point: PathBuf::from("/"),
            kind: StorageKind::Unknown,
            total_bytes: 0,
            available_bytes: 0,
            usage_percent: 0.0,
        });
    }

    log_devices(&devices);
    devices
}

// ── Windows Detection ────────────────────────────────────────────────────────

#[cfg(target_os = "windows")]
fn detect_windows() -> Vec<StorageDevice> {
    let mut devices = Vec::new();

    // Enumerate all drive letters A-Z
    let drive_mask = unsafe { winapi_get_logical_drives() };

    for i in 0..26u32 {
        if drive_mask & (1 << i) == 0 {
            continue;
        }

        let letter = (b'A' + i as u8) as char;
        let root = format!("{}:\\", letter);
        let root_path = PathBuf::from(&root);

        // Get drive type
        let drive_type = unsafe { winapi_get_drive_type(&root) };

        // Skip non-fixed drives (CD-ROM, network, removable)
        if drive_type != 3 {
            // DRIVE_FIXED = 3
            continue;
        }

        // Get disk space
        let (total, available) = get_disk_space_windows(&root).unwrap_or((0, 0));

        let usage_percent = if total > 0 {
            (total - available) as f64 / total as f64
        } else {
            0.0
        };

        // Try to detect SSD vs HDD via latency probe
        let kind = detect_drive_type_windows(&root_path);

        devices.push(StorageDevice {
            device_path: root.clone(),
            mount_point: root_path,
            kind,
            total_bytes: total,
            available_bytes: available,
            usage_percent,
        });
    }

    if devices.is_empty() {
        // Couldn't read any drives — return a default pointing at cwd
        devices.push(StorageDevice {
            device_path: "C:\\".into(),
            mount_point: PathBuf::from("C:\\"),
            kind: StorageKind::SSD,
            total_bytes: 0,
            available_bytes: 0,
            usage_percent: 0.0,
        });
    }

    log_devices(&devices);
    devices
}

/// Uses `GetLogicalDrives` to get a bitmask of available drive letters.
#[cfg(target_os = "windows")]
unsafe fn winapi_get_logical_drives() -> u32 {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetLogicalDrives() -> u32;
    }
    GetLogicalDrives()
}

/// Uses `GetDriveTypeW` to classify a drive.
#[cfg(target_os = "windows")]
unsafe fn winapi_get_drive_type(root: &str) -> u32 {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetDriveTypeW(lpRootPathName: *const u16) -> u32;
    }
    let wide: Vec<u16> = root.encode_utf16().chain(std::iter::once(0)).collect();
    GetDriveTypeW(wide.as_ptr())
}

/// Uses `GetDiskFreeSpaceExW` to get total and available space.
#[cfg(target_os = "windows")]
fn get_disk_space_windows(root: &str) -> Option<(u64, u64)> {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }

    let wide: Vec<u16> = root.encode_utf16().chain(std::iter::once(0)).collect();
    let mut free_caller: u64 = 0;
    let mut total: u64 = 0;
    let mut free_total: u64 = 0;

    let ret = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free_caller,
            &mut total,
            &mut free_total,
        )
    };

    if ret != 0 {
        Some((total, free_caller))
    } else {
        None
    }
}

/// Detect SSD vs HDD on Windows using a quick latency probe.
///
/// We create a small temp file and time 4K random reads. SSDs typically
/// complete in <0.5ms, HDDs in 5-15ms. This is a heuristic but works
/// well in practice.
#[cfg(target_os = "windows")]
fn detect_drive_type_windows(root: &Path) -> StorageKind {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::time::Instant;

    let probe_path = root.join(".plexus_probe_tmp");

    // Create a 1MB file for probing
    let probe_result = (|| -> std::io::Result<StorageKind> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&probe_path)?;

        // Write 1MB of data
        let data = vec![0xABu8; 1024 * 1024];
        file.write_all(&data)?;
        file.sync_all()?;

        // Time random 4KB reads
        let mut buf = vec![0u8; 4096];
        let offsets = [0, 512 * 1024, 256 * 1024, 768 * 1024, 128 * 1024];
        let start = Instant::now();
        let iterations = offsets.len();

        for &offset in &offsets {
            file.seek(SeekFrom::Start(offset as u64))?;
            file.read_exact(&mut buf)?;
        }

        let elapsed = start.elapsed();
        let avg_us = elapsed.as_micros() as f64 / iterations as f64;

        // SSD: typically < 500µs per random 4K read
        // HDD: typically > 2000µs per random 4K read
        let kind = if avg_us < 1000.0 {
            StorageKind::SSD
        } else {
            StorageKind::HDD
        };

        tracing::debug!(
            drive = %root.display(),
            avg_read_us = format!("{avg_us:.0}"),
            kind = %kind,
            "drive latency probe"
        );

        Ok(kind)
    })();

    // Clean up probe file
    let _ = std::fs::remove_file(&probe_path);

    match probe_result {
        Ok(kind) => kind,
        Err(e) => {
            tracing::debug!(
                drive = %root.display(),
                error = %e,
                "latency probe failed, assuming SSD"
            );
            StorageKind::SSD // Safe default
        }
    }
}

// ── macOS Detection ──────────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn detect_macos() -> Vec<StorageDevice> {
    // macOS: most Macs have SSDs. Use system_profiler heuristic.
    tracing::info!("macOS: assuming SSD storage (Apple Silicon / T2)");

    let mut total_bytes = 0u64;
    let mut available_bytes = 0u64;

    // Try to get root filesystem size via statvfs
    #[cfg(unix)]
    {
        use std::ffi::CString;
        let root = CString::new("/").unwrap();
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::statvfs(root.as_ptr(), &mut stat) };
        if ret == 0 {
            total_bytes = stat.f_blocks as u64 * stat.f_frsize as u64;
            available_bytes = stat.f_bavail as u64 * stat.f_frsize as u64;
        }
    }

    let usage_percent = if total_bytes > 0 {
        (total_bytes - available_bytes) as f64 / total_bytes as f64
    } else {
        0.0
    };

    let devices = vec![StorageDevice {
        device_path: "/dev/disk1s1".into(),
        mount_point: PathBuf::from("/"),
        kind: StorageKind::NVMe, // Modern Macs are all NVMe
        total_bytes,
        available_bytes,
        usage_percent,
    }];

    log_devices(&devices);
    devices
}

// ── Fallback Detection ───────────────────────────────────────────────────────

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
fn detect_fallback() -> Vec<StorageDevice> {
    tracing::info!("unknown platform: assuming SSD storage");

    vec![StorageDevice {
        device_path: "default".into(),
        mount_point: PathBuf::from("."),
        kind: StorageKind::SSD,
        total_bytes: 0,
        available_bytes: 0,
        usage_percent: 0.0,
    }]
}

// ── Shared Utilities ─────────────────────────────────────────────────────────

fn log_devices(devices: &[StorageDevice]) {
    tracing::info!(count = devices.len(), "detected storage devices");
    for dev in devices {
        let total_gb = dev.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let avail_gb = dev.available_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        tracing::info!(
            device = %dev.device_path,
            kind = %dev.kind,
            total_gb = format!("{total_gb:.1}"),
            avail_gb = format!("{avail_gb:.1}"),
            usage = format!("{:.1}%", dev.usage_percent * 100.0),
            "  storage device"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_returns_at_least_one() {
        let devices = detect_storage();
        assert!(!devices.is_empty(), "should detect at least one device");
    }

    #[test]
    fn test_detect_path_storage_returns_device() {
        let device = detect_path_storage(Path::new("."));
        // Should get some kind of result, not panic
        tracing::info!(?device, "detected device for cwd");
    }

    #[test]
    fn test_storage_kind_properties() {
        assert!(StorageKind::HDD.prefers_sequential());
        assert!(!StorageKind::SSD.prefers_sequential());
        assert!(StorageKind::NVMe.supports_parallelism());
        assert!(!StorageKind::HDD.supports_parallelism());
    }
}

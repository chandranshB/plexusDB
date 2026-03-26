//! Storage configuration.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};

/// Configuration for the storage subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base data directory.
    pub data_dir: PathBuf,
    /// Subdirectory name for PlexusDB data.
    pub data_subdir: String,
    /// S3 tiering threshold (0.0 - 1.0). Default: 0.8.
    pub s3_threshold: f64,
    /// S3 bucket name.
    pub s3_bucket: Option<String>,
    /// S3 endpoint (for MinIO / compatible).
    pub s3_endpoint: Option<String>,
    /// S3 region.
    pub s3_region: Option<String>,
    /// Enable automatic S3 tiering.
    pub s3_enabled: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./plexus-data"),
            data_subdir: "plexus".into(),
            s3_threshold: 0.8,
            s3_bucket: None,
            s3_endpoint: None,
            s3_region: Some("us-east-1".into()),
            s3_enabled: false,
        }
    }
}

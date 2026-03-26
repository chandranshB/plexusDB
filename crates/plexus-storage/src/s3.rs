//! S3 cold-tier agent — compresses and moves frozen SSTables to S3.
//!
//! When local disk usage exceeds the configured threshold (default 80%),
//! the oldest SSTables are compressed with zstd and uploaded to S3.

use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use super::config::StorageConfig;

/// S3 upload result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3UploadResult {
    pub s3_key: String,
    pub bucket: String,
    pub compressed_size: u64,
    pub original_size: u64,
}

/// S3 tiering agent.
pub struct S3Agent {
    config: StorageConfig,
}

impl S3Agent {
    pub fn new(config: StorageConfig) -> Self {
        Self { config }
    }

    /// Check if S3 tiering is configured and enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.s3_enabled && self.config.s3_bucket.is_some()
    }

    /// Compress an SSTable file for S3 upload.
    pub fn compress_for_upload(input: &Path) -> Result<(PathBuf, u64, u64), std::io::Error> {
        let data = std::fs::read(input)?;
        let original_size = data.len() as u64;

        let compressed = zstd::encode_all(data.as_slice(), 6)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let compressed_size = compressed.len() as u64;

        let output = input.with_extension("sst.zst");
        std::fs::write(&output, &compressed)?;

        tracing::info!(
            input = %input.display(),
            original = original_size,
            compressed = compressed_size,
            ratio = format!("{:.1}x", original_size as f64 / compressed_size as f64),
            "compressed SSTable for S3"
        );

        Ok((output, original_size, compressed_size))
    }

    /// Generate S3 key for an SSTable.
    pub fn s3_key(&self, file_name: &str) -> String {
        format!("plexus/frozen/{file_name}.zst")
    }

    /// Upload would use aws-sdk-s3 — placeholder for the async implementation.
    /// The actual upload is handled in the async runtime context.
    pub fn bucket(&self) -> Option<&str> {
        self.config.s3_bucket.as_deref()
    }
}

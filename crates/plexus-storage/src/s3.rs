//! S3 cold-tier agent — compresses and moves frozen SSTables to S3.
//!
//! When local disk usage exceeds the configured threshold (default 80%),
//! the oldest SSTables are compressed with zstd and uploaded to S3.

use super::config::StorageConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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

        let compressed = zstd::encode_all(data.as_slice(), 6).map_err(std::io::Error::other)?;
        let compressed_size = compressed.len() as u64;

        let output = input.with_extension("sst.zst");
        std::fs::write(&output, &compressed)?;

        tracing::info!(
            input = %input.display(),
            original = original_size,
            compressed = compressed_size,
            ratio = %format!("{:.1}x", original_size as f64 / compressed_size as f64),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_s3() -> StorageConfig {
        StorageConfig {
            s3_enabled: true,
            s3_bucket: Some("test-bucket".to_string()),
            ..StorageConfig::default()
        }
    }

    fn config_no_s3() -> StorageConfig {
        StorageConfig {
            s3_enabled: false,
            s3_bucket: None,
            ..StorageConfig::default()
        }
    }

    #[test]
    fn test_is_enabled_with_bucket() {
        let agent = S3Agent::new(config_with_s3());
        assert!(agent.is_enabled());
    }

    #[test]
    fn test_is_disabled_without_bucket() {
        let agent = S3Agent::new(config_no_s3());
        assert!(!agent.is_enabled());
    }

    #[test]
    fn test_is_disabled_when_flag_false() {
        let cfg = StorageConfig {
            s3_enabled: false,
            s3_bucket: Some("bucket".to_string()),
            ..StorageConfig::default()
        };
        let agent = S3Agent::new(cfg);
        assert!(!agent.is_enabled());
    }

    #[test]
    fn test_s3_key_format() {
        let agent = S3Agent::new(config_with_s3());
        let key = agent.s3_key("l2_000001.sst");
        assert_eq!(key, "plexus/frozen/l2_000001.sst.zst");
    }

    #[test]
    fn test_bucket_accessor() {
        let agent = S3Agent::new(config_with_s3());
        assert_eq!(agent.bucket(), Some("test-bucket"));

        let agent_no_bucket = S3Agent::new(config_no_s3());
        assert_eq!(agent_no_bucket.bucket(), None);
    }

    #[test]
    fn test_compress_for_upload() {
        let tmp = tempfile::TempDir::new().unwrap();
        let input = tmp.path().join("test.sst");
        let data = vec![0xABu8; 4096]; // 4KB of compressible data
        std::fs::write(&input, &data).unwrap();

        let (output_path, original_size, compressed_size) =
            S3Agent::compress_for_upload(&input).unwrap();

        assert!(output_path.exists());
        assert_eq!(original_size, 4096);
        assert!(
            compressed_size < original_size,
            "compressed should be smaller than original"
        );
        assert!(output_path.to_string_lossy().ends_with(".zst"));
    }
}

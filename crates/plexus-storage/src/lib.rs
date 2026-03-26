//! # plexus-storage
//!
//! Hardware detection and intelligent tiered storage routing.
//! The "Brain" of PlexusDB that decides WHERE data lives.

pub mod detect;
pub mod tier;
pub mod config;
pub mod s3;

pub use detect::{StorageDevice, StorageKind, detect_storage};
pub use tier::{TierRouter, StorageTier};
pub use config::StorageConfig;

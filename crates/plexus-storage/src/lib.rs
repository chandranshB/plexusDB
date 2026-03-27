//! # plexus-storage
//!
//! Hardware detection and intelligent tiered storage routing.
//! The "Brain" of PlexusDB that decides WHERE data lives.

pub mod config;
pub mod detect;
pub mod s3;
pub mod tier;

pub use config::StorageConfig;
pub use detect::{detect_storage, StorageDevice, StorageKind};
pub use tier::{StorageTier, TierRouter};

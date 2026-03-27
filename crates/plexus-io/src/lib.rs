//! # plexus-io
//!
//! Hardware-aware I/O abstraction layer for PlexusDB.
//!
//! Provides a unified [`IoBackend`] trait with two implementations:
//! - **`UringBackend`** (Linux): Uses `io_uring` with `O_DIRECT` for zero-copy,
//!   kernel-bypass I/O that saturates NVMe bandwidth.
//! - **`FallbackBackend`** (all platforms): Standard `pread`/`pwrite` for
//!   development and non-Linux deployments.
//!
//! All I/O uses 4096-byte aligned buffers via [`AlignedBuf`] to satisfy
//! `O_DIRECT` alignment requirements and enable DMA.

pub mod aligned;
pub mod traits;

#[cfg(target_os = "linux")]
pub mod uring;

pub mod fallback;

pub use aligned::{round_up_to_block, AlignedBuf, AlignedBufPool};
pub use traits::{IoBackend, IoCommand, IoCompletion};

pub use fallback::FallbackBackend;
#[cfg(target_os = "linux")]
pub use uring::UringBackend;

/// Block size used for Direct I/O alignment.
pub const BLOCK_SIZE: usize = 4096;

/// Maximum number of io_uring SQEs to batch before submitting.
pub const MAX_SQE_BATCH: usize = 64;

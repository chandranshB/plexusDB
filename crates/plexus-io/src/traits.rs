//! I/O backend trait definitions.
//!
//! All storage engine I/O goes through the [`IoBackend`] trait, allowing
//! transparent switching between `io_uring` (production) and standard
//! file I/O (development/non-Linux).

use crate::AlignedBuf;
use std::path::Path;

/// Errors from the I/O backend.
#[derive(Debug, thiserror::Error)]
pub enum IoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("buffer alignment error: address {addr:#x} is not {required}-byte aligned")]
    Alignment { addr: usize, required: usize },

    #[error("io_uring submission queue full")]
    QueueFull,

    #[error("operation cancelled")]
    Cancelled,

    #[error("file not found: {0}")]
    NotFound(String),
}

/// Represents a pending I/O command submitted to the backend.
#[derive(Debug, Clone)]
pub struct IoCommand {
    /// Unique ID for tracking this operation.
    pub id: u64,
    /// Type of operation.
    pub op: IoOp,
}

/// Types of I/O operations.
#[derive(Debug, Clone)]
pub enum IoOp {
    /// Read `len` bytes from `offset` in the file.
    Read { fd: i32, offset: u64, len: usize },
    /// Write buffer contents at `offset` in the file.
    Write { fd: i32, offset: u64 },
    /// Sync file data to disk (fdatasync).
    Fsync { fd: i32 },
}

/// Result of a completed I/O operation.
#[derive(Debug)]
pub struct IoCompletion {
    /// ID matching the original [`IoCommand`].
    pub id: u64,
    /// Number of bytes transferred, or error.
    pub result: Result<usize, IoError>,
    /// Buffer (returned to caller after read/write).
    pub buffer: Option<AlignedBuf>,
}

/// File handle returned by the backend.
#[derive(Debug)]
pub struct FileHandle {
    /// OS file descriptor.
    pub fd: i32,
    /// Whether this file was opened with O_DIRECT.
    pub direct_io: bool,
    /// File path (for debugging).
    pub path: String,
}

/// Open mode for files.
#[derive(Debug, Clone, Copy)]
pub enum OpenMode {
    /// Read-only access.
    ReadOnly,
    /// Write-only, create if not exists, append.
    WriteOnly,
    /// Read-write access, create if not exists.
    ReadWrite,
    /// Create new file, error if exists.
    CreateNew,
}

/// The core I/O backend trait.
///
/// Implementors provide platform-specific I/O optimized for the storage engine.
/// All operations use [`AlignedBuf`] to satisfy `O_DIRECT` requirements.
pub trait IoBackend: Send + Sync {
    /// Open a file with the specified mode.
    ///
    /// If `direct` is true, the file is opened with `O_DIRECT` (or equivalent).
    fn open(&self, path: &Path, mode: OpenMode, direct: bool) -> Result<FileHandle, IoError>;

    /// Close a file handle.
    fn close(&self, handle: FileHandle) -> Result<(), IoError>;

    /// Read `len` bytes from `offset` into an aligned buffer.
    ///
    /// Returns the buffer with data. The caller is responsible for returning
    /// the buffer to the pool when done.
    fn read(&self, handle: &FileHandle, offset: u64, len: usize) -> Result<AlignedBuf, IoError>;

    /// Write an aligned buffer to the file at `offset`.
    ///
    /// Returns the number of bytes written.
    fn write(&self, handle: &FileHandle, offset: u64, buf: &AlignedBuf) -> Result<usize, IoError>;

    /// Append data to the end of the file.
    ///
    /// Returns the offset at which the data was written and bytes written.
    fn append(&self, handle: &FileHandle, buf: &AlignedBuf) -> Result<(u64, usize), IoError>;

    /// Sync file data to stable storage.
    fn fsync(&self, handle: &FileHandle) -> Result<(), IoError>;

    /// Get the current file size.
    fn file_size(&self, handle: &FileHandle) -> Result<u64, IoError>;

    /// Delete a file from disk.
    fn delete(&self, path: &Path) -> Result<(), IoError>;

    /// Check if a file exists.
    fn exists(&self, path: &Path) -> bool;

    /// Get the name of this backend (for logging).
    fn name(&self) -> &'static str;
}

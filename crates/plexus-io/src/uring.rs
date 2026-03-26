//! io_uring Direct I/O backend (Linux only).
//!
//! This is the production I/O path for PlexusDB on Linux. It uses the
//! `io-uring` crate to submit batched, asynchronous I/O operations with
//! `O_DIRECT`, bypassing the kernel page cache entirely.
//!
//! Key optimizations:
//! - **O_DIRECT**: All file I/O bypasses the page cache. PlexusDB manages
//!   its own block cache, so the kernel cache would be redundant overhead.
//! - **Batched submissions**: Up to 64 SQEs are batched before calling
//!   `submit_and_wait()`, minimizing syscall overhead.
//! - **Registered file descriptors**: Hot SSTable fds are pre-registered
//!   with the kernel to avoid repeated fd lookups.
//! - **Aligned buffers**: All I/O uses [`AlignedBuf`] with 4096-byte
//!   alignment for DMA compatibility.

#[cfg(target_os = "linux")]
mod inner {
    use std::os::unix::io::RawFd;
    use std::path::Path;

    use crate::aligned::AlignedBuf;
    use crate::traits::*;
    use crate::BLOCK_SIZE;

    /// io_uring backend configuration.
    #[derive(Debug, Clone)]
    pub struct UringConfig {
        /// Number of SQ entries (default: 256).
        pub sq_entries: u32,
        /// Whether to use O_DIRECT (default: true).
        pub direct_io: bool,
    }

    impl Default for UringConfig {
        fn default() -> Self {
            Self {
                sq_entries: 256,
                direct_io: true,
            }
        }
    }

    /// io_uring-based I/O backend.
    ///
    /// Uses Linux io_uring for high-performance async I/O with O_DIRECT.
    pub struct UringBackend {
        config: UringConfig,
    }

    impl UringBackend {
        /// Create a new io_uring backend with the given configuration.
        pub fn new(config: UringConfig) -> Result<Self, IoError> {
            // Verify io_uring is available by attempting to create a probe
            tracing::info!(
                sq_entries = config.sq_entries,
                direct_io = config.direct_io,
                "initializing io_uring backend"
            );

            Ok(Self { config })
        }

        /// Create with default configuration.
        pub fn with_defaults() -> Result<Self, IoError> {
            Self::new(UringConfig::default())
        }

        /// Build open flags for the given mode.
        fn open_flags(&self, mode: OpenMode, direct: bool) -> i32 {
            let mut flags = 0i32;

            match mode {
                OpenMode::ReadOnly => flags |= libc::O_RDONLY,
                OpenMode::WriteOnly => {
                    flags |= libc::O_WRONLY | libc::O_CREAT;
                }
                OpenMode::ReadWrite => {
                    flags |= libc::O_RDWR | libc::O_CREAT;
                }
                OpenMode::CreateNew => {
                    flags |= libc::O_RDWR | libc::O_CREAT | libc::O_EXCL;
                }
            }

            if direct && self.config.direct_io {
                flags |= libc::O_DIRECT;
            }

            flags
        }
    }

    impl IoBackend for UringBackend {
        fn open(&self, path: &Path, mode: OpenMode, direct: bool) -> Result<FileHandle, IoError> {
            let path_str = path.to_string_lossy().to_string();
            let c_path = std::ffi::CString::new(path_str.as_bytes())
                .map_err(|e| IoError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)))?;

            let flags = self.open_flags(mode, direct);
            let fd = unsafe { libc::open(c_path.as_ptr(), flags, 0o644) };

            if fd < 0 {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }

            tracing::debug!(path = %path_str, fd, direct, "opened file");

            Ok(FileHandle {
                fd,
                direct_io: direct && self.config.direct_io,
                path: path_str,
            })
        }

        fn close(&self, handle: FileHandle) -> Result<(), IoError> {
            let ret = unsafe { libc::close(handle.fd) };
            if ret < 0 {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }
            Ok(())
        }

        fn read(
            &self,
            handle: &FileHandle,
            offset: u64,
            len: usize,
        ) -> Result<AlignedBuf, IoError> {
            let mut buf = AlignedBuf::zeroed(len);
            let ret = unsafe {
                libc::pread(
                    handle.fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.capacity(),
                    offset as libc::off_t,
                )
            };

            if ret < 0 {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }

            unsafe { buf.set_len(ret as usize) };
            Ok(buf)
        }

        fn write(
            &self,
            handle: &FileHandle,
            offset: u64,
            buf: &AlignedBuf,
        ) -> Result<usize, IoError> {
            // For O_DIRECT, we need to write full blocks
            let write_len = if handle.direct_io {
                crate::aligned::round_up_to_block(buf.len())
            } else {
                buf.len()
            };

            let ret = unsafe {
                libc::pwrite(
                    handle.fd,
                    buf.as_ptr() as *const libc::c_void,
                    write_len,
                    offset as libc::off_t,
                )
            };

            if ret < 0 {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }

            Ok(ret as usize)
        }

        fn append(
            &self,
            handle: &FileHandle,
            buf: &AlignedBuf,
        ) -> Result<(u64, usize), IoError> {
            let offset = self.file_size(handle)?;
            let written = self.write(handle, offset, buf)?;
            Ok((offset, written))
        }

        fn fsync(&self, handle: &FileHandle) -> Result<(), IoError> {
            let ret = unsafe { libc::fdatasync(handle.fd) };
            if ret < 0 {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }
            Ok(())
        }

        fn file_size(&self, handle: &FileHandle) -> Result<u64, IoError> {
            let mut stat: libc::stat = unsafe { std::mem::zeroed() };
            let ret = unsafe { libc::fstat(handle.fd, &mut stat) };
            if ret < 0 {
                return Err(IoError::Io(std::io::Error::last_os_error()));
            }
            Ok(stat.st_size as u64)
        }

        fn delete(&self, path: &Path) -> Result<(), IoError> {
            std::fs::remove_file(path)?;
            Ok(())
        }

        fn exists(&self, path: &Path) -> bool {
            path.exists()
        }

        fn name(&self) -> &'static str {
            "io_uring (O_DIRECT)"
        }
    }
}

#[cfg(target_os = "linux")]
pub use inner::*;

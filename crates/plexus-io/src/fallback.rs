//! Fallback I/O backend using standard file operations.
//!
//! Used on non-Linux platforms (macOS, Windows) for development, and as a
//! degraded-mode option on Linux if io_uring is unavailable.
//!
//! This backend uses standard `pread`/`pwrite` (or `seek` + `read`/`write`
//! on Windows) and does NOT use `O_DIRECT`. It still uses [`AlignedBuf`]
//! for API compatibility.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::aligned::AlignedBuf;
use crate::traits::*;

/// Standard file I/O backend (cross-platform).
pub struct FallbackBackend {
    /// File handles indexed by fd (simulated).
    files: Mutex<Vec<Option<File>>>,
}

impl FallbackBackend {
    pub fn new() -> Self {
        Self {
            files: Mutex::new(Vec::new()),
        }
    }

    fn store_file(&self, file: File) -> i32 {
        let mut files = self.files.lock().unwrap();
        // Find empty slot or append
        for (i, slot) in files.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(file);
                return i as i32;
            }
        }
        let fd = files.len() as i32;
        files.push(Some(file));
        fd
    }

    fn _get_file(&self, fd: i32) -> Result<std::sync::MutexGuard<'_, Vec<Option<File>>>, IoError> {
        let files = self.files.lock().unwrap();
        if fd < 0 || fd as usize >= files.len() || files[fd as usize].is_none() {
            return Err(IoError::NotFound(format!("fd {fd} not found")));
        }
        Ok(files)
    }
}

impl Default for FallbackBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl IoBackend for FallbackBackend {
    fn open(&self, path: &Path, mode: OpenMode, _direct: bool) -> Result<FileHandle, IoError> {
        let file = match mode {
            OpenMode::ReadOnly => {
                OpenOptions::new().read(true).open(path)?
            }
            OpenMode::WriteOnly => {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)?
            }
            OpenMode::ReadWrite => {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?
            }
            OpenMode::CreateNew => {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path)?
            }
        };

        let fd = self.store_file(file);
        let path_str = path.to_string_lossy().to_string();

        tracing::debug!(path = %path_str, fd, "opened file (fallback backend)");

        Ok(FileHandle {
            fd,
            direct_io: false,
            path: path_str,
        })
    }

    fn close(&self, handle: FileHandle) -> Result<(), IoError> {
        let mut files = self.files.lock().unwrap();
        if let Some(slot) = files.get_mut(handle.fd as usize) {
            *slot = None; // Drop the File, closing it
        }
        Ok(())
    }

    fn read(
        &self,
        handle: &FileHandle,
        offset: u64,
        len: usize,
    ) -> Result<AlignedBuf, IoError> {
        let mut files = self.files.lock().unwrap();
        let file = files[handle.fd as usize]
            .as_mut()
            .ok_or_else(|| IoError::NotFound(format!("fd {}", handle.fd)))?;

        file.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; len];
        let n = file.read(&mut data)?;
        data.truncate(n);

        Ok(AlignedBuf::from_slice(&data))
    }

    fn write(
        &self,
        handle: &FileHandle,
        offset: u64,
        buf: &AlignedBuf,
    ) -> Result<usize, IoError> {
        let mut files = self.files.lock().unwrap();
        let file = files[handle.fd as usize]
            .as_mut()
            .ok_or_else(|| IoError::NotFound(format!("fd {}", handle.fd)))?;

        file.seek(SeekFrom::Start(offset))?;
        let written = file.write(buf.as_bytes())?;
        Ok(written)
    }

    fn append(
        &self,
        handle: &FileHandle,
        buf: &AlignedBuf,
    ) -> Result<(u64, usize), IoError> {
        let mut files = self.files.lock().unwrap();
        let file = files[handle.fd as usize]
            .as_mut()
            .ok_or_else(|| IoError::NotFound(format!("fd {}", handle.fd)))?;

        let offset = file.seek(SeekFrom::End(0))?;
        let written = file.write(buf.as_bytes())?;
        Ok((offset, written))
    }

    fn fsync(&self, handle: &FileHandle) -> Result<(), IoError> {
        let files = self.files.lock().unwrap();
        let file = files[handle.fd as usize]
            .as_ref()
            .ok_or_else(|| IoError::NotFound(format!("fd {}", handle.fd)))?;

        file.sync_data()?;
        Ok(())
    }

    fn file_size(&self, handle: &FileHandle) -> Result<u64, IoError> {
        let files = self.files.lock().unwrap();
        let file = files[handle.fd as usize]
            .as_ref()
            .ok_or_else(|| IoError::NotFound(format!("fd {}", handle.fd)))?;

        let metadata = file.metadata()?;
        Ok(metadata.len())
    }

    fn delete(&self, path: &Path) -> Result<(), IoError> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn name(&self) -> &'static str {
        "fallback (standard I/O)"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_dir() -> PathBuf {
        let dir = std::env::temp_dir().join("plexus_io_test");
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_write_read_roundtrip() {
        let backend = FallbackBackend::new();
        let path = test_dir().join("test_roundtrip.dat");
        let _ = std::fs::remove_file(&path);

        let handle = backend.open(&path, OpenMode::ReadWrite, false).unwrap();

        let data = b"hello plexusdb engine";
        let buf = AlignedBuf::from_slice(data);
        let written = backend.write(&handle, 0, &buf).unwrap();
        assert_eq!(written, data.len());

        let read_buf = backend.read(&handle, 0, data.len()).unwrap();
        assert_eq!(read_buf.as_bytes(), data);

        backend.close(handle).unwrap();
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_append() {
        let backend = FallbackBackend::new();
        let path = test_dir().join("test_append.dat");
        let _ = std::fs::remove_file(&path);

        let handle = backend.open(&path, OpenMode::ReadWrite, false).unwrap();

        let buf1 = AlignedBuf::from_slice(b"aaa");
        let (off1, _) = backend.append(&handle, &buf1).unwrap();
        assert_eq!(off1, 0);

        let buf2 = AlignedBuf::from_slice(b"bbb");
        let (off2, _) = backend.append(&handle, &buf2).unwrap();
        assert_eq!(off2, 3);

        let read = backend.read(&handle, 0, 6).unwrap();
        assert_eq!(read.as_bytes(), b"aaabbb");

        backend.close(handle).unwrap();
        std::fs::remove_file(&path).unwrap();
    }
}

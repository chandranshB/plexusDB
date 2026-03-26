//! 4096-byte aligned buffer allocator for Direct I/O.
//!
//! `O_DIRECT` requires all buffers to be aligned to the block device's logical
//! sector size (typically 512 or 4096 bytes). This module provides [`AlignedBuf`],
//! a heap-allocated buffer with guaranteed 4096-byte alignment that enables
//! the kernel to perform DMA directly to/from the drive.

use std::alloc::{Layout, alloc, dealloc};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use crate::BLOCK_SIZE;

/// A heap-allocated buffer with guaranteed 4096-byte alignment.
///
/// This is the fundamental I/O unit for PlexusDB. Every read and write
/// operation passes through an `AlignedBuf` to satisfy `O_DIRECT` requirements.
///
/// # Safety
///
/// The buffer is allocated via `std::alloc::alloc` with alignment = 4096.
/// It is deallocated on `Drop`. The buffer must not be used after drop.
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
    capacity: usize,
}

// SAFETY: AlignedBuf owns its memory and doesn't use thread-local storage.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Create a new aligned buffer with the given capacity.
    ///
    /// Capacity is rounded up to the nearest multiple of `BLOCK_SIZE` (4096).
    ///
    /// # Panics
    ///
    /// Panics if allocation fails (out of memory).
    pub fn new(capacity: usize) -> Self {
        let capacity = round_up(capacity, BLOCK_SIZE);
        let layout = Layout::from_size_align(capacity, BLOCK_SIZE)
            .expect("invalid layout");

        // SAFETY: Layout is valid (non-zero size, power-of-two alignment).
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr).expect("allocation failed: out of memory");

        AlignedBuf {
            ptr,
            len: 0,
            capacity,
        }
    }

    /// Create a zeroed aligned buffer.
    pub fn zeroed(capacity: usize) -> Self {
        let mut buf = Self::new(capacity);
        // SAFETY: ptr is valid for `capacity` bytes.
        unsafe {
            std::ptr::write_bytes(buf.ptr.as_ptr(), 0, buf.capacity);
        }
        buf.len = buf.capacity;
        buf
    }

    /// Create an aligned buffer from existing data.
    ///
    /// Data is copied into a new aligned allocation. The capacity is rounded
    /// up to the nearest block size.
    pub fn from_slice(data: &[u8]) -> Self {
        let mut buf = Self::new(data.len());
        // SAFETY: Both pointers are valid, non-overlapping, and sized correctly.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf.ptr.as_ptr(), data.len());
        }
        buf.len = data.len();
        buf
    }

    /// Returns the number of bytes written to this buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if no bytes have been written.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the total allocated capacity (always a multiple of 4096).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns a raw pointer to the buffer. Used for io_uring SQE submissions.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to the buffer.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Set the logical length of the buffer (e.g., after an io_uring read completes).
    ///
    /// # Safety
    ///
    /// Caller must ensure that `len <= capacity` and that the first `len` bytes
    /// contain valid data.
    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.capacity);
        self.len = len;
    }

    /// Append data to the buffer. Returns the number of bytes actually written.
    pub fn extend_from_slice(&mut self, data: &[u8]) -> usize {
        let available = self.capacity - self.len;
        let to_copy = data.len().min(available);
        if to_copy > 0 {
            // SAFETY: We've verified the copy fits within capacity.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    self.ptr.as_ptr().add(self.len),
                    to_copy,
                );
            }
            self.len += to_copy;
        }
        to_copy
    }

    /// Reset the buffer length to 0 without deallocating.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Returns the data as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: ptr is valid for `len` bytes, which were written by safe code.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns the full capacity as a mutable byte slice (for reads).
    #[inline]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for `capacity` bytes.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity) }
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl DerefMut for AlignedBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for `len` bytes.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, BLOCK_SIZE)
            .expect("invalid layout in drop");
        // SAFETY: ptr was allocated with this exact layout.
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl Clone for AlignedBuf {
    fn clone(&self) -> Self {
        Self::from_slice(self.as_bytes())
    }
}

impl std::fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuf")
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .field("aligned", &(self.ptr.as_ptr() as usize % BLOCK_SIZE == 0))
            .finish()
    }
}

/// Round `value` up to the nearest multiple of `align`.
#[inline]
const fn round_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Round `value` up to the nearest block size (4096).
/// Public utility for other crates (e.g., the uring backend).
#[inline]
pub const fn round_up_to_block(value: usize) -> usize {
    round_up(value, BLOCK_SIZE)
}

// ── Buffer Pool ──────────────────────────────────────────────────────────────

/// A pool of reusable aligned buffers to minimize allocation overhead.
///
/// Hot-path I/O operations acquire a buffer from the pool and return it
/// after completion, avoiding repeated `alloc`/`dealloc` syscalls.
pub struct AlignedBufPool {
    buffers: crossbeam_channel::Receiver<AlignedBuf>,
    sender: crossbeam_channel::Sender<AlignedBuf>,
    buf_capacity: usize,
}

impl AlignedBufPool {
    /// Create a new pool with `count` pre-allocated buffers of `buf_capacity` bytes.
    pub fn new(count: usize, buf_capacity: usize) -> Self {
        let (sender, buffers) = crossbeam_channel::bounded(count);
        for _ in 0..count {
            let _ = sender.send(AlignedBuf::zeroed(buf_capacity));
        }
        AlignedBufPool {
            buffers,
            sender,
            buf_capacity,
        }
    }

    /// Acquire a buffer from the pool. Allocates a new one if the pool is empty.
    pub fn acquire(&self) -> AlignedBuf {
        self.buffers
            .try_recv()
            .unwrap_or_else(|_| AlignedBuf::zeroed(self.buf_capacity))
    }

    /// Return a buffer to the pool. If the pool is full, the buffer is dropped.
    pub fn release(&self, mut buf: AlignedBuf) {
        buf.clear();
        let _ = self.sender.try_send(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alignment() {
        let buf = AlignedBuf::new(100);
        assert_eq!(buf.as_ptr() as usize % BLOCK_SIZE, 0);
        assert_eq!(buf.capacity(), BLOCK_SIZE); // rounded up
    }

    #[test]
    fn test_from_slice() {
        let data = b"hello plexus";
        let buf = AlignedBuf::from_slice(data);
        assert_eq!(buf.as_bytes(), data);
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.capacity(), BLOCK_SIZE);
    }

    #[test]
    fn test_extend() {
        let mut buf = AlignedBuf::new(BLOCK_SIZE);
        let written = buf.extend_from_slice(b"hello");
        assert_eq!(written, 5);
        assert_eq!(buf.len(), 5);
        assert_eq!(&buf[..5], b"hello");
    }

    #[test]
    fn test_zeroed() {
        let buf = AlignedBuf::zeroed(BLOCK_SIZE);
        assert!(buf.iter().all(|&b| b == 0));
        assert_eq!(buf.len(), BLOCK_SIZE);
    }

    #[test]
    fn test_pool() {
        let pool = AlignedBufPool::new(4, BLOCK_SIZE);
        let b1 = pool.acquire();
        let b2 = pool.acquire();
        assert_eq!(b1.capacity(), BLOCK_SIZE);
        pool.release(b1);
        pool.release(b2);
        // Pool should have 4 buffers again
        let _b3 = pool.acquire();
    }

    #[test]
    fn test_round_up() {
        assert_eq!(round_up(0, 4096), 0);
        assert_eq!(round_up(1, 4096), 4096);
        assert_eq!(round_up(4096, 4096), 4096);
        assert_eq!(round_up(4097, 4096), 8192);
    }
}

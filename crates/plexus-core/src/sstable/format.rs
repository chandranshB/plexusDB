//! SSTable (Sorted String Table) on-disk format specification.
//!
//! ## File Layout
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │                    Data Blocks                           │
//! │  ┌─────────────────────────────────────────────────────┐ │
//! │  │ Block 0: [entry₀, entry₁, ..., entryₙ] (compressed)│ │
//! │  ├─────────────────────────────────────────────────────┤ │
//! │  │ Block 1: [entry₀, entry₁, ..., entryₘ] (compressed)│ │
//! │  ├─────────────────────────────────────────────────────┤ │
//! │  │ ...                                                 │ │
//! │  └─────────────────────────────────────────────────────┘ │
//! ├──────────────────────────────────────────────────────────┤
//! │                    Meta Block                            │
//! │  ┌─────────────────────────────────────────────────────┐ │
//! │  │ Bloom Filter (serialized bit array)                 │ │
//! │  └─────────────────────────────────────────────────────┘ │
//! ├──────────────────────────────────────────────────────────┤
//! │                    Index Block                           │
//! │  ┌──────────────┬──────────┬──────────┬───────────────┐ │
//! │  │ first_key    │ offset   │ length   │ block_checksum│ │
//! │  ├──────────────┼──────────┼──────────┼───────────────┤ │
//! │  │ first_key    │ offset   │ length   │ block_checksum│ │
//! │  ├──────────────┼──────────┼──────────┼───────────────┤ │
//! │  │ ...          │ ...      │ ...      │ ...           │ │
//! │  └──────────────┴──────────┴──────────┴───────────────┘ │
//! ├──────────────────────────────────────────────────────────┤
//! │                    Footer (48 bytes)                     │
//! │  ┌──────────┬─────────┬──────────┬──────────┬─────────┐ │
//! │  │ magic(8) │ ver (4) │ meta_off │ idx_off  │ chk(8)  │ │
//! │  │ PLEXUSST │         │   (8)    │   (8)    │ blake3  │ │
//! │  └──────────┴─────────┴──────────┴──────────┴─────────┘ │
//! └──────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

/// Magic bytes at the start of the footer.
pub const MAGIC: &[u8; 8] = b"PLEXUSST";

/// Current SSTable format version.
pub const FORMAT_VERSION: u32 = 1;

/// Footer size in bytes.
pub const FOOTER_SIZE: usize = 48;

/// Default block size (4KB, aligned for O_DIRECT).
pub const DEFAULT_BLOCK_SIZE: usize = 4096;

/// SSTable footer — the last 48 bytes of every SSTable file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Footer {
    /// Magic bytes: "PLEXUSST"
    pub magic: [u8; 8],
    /// Format version
    pub version: u32,
    /// Offset of the meta block (bloom filter)
    pub meta_offset: u64,
    /// Offset of the index block
    pub index_offset: u64,
    /// Total number of entries in the SSTable
    pub entry_count: u64,
    /// Blake3 checksum of the entire file (excluding footer)
    pub checksum: [u8; 8], // First 8 bytes of blake3 hash
}

impl Footer {
    /// Encode footer to exactly FOOTER_SIZE bytes.
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..20].copy_from_slice(&self.meta_offset.to_le_bytes());
        buf[20..28].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[28..36].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[36..44].copy_from_slice(&self.checksum);
        // bytes 44..48 reserved (padding)
        buf
    }

    /// Decode footer from bytes.
    pub fn decode(data: &[u8; FOOTER_SIZE]) -> Result<Self, &'static str> {
        let magic: [u8; 8] = data[0..8].try_into().unwrap();
        if &magic != MAGIC {
            return Err("invalid SSTable magic bytes");
        }

        let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err("unsupported SSTable format version");
        }

        let meta_offset = u64::from_le_bytes(data[12..20].try_into().unwrap());
        let index_offset = u64::from_le_bytes(data[20..28].try_into().unwrap());
        let entry_count = u64::from_le_bytes(data[28..36].try_into().unwrap());
        let checksum: [u8; 8] = data[36..44].try_into().unwrap();

        Ok(Self {
            magic,
            version,
            meta_offset,
            index_offset,
            entry_count,
            checksum,
        })
    }
}

/// An index entry pointing to a data block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    /// First key in the block (for binary search).
    pub first_key: Vec<u8>,
    /// Last key in the block (for range checks).
    pub last_key: Vec<u8>,
    /// Byte offset of the block in the file.
    pub offset: u64,
    /// Compressed size of the block in bytes.
    pub length: u32,
    /// Uncompressed size (for buffer allocation).
    pub uncompressed_length: u32,
    /// xxh3 checksum of the compressed block data.
    pub checksum: u64,
}

impl IndexEntry {
    /// Encode an index entry to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // first_key
        buf.extend_from_slice(&(self.first_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.first_key);

        // last_key
        buf.extend_from_slice(&(self.last_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.last_key);

        // offset, length, uncompressed_length, checksum
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());
        buf.extend_from_slice(&self.uncompressed_length.to_le_bytes());
        buf.extend_from_slice(&self.checksum.to_le_bytes());

        buf
    }

    /// Decode an index entry from bytes. Returns (entry, bytes_consumed).
    pub fn decode(data: &[u8]) -> Result<(Self, usize), &'static str> {
        let mut pos = 0;

        if data.len() < 2 {
            return Err("index entry too short");
        }

        // first_key
        let fk_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + fk_len > data.len() { return Err("first_key overflow"); }
        let first_key = data[pos..pos + fk_len].to_vec();
        pos += fk_len;

        // last_key
        if pos + 2 > data.len() { return Err("missing last_key length"); }
        let lk_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + lk_len > data.len() { return Err("last_key overflow"); }
        let last_key = data[pos..pos + lk_len].to_vec();
        pos += lk_len;

        // Fixed fields: offset(8) + length(4) + uncompressed(4) + checksum(8) = 24
        if pos + 24 > data.len() { return Err("missing fixed fields"); }

        let offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let uncompressed_length = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let checksum = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        Ok((
            IndexEntry {
                first_key,
                last_key,
                offset,
                length,
                uncompressed_length,
                checksum,
            },
            pos,
        ))
    }
}

/// Block compression type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compression {
    None,
    Zstd,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_footer_roundtrip() {
        let footer = Footer {
            magic: *MAGIC,
            version: FORMAT_VERSION,
            meta_offset: 1000,
            index_offset: 2000,
            entry_count: 50000,
            checksum: [1, 2, 3, 4, 5, 6, 7, 8],
        };

        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();

        assert_eq!(decoded.meta_offset, 1000);
        assert_eq!(decoded.index_offset, 2000);
        assert_eq!(decoded.entry_count, 50000);
    }

    #[test]
    fn test_index_entry_roundtrip() {
        let entry = IndexEntry {
            first_key: b"aaa".to_vec(),
            last_key: b"zzz".to_vec(),
            offset: 4096,
            length: 3800,
            uncompressed_length: 4096,
            checksum: 0xDEADBEEF,
        };

        let encoded = entry.encode();
        let (decoded, consumed) = IndexEntry::decode(&encoded).unwrap();

        assert_eq!(decoded.first_key, b"aaa");
        assert_eq!(decoded.last_key, b"zzz");
        assert_eq!(decoded.offset, 4096);
        assert_eq!(consumed, encoded.len());
    }
}

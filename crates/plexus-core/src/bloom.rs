//! Bloom filter for probabilistic key existence checks.
//!
//! Each SSTable has a bloom filter that answers "is this key possibly in this file?"
//! with a configurable false-positive rate (default 1%). This eliminates >99% of
//! unnecessary disk reads on point lookups.

use xxhash_rust::xxh3::xxh3_64;

/// A space-efficient probabilistic data structure.
pub struct BloomFilter {
    /// Bit array.
    bits: Vec<u8>,
    /// Number of bits in the filter.
    num_bits: usize,
    /// Number of hash functions.
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a new bloom filter sized for `expected_items` with `fp_rate` false positive rate.
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        let expected_items = expected_items.max(1);
        let fp_rate = fp_rate.clamp(0.0001, 0.5);

        // Optimal bit count: m = -n * ln(p) / (ln(2))^2
        let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits = (-(expected_items as f64) * fp_rate.ln() / ln2_sq).ceil() as usize;
        let num_bits = num_bits.max(64); // minimum 8 bytes

        // Optimal hash count: k = (m/n) * ln(2)
        let num_hashes =
            ((num_bits as f64 / expected_items as f64) * std::f64::consts::LN_2).ceil() as u32;
        let num_hashes = num_hashes.clamp(1, 30);

        let byte_count = num_bits.div_ceil(8);

        Self {
            bits: vec![0u8; byte_count],
            num_bits,
            num_hashes,
        }
    }

    /// Insert a key into the bloom filter.
    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let bit_index = self.hash(key, i);
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;
            self.bits[byte_index] |= 1 << bit_offset;
        }
    }

    /// Check if a key might be in the set.
    ///
    /// Returns `false` if the key is definitely not present.
    /// Returns `true` if the key might be present (with fp_rate probability of false positive).
    pub fn may_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let bit_index = self.hash(key, i);
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;
            if self.bits[byte_index] & (1 << bit_offset) == 0 {
                return false;
            }
        }
        true
    }

    /// Compute the i-th hash for a key.
    ///
    /// Uses the double-hashing technique: h(key, i) = (h1 + i*h2) % num_bits
    /// where h1 and h2 are derived from xxh3.
    fn hash(&self, key: &[u8], i: u32) -> usize {
        let h = xxh3_64(key);
        let h1 = h as u32 as u64;
        let h2 = (h >> 32) as u32 as u64;
        ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.num_bits as u64) as usize
    }

    /// Serialize the bloom filter to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.bits.len());
        out.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        out.extend_from_slice(&self.num_hashes.to_le_bytes());
        out.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialize a bloom filter from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, &'static str> {
        if data.len() < 12 {
            return Err("bloom filter data too short");
        }

        let num_bits = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let num_hashes = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let byte_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

        if num_hashes == 0 || num_hashes > 30 {
            return Err("bloom filter hash count out of range (1..30)");
        }

        if num_bits == 0 {
            return Err("bloom filter has zero bits");
        }

        // Validate bit/byte consistency
        let expected_bytes = num_bits.div_ceil(8);
        if byte_count != expected_bytes {
            return Err("bloom filter bit/byte count mismatch");
        }

        if data.len() < 12 + byte_count {
            return Err("bloom filter data truncated");
        }

        let bits = data[12..12 + byte_count].to_vec();

        Ok(Self {
            bits,
            num_bits,
            num_hashes,
        })
    }

    /// Approximate memory usage in bytes.
    pub fn size_bytes(&self) -> usize {
        self.bits.len() + std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_check() {
        let mut bf = BloomFilter::new(1000, 0.01);

        bf.insert(b"hello");
        bf.insert(b"world");
        bf.insert(b"plexus");

        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
        assert!(bf.may_contain(b"plexus"));
    }

    #[test]
    fn test_false_negative_rate() {
        // Bloom filters must NEVER produce false negatives
        let mut bf = BloomFilter::new(10000, 0.01);

        let keys: Vec<Vec<u8>> = (0..10000)
            .map(|i| format!("key_{i:06}").into_bytes())
            .collect();

        for key in &keys {
            bf.insert(key);
        }

        for key in &keys {
            assert!(
                bf.may_contain(key),
                "false negative for key: {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn test_false_positive_rate() {
        let mut bf = BloomFilter::new(10000, 0.01);

        for i in 0..10000 {
            bf.insert(format!("key_{i:06}").as_bytes());
        }

        // Check keys that were NOT inserted
        let mut false_positives = 0;
        let test_count = 10000;
        for i in 10000..20000 {
            if bf.may_contain(format!("key_{i:06}").as_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / test_count as f64;
        // Allow up to 3% (should be ~1%)
        assert!(
            fp_rate < 0.03,
            "false positive rate too high: {fp_rate:.3} ({false_positives}/{test_count})"
        );
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"test_key");

        let bytes = bf.to_bytes();
        let bf2 = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(bf2.may_contain(b"test_key"));
        // Verify structural fields survived roundtrip
        assert_eq!(bf.num_bits, bf2.num_bits);
        assert_eq!(bf.num_hashes, bf2.num_hashes);
        assert_eq!(bf.bits.len(), bf2.bits.len());
    }

    #[test]
    fn test_from_bytes_corrupt_data() {
        // Too short
        assert!(BloomFilter::from_bytes(b"short").is_err());

        // Valid header but truncated bit array
        let mut data = vec![0u8; 12];
        // num_bits = 64, num_hashes = 3, byte_count = 8
        data[0..4].copy_from_slice(&64u32.to_le_bytes());
        data[4..8].copy_from_slice(&3u32.to_le_bytes());
        data[8..12].copy_from_slice(&8u32.to_le_bytes());
        // Missing the 8 bytes of bit data
        assert!(BloomFilter::from_bytes(&data).is_err());
    }

    #[test]
    fn test_from_bytes_invalid_hash_count() {
        let mut data = vec![0u8; 20]; // 12 header + 8 bits
                                      // num_bits = 64, num_hashes = 0 (invalid), byte_count = 8
        data[0..4].copy_from_slice(&64u32.to_le_bytes());
        data[4..8].copy_from_slice(&0u32.to_le_bytes()); // zero hashes
        data[8..12].copy_from_slice(&8u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_err());

        // num_hashes = 31 (over limit)
        data[4..8].copy_from_slice(&31u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_err());
    }

    #[test]
    fn test_from_bytes_bit_byte_mismatch() {
        let mut data = vec![0u8; 20]; // 12 header + 8 bits
                                      // num_bits = 64, num_hashes = 3, byte_count = 7 (should be 8 for 64 bits)
        data[0..4].copy_from_slice(&64u32.to_le_bytes());
        data[4..8].copy_from_slice(&3u32.to_le_bytes());
        data[8..12].copy_from_slice(&7u32.to_le_bytes()); // Wrong!
        assert!(BloomFilter::from_bytes(&data).is_err());
    }
}

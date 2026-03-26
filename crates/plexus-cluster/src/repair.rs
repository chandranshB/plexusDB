//! Auto-repair agent — detects and recovers corrupted or missing data.
//!
//! Uses Merkle trees to compare key ranges between replica nodes.
//! When mismatches are found, missing/corrupted blocks are streamed
//! from a healthy replica.

use blake3;
use serde::{Deserialize, Serialize};

/// A node in the Merkle tree over a key range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleNode {
    /// Hash of this subtree.
    pub hash: [u8; 32],
    /// Key range covered [min_key, max_key].
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    /// Number of entries in this subtree.
    pub count: u64,
}

/// Merkle tree for anti-entropy repair.
pub struct MerkleTree {
    /// Leaf nodes (one per SSTable block or key range partition).
    leaves: Vec<MerkleNode>,
    /// Root hash.
    root_hash: [u8; 32],
}

impl MerkleTree {
    /// Build a Merkle tree from a set of key-hash pairs.
    ///
    /// Each entry is (key, blake3_hash_of_value).
    pub fn build(entries: &[(Vec<u8>, [u8; 32])]) -> Self {
        if entries.is_empty() {
            return Self {
                leaves: Vec::new(),
                root_hash: [0u8; 32],
            };
        }

        // Partition entries into ~64 leaves
        let leaf_count = 64.min(entries.len());
        let chunk_size = (entries.len() + leaf_count - 1) / leaf_count;

        let mut leaves = Vec::new();
        for chunk in entries.chunks(chunk_size) {
            let mut hasher = blake3::Hasher::new();
            for (_, hash) in chunk {
                hasher.update(hash);
            }
            let hash = hasher.finalize();

            leaves.push(MerkleNode {
                hash: *hash.as_bytes(),
                min_key: chunk.first().map(|(k, _)| k.clone()).unwrap_or_default(),
                max_key: chunk.last().map(|(k, _)| k.clone()).unwrap_or_default(),
                count: chunk.len() as u64,
            });
        }

        // Compute root hash
        let mut root_hasher = blake3::Hasher::new();
        for leaf in &leaves {
            root_hasher.update(&leaf.hash);
        }
        let root_hash = *root_hasher.finalize().as_bytes();

        Self { leaves, root_hash }
    }

    /// Get the root hash.
    pub fn root_hash(&self) -> &[u8; 32] {
        &self.root_hash
    }

    /// Compare two Merkle trees and return differing key ranges.
    pub fn diff(&self, other: &MerkleTree) -> Vec<DiffRange> {
        if self.root_hash == other.root_hash {
            return Vec::new(); // Trees are identical
        }

        let mut diffs = Vec::new();

        let max_leaves = self.leaves.len().max(other.leaves.len());
        for i in 0..max_leaves {
            let self_leaf = self.leaves.get(i);
            let other_leaf = other.leaves.get(i);

            match (self_leaf, other_leaf) {
                (Some(a), Some(b)) if a.hash != b.hash => {
                    diffs.push(DiffRange {
                        min_key: a.min_key.clone(),
                        max_key: a.max_key.clone(),
                        local_count: a.count,
                        remote_count: b.count,
                    });
                }
                (Some(a), None) => {
                    diffs.push(DiffRange {
                        min_key: a.min_key.clone(),
                        max_key: a.max_key.clone(),
                        local_count: a.count,
                        remote_count: 0,
                    });
                }
                (None, Some(b)) => {
                    diffs.push(DiffRange {
                        min_key: b.min_key.clone(),
                        max_key: b.max_key.clone(),
                        local_count: 0,
                        remote_count: b.count,
                    });
                }
                _ => {}
            }
        }

        tracing::info!(
            diff_ranges = diffs.len(),
            "Merkle tree comparison complete"
        );

        diffs
    }
}

/// A key range where two replicas differ.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffRange {
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub local_count: u64,
    pub remote_count: u64,
}

/// Repair action to take for a diff range.
#[derive(Debug)]
pub enum RepairAction {
    /// Pull data from the remote node for this key range.
    Pull { from_node: String, range: DiffRange },
    /// Push data to the remote node for this key range.
    Push { to_node: String, range: DiffRange },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_value(val: &[u8]) -> [u8; 32] {
        *blake3::hash(val).as_bytes()
    }

    #[test]
    fn test_identical_trees() {
        let entries: Vec<(Vec<u8>, [u8; 32])> = (0..100)
            .map(|i| {
                let key = format!("key_{i:03}").into_bytes();
                let hash = hash_value(format!("val_{i}").as_bytes());
                (key, hash)
            })
            .collect();

        let tree1 = MerkleTree::build(&entries);
        let tree2 = MerkleTree::build(&entries);

        let diffs = tree1.diff(&tree2);
        assert!(diffs.is_empty(), "identical trees should have no diffs");
    }

    #[test]
    fn test_different_trees() {
        let entries1: Vec<(Vec<u8>, [u8; 32])> = (0..100)
            .map(|i| {
                let key = format!("key_{i:03}").into_bytes();
                let hash = hash_value(format!("val_{i}").as_bytes());
                (key, hash)
            })
            .collect();

        let mut entries2 = entries1.clone();
        // Modify one entry
        entries2[50].1 = hash_value(b"modified_value");

        let tree1 = MerkleTree::build(&entries1);
        let tree2 = MerkleTree::build(&entries2);

        let diffs = tree1.diff(&tree2);
        assert!(!diffs.is_empty(), "modified trees should have diffs");
    }
}

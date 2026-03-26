//! Consistent hash ring for data partitioning.
//!
//! Uses virtual nodes to ensure even key distribution across physical nodes.
//! When a node joins or leaves, only ~1/N of keys need to migrate.

use std::collections::BTreeMap;
use xxhash_rust::xxh3::xxh3_64;
use serde::{Deserialize, Serialize};

/// Number of virtual nodes per physical node.
const DEFAULT_VNODES: usize = 256;

/// A node in the hash ring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingNode {
    pub node_id: String,
    pub address: String,
}

/// Consistent hash ring.
pub struct HashRing {
    ring: BTreeMap<u64, RingNode>,
    vnodes_per_node: usize,
    node_count: usize,
}

impl HashRing {
    /// Create a new empty hash ring.
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes_per_node: DEFAULT_VNODES,
            node_count: 0,
        }
    }

    /// Create a hash ring with custom virtual node count.
    pub fn with_vnodes(vnodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes_per_node: vnodes,
            node_count: 0,
        }
    }

    /// Add a node to the ring.
    pub fn add_node(&mut self, node_id: &str, address: &str) {
        for i in 0..self.vnodes_per_node {
            let vnode_key = format!("{node_id}:{i}");
            let hash = xxh3_64(vnode_key.as_bytes());
            self.ring.insert(hash, RingNode {
                node_id: node_id.to_string(),
                address: address.to_string(),
            });
        }
        self.node_count += 1;

        tracing::info!(
            node = node_id,
            vnodes = self.vnodes_per_node,
            total_nodes = self.node_count,
            "added node to hash ring"
        );
    }

    /// Remove a node from the ring.
    pub fn remove_node(&mut self, node_id: &str) {
        self.ring.retain(|_, v| v.node_id != node_id);
        self.node_count = self.node_count.saturating_sub(1);

        tracing::info!(
            node = node_id,
            total_nodes = self.node_count,
            "removed node from hash ring"
        );
    }

    /// Get the primary node responsible for a key.
    pub fn get_node(&self, key: &[u8]) -> Option<&RingNode> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = xxh3_64(key);

        // Walk clockwise from the key's hash position
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node)| node)
    }

    /// Get N replica nodes for a key (distinct physical nodes).
    pub fn get_replicas(&self, key: &[u8], count: usize) -> Vec<&RingNode> {
        if self.ring.is_empty() {
            return Vec::new();
        }

        let hash = xxh3_64(key);
        let mut replicas = Vec::new();
        let mut seen_nodes = std::collections::HashSet::new();

        // Walk clockwise from the key's hash, collecting distinct physical nodes
        let iter = self.ring.range(hash..).chain(self.ring.iter());

        for (_, node) in iter {
            if seen_nodes.insert(&node.node_id) {
                replicas.push(node);
                if replicas.len() >= count {
                    break;
                }
            }
        }

        replicas
    }

    /// Check if the ring contains a specific node.
    pub fn contains(&self, node_id: &str) -> bool {
        self.ring.values().any(|n| n.node_id == node_id)
    }

    /// Number of physical nodes.
    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// Number of virtual nodes (total ring entries).
    pub fn vnode_count(&self) -> usize {
        self.ring.len()
    }

    /// Get all unique physical nodes.
    pub fn nodes(&self) -> Vec<RingNode> {
        let mut seen = std::collections::HashSet::new();
        self.ring
            .values()
            .filter(|n| seen.insert(n.node_id.clone()))
            .cloned()
            .collect()
    }
}

impl Default for HashRing {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_lookup() {
        let mut ring = HashRing::new();
        ring.add_node("node-1", "192.168.1.1:9090");
        ring.add_node("node-2", "192.168.1.2:9090");
        ring.add_node("node-3", "192.168.1.3:9090");

        // Every key should map to some node
        for i in 0..100 {
            let key = format!("key_{i}").into_bytes();
            let node = ring.get_node(&key);
            assert!(node.is_some());
        }
    }

    #[test]
    fn test_replicas_are_distinct() {
        let mut ring = HashRing::new();
        ring.add_node("node-1", "192.168.1.1:9090");
        ring.add_node("node-2", "192.168.1.2:9090");
        ring.add_node("node-3", "192.168.1.3:9090");

        let replicas = ring.get_replicas(b"test_key", 3);
        assert_eq!(replicas.len(), 3);

        // All replicas should be distinct physical nodes
        let ids: std::collections::HashSet<_> = replicas.iter().map(|r| &r.node_id).collect();
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn test_distribution_evenness() {
        let mut ring = HashRing::new();
        ring.add_node("node-1", "1:9090");
        ring.add_node("node-2", "2:9090");
        ring.add_node("node-3", "3:9090");

        let mut counts = std::collections::HashMap::new();
        let total = 10000;

        for i in 0..total {
            let key = format!("key_{i}").into_bytes();
            let node = ring.get_node(&key).unwrap();
            *counts.entry(node.node_id.clone()).or_insert(0) += 1;
        }

        // Each node should get roughly 33% ± 10%
        for (node, count) in &counts {
            let ratio = *count as f64 / total as f64;
            assert!(
                ratio > 0.2 && ratio < 0.5,
                "node {node} got {ratio:.2} of keys (expected ~0.33)"
            );
        }
    }

    #[test]
    fn test_minimal_redistribution() {
        let mut ring = HashRing::new();
        ring.add_node("node-1", "1:9090");
        ring.add_node("node-2", "2:9090");

        // Record assignments
        let keys: Vec<Vec<u8>> = (0..1000).map(|i| format!("k{i}").into_bytes()).collect();
        let before: Vec<String> = keys.iter()
            .map(|k| ring.get_node(k).unwrap().node_id.clone())
            .collect();

        // Add a third node
        ring.add_node("node-3", "3:9090");

        let after: Vec<String> = keys.iter()
            .map(|k| ring.get_node(k).unwrap().node_id.clone())
            .collect();

        // Count how many keys moved
        let moved = before.iter().zip(after.iter()).filter(|(a, b)| a != b).count();
        let move_ratio = moved as f64 / keys.len() as f64;

        // Should be roughly 1/3 (±15%)
        assert!(
            move_ratio < 0.5,
            "too many keys moved: {move_ratio:.2} (expected ~0.33)"
        );
    }
}

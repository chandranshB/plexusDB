//! # plexus-cluster
//!
//! Distributed layer for PlexusDB — consensus, discovery, and repair.

pub mod gossip;
pub mod hash_ring;
pub mod repair;

/// Cluster-level errors.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("gossip error: {0}")]
    Gossip(String),

    #[error("raft error: {0}")]
    Raft(String),

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

//! gRPC service handlers for PlexusDB.
//!
//! Implements the PlexusDB gRPC service defined in plexus.proto.

/// The PlexusDB gRPC service implementation.
///
/// This struct will implement the generated `PlexusDb` trait from tonic-build.
/// For now, it holds references to the core engine components.
pub struct PlexusService {
    node_id: String,
    version: String,
}

impl PlexusService {
    /// Create a new PlexusDB service.
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Get the node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get the version.
    pub fn version(&self) -> &str {
        &self.version
    }
}

//! # plexus-api
//!
//! gRPC service implementation for PlexusDB.

pub mod service;

// The protobuf-generated code will be here after running `cargo build`.
// The build.rs script invokes tonic-build to generate from plexus.proto.
// For now, we conditionally include it if the file exists.
#[cfg(feature = "codegen")]
pub mod proto {
    include!("generated/plexus.v1.rs");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_creation() {
        let svc = service::PlexusService::new("test-node-1".to_string());
        assert_eq!(svc.node_id(), "test-node-1");
        assert!(!svc.version().is_empty());
    }

    #[test]
    fn test_service_version_matches_cargo() {
        let svc = service::PlexusService::new("node".to_string());
        assert_eq!(svc.version(), env!("CARGO_PKG_VERSION"));
    }
}

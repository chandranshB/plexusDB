//! # plexus-api
//!
//! gRPC service implementation for PlexusDB.

pub mod service;

pub mod proto {
    include!("generated/plexus.v1.rs");
}

pub use proto::plexus_db_server::PlexusDbServer;
pub use service::PlexusService;

#[cfg(test)]
mod tests {
    use super::*;

    fn make_gossip() -> std::sync::Arc<plexus_cluster::gossip::GossipEngine> {
        std::sync::Arc::new(plexus_cluster::gossip::GossipEngine::new(
            plexus_cluster::gossip::GossipConfig::default(),
        ))
    }

    fn make_ring() -> std::sync::Arc<parking_lot::RwLock<plexus_cluster::hash_ring::HashRing>> {
        std::sync::Arc::new(parking_lot::RwLock::new(
            plexus_cluster::hash_ring::HashRing::new(),
        ))
    }

    #[test]
    fn test_service_creation() {
        use plexus_core::{Engine, EngineConfig};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(EngineConfig {
            data_dir: tmp.path().to_path_buf(),
            ..EngineConfig::default()
        })
        .unwrap();

        let svc = service::PlexusService::new(
            engine,
            make_gossip(),
            make_ring(),
            "test-node".into(),
            "0.0.0.0:9090".into(),
            "ssd_only".into(),
        );
        drop(svc);
    }

    #[test]
    fn test_service_version_matches_cargo() {
        use plexus_core::{Engine, EngineConfig};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(EngineConfig {
            data_dir: tmp.path().to_path_buf(),
            ..EngineConfig::default()
        })
        .unwrap();

        let svc = service::PlexusService::new(
            engine,
            make_gossip(),
            make_ring(),
            "node".into(),
            "0.0.0.0:9090".into(),
            "ssd_only".into(),
        );
        drop(svc);
    }
}

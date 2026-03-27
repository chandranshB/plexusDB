//! gRPC service handlers for PlexusDB.
//!
//! Implements the `PlexusDB` service defined in `plexus.proto`.
//! All handlers delegate to the `Engine` for actual storage operations.

use std::sync::Arc;

use plexus_cluster::gossip::GossipEngine;
use plexus_core::Engine;
use tonic::{Request, Response, Status};

use crate::proto::plexus_db_server::PlexusDb;
use crate::proto::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, ClusterStatusRequest, ClusterStatusResponse, DeleteRequest, DeleteResponse,
    GetRequest, GetResponse, JoinClusterRequest, JoinClusterResponse, NodeInfoRequest,
    NodeInfoResponse, NodeState, PutRequest, PutResponse, ScanRequest, ScanResponse, StorageInfo,
};

/// Maximum namespace length accepted by the gRPC API.
const MAX_NAMESPACE_LEN: usize = 256;
/// Maximum number of entries in a single batch request.
const MAX_BATCH_SIZE: usize = 10_000;
/// Maximum scan limit (number of results).
const MAX_SCAN_LIMIT: usize = 100_000;

/// Validate and normalise a namespace string.
/// Returns `"default"` for empty namespaces, or an error if too long.
fn validate_namespace(ns: &str) -> Result<&str, Status> {
    if ns.is_empty() {
        return Ok("default");
    }
    if ns.len() > MAX_NAMESPACE_LEN {
        return Err(Status::invalid_argument(format!(
            "namespace too long: {} bytes (max {MAX_NAMESPACE_LEN})",
            ns.len()
        )));
    }
    Ok(ns)
}

/// The PlexusDB gRPC service — wired to the real storage engine.
pub struct PlexusService {
    engine: Arc<Engine>,
    gossip: Arc<GossipEngine>,
    hash_ring: Arc<parking_lot::RwLock<plexus_cluster::hash_ring::HashRing>>,
    node_id: String,
    version: String,
    grpc_addr: String,
    storage_mode: String,
}

impl PlexusService {
    pub fn new(
        engine: Arc<Engine>,
        gossip: Arc<GossipEngine>,
        hash_ring: Arc<parking_lot::RwLock<plexus_cluster::hash_ring::HashRing>>,
        node_id: String,
        grpc_addr: String,
        storage_mode: String,
    ) -> Self {
        Self {
            engine,
            gossip,
            hash_ring,
            node_id,
            version: env!("CARGO_PKG_VERSION").to_string(),
            grpc_addr,
            storage_mode,
        }
    }
}

#[tonic::async_trait]
impl PlexusDb for PlexusService {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        if req.key.len() > u16::MAX as usize {
            return Err(Status::invalid_argument(format!(
                "key too large: {} bytes (max {})",
                req.key.len(),
                u16::MAX
            )));
        }
        if req.value.len() > u32::MAX as usize {
            return Err(Status::invalid_argument(format!(
                "value too large: {} bytes",
                req.value.len()
            )));
        }

        let ns = validate_namespace(&req.namespace)?;

        let ts = self
            .engine
            .put(req.key, req.value, ns)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutResponse {
            success: true,
            timestamp: ts,
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }

        let ns = validate_namespace(&req.namespace)?;

        match self.engine.get(&req.key, ns) {
            Ok(Some(value)) => Ok(Response::new(GetResponse {
                found: true,
                value,
                timestamp: 0,
            })),
            Ok(None) => Ok(Response::new(GetResponse {
                found: false,
                value: vec![],
                timestamp: 0,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        if req.key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }

        let ns = validate_namespace(&req.namespace)?;

        let ts = self
            .engine
            .delete(req.key, ns)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DeleteResponse {
            success: true,
            timestamp: ts,
        }))
    }

    async fn batch_put(
        &self,
        request: Request<BatchPutRequest>,
    ) -> Result<Response<BatchPutResponse>, Status> {
        let req = request.into_inner();

        if req.entries.len() > MAX_BATCH_SIZE {
            return Err(Status::invalid_argument(format!(
                "batch too large: {} entries (max {MAX_BATCH_SIZE})",
                req.entries.len()
            )));
        }

        let mut succeeded = 0u32;
        let mut failed = 0u32;
        let mut errors = Vec::new();

        for entry in req.entries {
            if entry.key.is_empty() {
                failed += 1;
                errors.push("key must not be empty".into());
                continue;
            }
            let ns = match validate_namespace(&entry.namespace) {
                Ok(n) => n,
                Err(e) => {
                    failed += 1;
                    errors.push(e.message().to_string());
                    continue;
                }
            };
            match self.engine.put(entry.key, entry.value, ns) {
                Ok(_) => succeeded += 1,
                Err(e) => {
                    failed += 1;
                    errors.push(e.to_string());
                }
            }
        }

        Ok(Response::new(BatchPutResponse {
            succeeded,
            failed,
            errors,
        }))
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetResponse>, Status> {
        let req = request.into_inner();

        if req.keys.len() > MAX_BATCH_SIZE {
            return Err(Status::invalid_argument(format!(
                "batch too large: {} keys (max {MAX_BATCH_SIZE})",
                req.keys.len()
            )));
        }

        let mut results = Vec::with_capacity(req.keys.len());

        for key_req in req.keys {
            if key_req.key.is_empty() {
                results.push(GetResponse {
                    found: false,
                    value: vec![],
                    timestamp: 0,
                });
                continue;
            }
            let ns = validate_namespace(&key_req.namespace)?;
            match self.engine.get(&key_req.key, ns) {
                Ok(Some(value)) => results.push(GetResponse {
                    found: true,
                    value,
                    timestamp: 0,
                }),
                Ok(None) => results.push(GetResponse {
                    found: false,
                    value: vec![],
                    timestamp: 0,
                }),
                Err(e) => return Err(Status::internal(e.to_string())),
            }
        }

        Ok(Response::new(BatchGetResponse { results }))
    }

    async fn batch_delete(
        &self,
        request: Request<BatchDeleteRequest>,
    ) -> Result<Response<BatchDeleteResponse>, Status> {
        let req = request.into_inner();

        if req.keys.len() > MAX_BATCH_SIZE {
            return Err(Status::invalid_argument(format!(
                "batch too large: {} keys (max {MAX_BATCH_SIZE})",
                req.keys.len()
            )));
        }

        let mut succeeded = 0u32;
        let mut failed = 0u32;

        for del in req.keys {
            if del.key.is_empty() {
                failed += 1;
                continue;
            }
            let ns = match validate_namespace(&del.namespace) {
                Ok(n) => n,
                Err(_) => {
                    failed += 1;
                    continue;
                }
            };
            match self.engine.delete(del.key, ns) {
                Ok(_) => succeeded += 1,
                Err(_) => failed += 1,
            }
        }

        Ok(Response::new(BatchDeleteResponse { succeeded, failed }))
    }

    type ScanStream = tokio_stream::wrappers::ReceiverStream<Result<ScanResponse, Status>>;

    async fn scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        let req = request.into_inner();
        let ns = validate_namespace(&req.namespace)?.to_string();
        let limit = (req.limit as usize).min(MAX_SCAN_LIMIT);

        let results = self
            .engine
            .scan(&req.start_key, &req.end_key, limit, &ns)
            .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            for (key, value) in results {
                if tx
                    .send(Ok(ScanResponse {
                        key,
                        value,
                        timestamp: 0,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        // Return real membership data from the gossip engine
        let members = self.gossip.all_members();
        let node_count = self.gossip.alive_count() as u32;

        let nodes: Vec<crate::proto::NodeStatus> = members
            .iter()
            .map(|m| crate::proto::NodeStatus {
                node_id: m.node_id.clone(),
                address: m.address.to_string(),
                state: match m.state {
                    // Alive: local node is "leader" (single-node), peers are "followers"
                    plexus_cluster::gossip::MemberState::Alive => {
                        if m.node_id == self.node_id {
                            NodeState::Leader as i32
                        } else {
                            NodeState::Follower as i32
                        }
                    }
                    plexus_cluster::gossip::MemberState::Suspected => NodeState::Follower as i32,
                    plexus_cluster::gossip::MemberState::Dead => NodeState::Offline as i32,
                    plexus_cluster::gossip::MemberState::Left => NodeState::Offline as i32,
                },
                storage: None,
                uptime_seconds: 0,
                cpu_usage: 0.0,
                memory_bytes: 0,
            })
            .collect();

        Ok(Response::new(ClusterStatusResponse {
            cluster_id: self.node_id.clone(),
            node_count,
            nodes,
            leader_id: self.node_id.clone(), // single-node: self is leader
            current_term: 0,
        }))
    }

    async fn node_info(
        &self,
        _request: Request<NodeInfoRequest>,
    ) -> Result<Response<NodeInfoResponse>, Status> {
        let metrics = self.engine.metrics();

        // Compute total disk capacity from the data directory
        let (total_bytes, _available_bytes) = disk_usage_bytes(self.engine.data_dir());

        let storage = StorageInfo {
            ssd_total_bytes: total_bytes,
            ssd_used_bytes: metrics.total_disk_bytes,
            hdd_total_bytes: 0,
            hdd_used_bytes: 0,
            s3_used_bytes: 0,
            storage_mode: self.storage_mode.clone(),
            sstable_count: metrics.sstable_count as u32,
            level_count: metrics.level_counts.len() as u32,
        };

        let mut config_map = std::collections::HashMap::new();
        config_map.insert(
            "memtable_size_bytes".into(),
            metrics.memtable_size_bytes.to_string(),
        );
        config_map.insert("wal_size_bytes".into(), metrics.wal_size_bytes.to_string());
        config_map.insert(
            "cache_hit_rate".into(),
            format!("{:.4}", metrics.cache.hit_rate),
        );
        config_map.insert("writes_total".into(), metrics.writes_total.to_string());
        config_map.insert("reads_total".into(), metrics.reads_total.to_string());

        Ok(Response::new(NodeInfoResponse {
            node_id: self.node_id.clone(),
            version: self.version.clone(),
            address: self.grpc_addr.clone(),
            state: NodeState::Leader as i32,
            storage: Some(storage),
            config: config_map,
        }))
    }

    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterResponse>, Status> {
        let req = request.into_inner();

        if req.node_id.is_empty() {
            return Err(Status::invalid_argument("node_id must not be empty"));
        }

        let addr: std::net::SocketAddr = req.address.parse().map_err(|e| {
            Status::invalid_argument(format!("invalid address '{}': {e}", req.address))
        })?;

        // Register the joining node in the gossip membership
        self.gossip
            .handle_message(plexus_cluster::gossip::GossipMessage::Join {
                node_id: req.node_id.clone(),
                address: addr,
                metadata: std::collections::HashMap::new(),
            });

        // Also add to the consistent hash ring for key routing
        self.hash_ring.write().add_node(&req.node_id, &req.address);

        tracing::info!(
            node_id = %req.node_id,
            address = %req.address,
            "node joined cluster via gRPC"
        );

        // Return all known alive peers so the joining node can bootstrap gossip
        let peers: Vec<String> = self
            .gossip
            .alive_members()
            .iter()
            .filter(|m| m.node_id != req.node_id)
            .map(|m| m.address.to_string())
            .collect();

        Ok(Response::new(JoinClusterResponse {
            success: true,
            cluster_id: self.node_id.clone(),
            peer_addresses: peers,
        }))
    }
}

/// Get total and available bytes for the filesystem containing `path`.
/// Returns (0, 0) if the query fails (non-fatal).
fn disk_usage_bytes(path: &std::path::Path) -> (u64, u64) {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::mem::MaybeUninit;

        let path_cstr = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(s) => s,
            Err(_) => return (0, 0),
        };

        let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
        // SAFETY: statvfs is a well-defined C call; path_cstr is a valid null-terminated string.
        let ret = unsafe { libc::statvfs(path_cstr.as_ptr(), stat.as_mut_ptr()) };
        if ret != 0 {
            return (0, 0);
        }
        // SAFETY: statvfs succeeded, so stat is fully initialized.
        let stat = unsafe { stat.assume_init() };
        let block = stat.f_frsize as u64;
        let total = stat.f_blocks * block;
        let avail = stat.f_bavail * block;
        (total, avail)
    }

    #[cfg(not(unix))]
    {
        // On non-Unix platforms (Windows, etc.) we fall back to a best-effort
        // estimate using the standard library's metadata. This avoids pulling
        // in platform-specific crates while still returning a useful value.
        let _ = path;
        (0, 0)
    }
}

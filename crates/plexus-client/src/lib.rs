//! # plexus-client
//!
//! Ergonomic Rust client for PlexusDB.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use plexus_client::PlexusClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), plexus_client::Error> {
//!     let client = PlexusClient::connect("http://127.0.0.1:9090").await?;
//!
//!     // Put a value
//!     client.put("my-key", "hello world").await?;
//!
//!     // Get it back
//!     if let Some(value) = client.get("my-key").await? {
//!         println!("got: {}", String::from_utf8_lossy(&value));
//!     }
//!
//!     // Delete it
//!     client.delete("my-key").await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Namespaces
//!
//! Namespaces provide logical isolation between datasets. Keys in different
//! namespaces never collide, even if they are identical byte sequences.
//!
//! ```rust,no_run
//! use plexus_client::PlexusClient;
//!
//! # async fn example() -> Result<(), plexus_client::Error> {
//! let client = PlexusClient::connect("http://127.0.0.1:9090").await?;
//! let ns = client.namespace("users");
//!
//! ns.put("alice", b"{ \"role\": \"admin\" }").await?;
//! ns.put("bob",   b"{ \"role\": \"viewer\" }").await?;
//!
//! let scan = ns.scan("a".."z", 100).await?;
//! for (key, value) in scan {
//!     println!("{}: {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
//! }
//! # Ok(())
//! # }
//! ```

use plexus_api::proto::plexus_db_client::PlexusDbClient;
use plexus_api::proto::{
    BatchDeleteRequest, BatchGetRequest, BatchPutRequest, DeleteRequest, GetRequest,
    NodeInfoRequest, PutRequest, ScanRequest,
};
use tonic::transport::Channel;

/// Errors returned by the PlexusDB client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to connect to the server.
    #[error("connection failed: {0}")]
    Connect(#[from] tonic::transport::Error),

    /// A gRPC call returned an error status.
    #[error("rpc error ({code}): {message}")]
    Rpc { code: String, message: String },

    /// The server returned an unexpected response.
    #[error("protocol error: {0}")]
    Protocol(String),
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        Error::Rpc {
            code: s.code().description().to_string(),
            message: s.message().to_string(),
        }
    }
}

/// A connected PlexusDB client.
///
/// Cheap to clone — all clones share the same underlying connection pool.
/// Concurrent requests are fully supported without any internal locking.
#[derive(Clone)]
pub struct PlexusClient {
    // PlexusDbClient<Channel> is Clone and uses tonic's internal connection
    // pool — no Mutex needed. Cloning is O(1) and all clones share the pool.
    inner: PlexusDbClient<Channel>,
}

impl PlexusClient {
    /// Connect to a PlexusDB node.
    ///
    /// `endpoint` is a URI such as `"http://127.0.0.1:9090"`.
    ///
    /// The connection is established lazily on the first RPC call.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, Error> {
        let endpoint = endpoint.into();
        tracing::debug!(endpoint, "connecting to PlexusDB");
        let client = PlexusDbClient::connect(endpoint).await?;
        Ok(Self { inner: client })
    }

    /// Return a namespace-scoped view of this client.
    ///
    /// All operations on the returned [`Namespace`] are automatically scoped
    /// to `name`. Use `"default"` for the default namespace.
    pub fn namespace(&self, name: impl Into<String>) -> Namespace {
        Namespace {
            client: self.clone(),
            name: name.into(),
        }
    }

    // ── Core operations (default namespace) ──────────────────────────────────

    /// Store a key-value pair in the default namespace.
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64, Error> {
        self.put_ns(key, value, "default").await
    }

    /// Retrieve a value from the default namespace.
    ///
    /// Returns `None` if the key does not exist or has been deleted.
    pub async fn get(&self, key: impl Into<Vec<u8>>) -> Result<Option<Vec<u8>>, Error> {
        self.get_ns(key, "default").await
    }

    /// Delete a key from the default namespace.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> Result<u64, Error> {
        self.delete_ns(key, "default").await
    }

    /// Scan a key range in the default namespace.
    ///
    /// `range` is any `RangeBounds<impl Into<Vec<u8>>>`.
    /// `limit` caps the number of results (0 = unlimited).
    pub async fn scan(
        &self,
        range: impl ScanRange,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        self.scan_ns(range, limit, "default").await
    }

    // ── Batch operations (default namespace) ─────────────────────────────────

    /// Write multiple key-value pairs atomically (best-effort batch).
    ///
    /// Returns `(succeeded, failed)` counts.
    pub async fn batch_put(
        &self,
        pairs: impl IntoIterator<Item = (impl Into<Vec<u8>>, impl Into<Vec<u8>>)>,
    ) -> Result<(u32, u32), Error> {
        self.batch_put_ns(pairs, "default").await
    }

    /// Read multiple keys in a single round-trip.
    ///
    /// Returns a `Vec` of `Option<Vec<u8>>` in the same order as `keys`.
    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        self.batch_get_ns(keys, "default").await
    }

    /// Delete multiple keys in a single round-trip.
    ///
    /// Returns `(succeeded, failed)` counts.
    pub async fn batch_delete(
        &self,
        keys: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    ) -> Result<(u32, u32), Error> {
        self.batch_delete_ns(keys, "default").await
    }

    // ── Cluster / admin ───────────────────────────────────────────────────────

    /// Fetch live node information and metrics.
    pub async fn node_info(&self) -> Result<NodeInfo, Error> {
        let resp = self
            .inner
            .clone()
            .node_info(NodeInfoRequest {})
            .await?
            .into_inner();

        Ok(NodeInfo {
            node_id: resp.node_id,
            version: resp.version,
            address: resp.address,
            sstable_count: resp.storage.as_ref().map(|s| s.sstable_count).unwrap_or(0),
            disk_used_bytes: resp.storage.as_ref().map(|s| s.ssd_used_bytes).unwrap_or(0),
            storage_mode: resp
                .storage
                .as_ref()
                .map(|s| s.storage_mode.clone())
                .unwrap_or_default(),
            writes_total: resp
                .config
                .get("writes_total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            reads_total: resp
                .config
                .get("reads_total")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            cache_hit_rate: resp
                .config
                .get("cache_hit_rate")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
        })
    }

    // ── Namespaced internals ──────────────────────────────────────────────────

    pub(crate) async fn put_ns(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        namespace: &str,
    ) -> Result<u64, Error> {
        let resp = self
            .inner
            .clone()
            .put(PutRequest {
                key: key.into(),
                value: value.into(),
                namespace: namespace.to_string(),
                ttl_seconds: 0,
            })
            .await?
            .into_inner();
        Ok(resp.timestamp)
    }

    pub(crate) async fn get_ns(
        &self,
        key: impl Into<Vec<u8>>,
        namespace: &str,
    ) -> Result<Option<Vec<u8>>, Error> {
        let resp = self
            .inner
            .clone()
            .get(GetRequest {
                key: key.into(),
                namespace: namespace.to_string(),
                consistency: 0,
            })
            .await?
            .into_inner();
        Ok(if resp.found { Some(resp.value) } else { None })
    }

    pub(crate) async fn delete_ns(
        &self,
        key: impl Into<Vec<u8>>,
        namespace: &str,
    ) -> Result<u64, Error> {
        let resp = self
            .inner
            .clone()
            .delete(DeleteRequest {
                key: key.into(),
                namespace: namespace.to_string(),
            })
            .await?
            .into_inner();
        Ok(resp.timestamp)
    }

    pub(crate) async fn scan_ns(
        &self,
        range: impl ScanRange,
        limit: u32,
        namespace: &str,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let (start, end) = range.into_bounds();
        let mut stream = self
            .inner
            .clone()
            .scan(ScanRequest {
                start_key: start,
                end_key: end,
                limit,
                namespace: namespace.to_string(),
                consistency: 0,
                reverse: false,
            })
            .await?
            .into_inner();

        let mut results = Vec::new();
        while let Some(msg) = stream.message().await? {
            results.push((msg.key, msg.value));
        }
        Ok(results)
    }

    pub(crate) async fn batch_put_ns(
        &self,
        pairs: impl IntoIterator<Item = (impl Into<Vec<u8>>, impl Into<Vec<u8>>)>,
        namespace: &str,
    ) -> Result<(u32, u32), Error> {
        let entries: Vec<_> = pairs
            .into_iter()
            .map(|(k, v)| PutRequest {
                key: k.into(),
                value: v.into(),
                namespace: namespace.to_string(),
                ttl_seconds: 0,
            })
            .collect();
        let resp = self
            .inner
            .clone()
            .batch_put(BatchPutRequest { entries })
            .await?
            .into_inner();
        Ok((resp.succeeded, resp.failed))
    }

    pub(crate) async fn batch_get_ns(
        &self,
        keys: impl IntoIterator<Item = impl Into<Vec<u8>>>,
        namespace: &str,
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let key_reqs: Vec<_> = keys
            .into_iter()
            .map(|k| GetRequest {
                key: k.into(),
                namespace: namespace.to_string(),
                consistency: 0,
            })
            .collect();
        let resp = self
            .inner
            .clone()
            .batch_get(BatchGetRequest { keys: key_reqs })
            .await?
            .into_inner();
        Ok(resp
            .results
            .into_iter()
            .map(|r| if r.found { Some(r.value) } else { None })
            .collect())
    }

    pub(crate) async fn batch_delete_ns(
        &self,
        keys: impl IntoIterator<Item = impl Into<Vec<u8>>>,
        namespace: &str,
    ) -> Result<(u32, u32), Error> {
        let key_reqs: Vec<_> = keys
            .into_iter()
            .map(|k| DeleteRequest {
                key: k.into(),
                namespace: namespace.to_string(),
            })
            .collect();
        let resp = self
            .inner
            .clone()
            .batch_delete(BatchDeleteRequest { keys: key_reqs })
            .await?
            .into_inner();
        Ok((resp.succeeded, resp.failed))
    }
}

// ── Namespace ─────────────────────────────────────────────────────────────────

/// A namespace-scoped view of a [`PlexusClient`].
///
/// All operations are automatically scoped to the namespace this was created with.
#[derive(Clone)]
pub struct Namespace {
    client: PlexusClient,
    name: String,
}

impl Namespace {
    /// Store a key-value pair in this namespace.
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64, Error> {
        self.client.put_ns(key, value, &self.name).await
    }

    /// Retrieve a value from this namespace.
    pub async fn get(&self, key: impl Into<Vec<u8>>) -> Result<Option<Vec<u8>>, Error> {
        self.client.get_ns(key, &self.name).await
    }

    /// Delete a key from this namespace.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> Result<u64, Error> {
        self.client.delete_ns(key, &self.name).await
    }

    /// Scan a key range in this namespace.
    pub async fn scan(
        &self,
        range: impl ScanRange,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        self.client.scan_ns(range, limit, &self.name).await
    }

    /// Write multiple key-value pairs in this namespace.
    pub async fn batch_put(
        &self,
        pairs: impl IntoIterator<Item = (impl Into<Vec<u8>>, impl Into<Vec<u8>>)>,
    ) -> Result<(u32, u32), Error> {
        self.client.batch_put_ns(pairs, &self.name).await
    }

    /// Read multiple keys from this namespace.
    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        self.client.batch_get_ns(keys, &self.name).await
    }

    /// Delete multiple keys from this namespace.
    pub async fn batch_delete(
        &self,
        keys: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    ) -> Result<(u32, u32), Error> {
        self.client.batch_delete_ns(keys, &self.name).await
    }

    /// Return the namespace name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

// ── NodeInfo ──────────────────────────────────────────────────────────────────

/// Live node information returned by [`PlexusClient::node_info`].
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: String,
    pub version: String,
    pub address: String,
    pub sstable_count: u32,
    pub disk_used_bytes: u64,
    pub storage_mode: String,
    pub writes_total: u64,
    pub reads_total: u64,
    pub cache_hit_rate: f64,
}

// ── ScanRange ─────────────────────────────────────────────────────────────────

/// Anything that can be used as a scan range.
///
/// Implemented for:
/// - `..` (full scan)
/// - `"start"..` (from start, unbounded end)
/// - `"start".."end"` (bounded range)
/// - `("start", "end")` tuple
pub trait ScanRange {
    fn into_bounds(self) -> (Vec<u8>, Vec<u8>);
}

impl ScanRange for std::ops::RangeFull {
    fn into_bounds(self) -> (Vec<u8>, Vec<u8>) {
        (vec![], vec![])
    }
}

impl<T: Into<Vec<u8>>> ScanRange for std::ops::RangeFrom<T> {
    fn into_bounds(self) -> (Vec<u8>, Vec<u8>) {
        (self.start.into(), vec![])
    }
}

impl<T: Into<Vec<u8>>> ScanRange for std::ops::Range<T> {
    fn into_bounds(self) -> (Vec<u8>, Vec<u8>) {
        (self.start.into(), self.end.into())
    }
}

impl<T: Into<Vec<u8>>> ScanRange for std::ops::RangeInclusive<T> {
    fn into_bounds(self) -> (Vec<u8>, Vec<u8>) {
        let (start, end) = self.into_inner();
        let mut end: Vec<u8> = end.into();
        // Make inclusive by appending a zero byte (lexicographic successor)
        end.push(0);
        (start.into(), end)
    }
}

impl<S, E> ScanRange for (S, E)
where
    S: Into<Vec<u8>>,
    E: Into<Vec<u8>>,
{
    fn into_bounds(self) -> (Vec<u8>, Vec<u8>) {
        (self.0.into(), self.1.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_range_full() {
        let (s, e) = (..).into_bounds();
        assert!(s.is_empty());
        assert!(e.is_empty());
    }

    #[test]
    fn test_scan_range_from() {
        let (s, e) = ("abc".to_string()..).into_bounds();
        assert_eq!(s, b"abc");
        assert!(e.is_empty());
    }

    #[test]
    fn test_scan_range_bounded() {
        let (s, e) = ("a".to_string().."z".to_string()).into_bounds();
        assert_eq!(s, b"a");
        assert_eq!(e, b"z");
    }

    #[test]
    fn test_scan_range_inclusive() {
        let (s, e) = ("a".to_string()..="z".to_string()).into_bounds();
        assert_eq!(s, b"a");
        // inclusive end has a zero byte appended
        assert_eq!(e, b"z\x00");
    }

    #[test]
    fn test_scan_range_tuple() {
        let (s, e) = (b"start".to_vec(), b"end".to_vec()).into_bounds();
        assert_eq!(s, b"start");
        assert_eq!(e, b"end");
    }

    #[test]
    fn test_error_from_status() {
        let status = tonic::Status::not_found("key not found");
        let err = Error::from(status);
        assert!(matches!(err, Error::Rpc { .. }));
        assert!(err.to_string().contains("key not found"));
    }
}

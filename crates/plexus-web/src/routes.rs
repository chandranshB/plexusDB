//! Axum routes for the Web UI.

use axum::{
    extract::State,
    http::{header, StatusCode, Uri},
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use serde_json::json;
use std::sync::Arc;

use crate::StaticAssets;

/// Shared state for the web UI.
pub struct WebState {
    pub node_id: String,
    pub version: String,
    pub start_time: std::time::Instant,
    pub storage_mode: String,
    /// Live engine metrics — None if engine not yet started.
    pub engine: Option<Arc<plexus_core::Engine>>,
}

/// Build the web UI router.
pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_handler))
        .route("/api/health", get(health_handler))
        .route("/api/metrics", get(metrics_handler))
        .route("/metrics", get(prometheus_handler))
        .fallback(static_handler)
        .with_state(state)
}

async fn index_handler() -> impl IntoResponse {
    match StaticAssets::get("index.html") {
        Some(content) => Html(
            std::str::from_utf8(content.data.as_ref())
                .unwrap_or("<h1>PlexusDB</h1>")
                .to_string(),
        )
        .into_response(),
        None => Html("<h1>PlexusDB Dashboard</h1><p>Static assets not found.</p>".to_string())
            .into_response(),
    }
}

async fn status_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let uptime = state.start_time.elapsed().as_secs();
    Json(json!({
        "node_id": state.node_id,
        "version": state.version,
        "uptime_seconds": uptime,
        "status": "running",
        "storage_mode": state.storage_mode,
    }))
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

/// Real-time engine metrics endpoint.
async fn metrics_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    match &state.engine {
        None => Json(json!({ "error": "engine not started" })),
        Some(engine) => {
            let m = engine.metrics();
            let levels: Vec<_> = m
                .level_counts
                .iter()
                .map(|(l, c)| json!({ "level": l, "sstables": c }))
                .collect();

            Json(json!({
                "writes_total": m.writes_total,
                "reads_total": m.reads_total,
                "deletes_total": m.deletes_total,
                "memtable": {
                    "size_bytes": m.memtable_size_bytes,
                    "entries": m.memtable_entries,
                },
                "wal_size_bytes": m.wal_size_bytes,
                "sstables": {
                    "total": m.sstable_count,
                    "total_disk_bytes": m.total_disk_bytes,
                    "levels": levels,
                },
                "cache": {
                    "max_bytes": m.cache.max_size,
                    "used_bytes": m.cache.current_size,
                    "entries": m.cache.entry_count,
                    "hits": m.cache.hits,
                    "misses": m.cache.misses,
                    "hit_rate": m.cache.hit_rate,
                }
            }))
        }
    }
}
/// Prometheus text format metrics endpoint (`/metrics`).
///
/// Compatible with any Prometheus scraper or Grafana agent.
async fn prometheus_handler(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    let mut out = String::with_capacity(1024);

    let uptime = state.start_time.elapsed().as_secs();
    out.push_str(&format!(
        "# HELP plexus_uptime_seconds Node uptime in seconds\n\
         # TYPE plexus_uptime_seconds gauge\n\
         plexus_uptime_seconds{{node=\"{}\",version=\"{}\"}} {}\n",
        state.node_id, state.version, uptime
    ));

    if let Some(engine) = &state.engine {
        let m = engine.metrics();

        out.push_str(&format!(
            "# HELP plexus_writes_total Total write operations\n\
             # TYPE plexus_writes_total counter\n\
             plexus_writes_total{{node=\"{}\"}} {}\n\
             # HELP plexus_reads_total Total read operations\n\
             # TYPE plexus_reads_total counter\n\
             plexus_reads_total{{node=\"{}\"}} {}\n\
             # HELP plexus_deletes_total Total delete operations\n\
             # TYPE plexus_deletes_total counter\n\
             plexus_deletes_total{{node=\"{}\"}} {}\n",
            state.node_id,
            m.writes_total,
            state.node_id,
            m.reads_total,
            state.node_id,
            m.deletes_total,
        ));

        out.push_str(&format!(
            "# HELP plexus_memtable_size_bytes Active MemTable size in bytes\n\
             # TYPE plexus_memtable_size_bytes gauge\n\
             plexus_memtable_size_bytes{{node=\"{}\"}} {}\n\
             # HELP plexus_memtable_entries Active MemTable entry count\n\
             # TYPE plexus_memtable_entries gauge\n\
             plexus_memtable_entries{{node=\"{}\"}} {}\n",
            state.node_id, m.memtable_size_bytes, state.node_id, m.memtable_entries,
        ));

        out.push_str(&format!(
            "# HELP plexus_wal_size_bytes Current WAL file size in bytes\n\
             # TYPE plexus_wal_size_bytes gauge\n\
             plexus_wal_size_bytes{{node=\"{}\"}} {}\n\
             # HELP plexus_sstable_count Total number of active SSTables\n\
             # TYPE plexus_sstable_count gauge\n\
             plexus_sstable_count{{node=\"{}\"}} {}\n\
             # HELP plexus_disk_bytes_total Total bytes used on disk\n\
             # TYPE plexus_disk_bytes_total gauge\n\
             plexus_disk_bytes_total{{node=\"{}\"}} {}\n",
            state.node_id,
            m.wal_size_bytes,
            state.node_id,
            m.sstable_count,
            state.node_id,
            m.total_disk_bytes,
        ));

        out.push_str(&format!(
            "# HELP plexus_cache_hits_total Block cache hit count\n\
             # TYPE plexus_cache_hits_total counter\n\
             plexus_cache_hits_total{{node=\"{}\"}} {}\n\
             # HELP plexus_cache_misses_total Block cache miss count\n\
             # TYPE plexus_cache_misses_total counter\n\
             plexus_cache_misses_total{{node=\"{}\"}} {}\n\
             # HELP plexus_cache_hit_rate Block cache hit rate (0.0-1.0)\n\
             # TYPE plexus_cache_hit_rate gauge\n\
             plexus_cache_hit_rate{{node=\"{}\"}} {:.4}\n\
             # HELP plexus_cache_used_bytes Block cache bytes currently used\n\
             # TYPE plexus_cache_used_bytes gauge\n\
             plexus_cache_used_bytes{{node=\"{}\"}} {}\n",
            state.node_id,
            m.cache.hits,
            state.node_id,
            m.cache.misses,
            state.node_id,
            m.cache.hit_rate,
            state.node_id,
            m.cache.current_size,
        ));

        for (level, count) in &m.level_counts {
            out.push_str(&format!(
                "plexus_sstable_level_count{{node=\"{}\",level=\"L{}\"}} {}\n",
                state.node_id, level, count
            ));
        }
    }

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    match StaticAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, mime.as_ref())],
                content.data.to_vec(),
            )
                .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    fn make_state() -> Arc<WebState> {
        Arc::new(WebState {
            node_id: "test-node".to_string(),
            version: "0.1.0".to_string(),
            start_time: std::time::Instant::now(),
            storage_mode: "ssd_only".to_string(),
            engine: None,
        })
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = router(make_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_status_endpoint() {
        let app = router(make_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["node_id"], "test-node");
        assert_eq!(json["status"], "running");
    }

    #[tokio::test]
    async fn test_metrics_endpoint_no_engine() {
        let app = router(make_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"].is_string());
    }

    #[tokio::test]
    async fn test_metrics_endpoint_with_engine() {
        use plexus_core::{Engine, EngineConfig};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let engine = Engine::open(EngineConfig {
            data_dir: tmp.path().to_path_buf(),
            ..EngineConfig::default()
        })
        .unwrap();
        engine.put(b"k".to_vec(), b"v".to_vec(), "default").unwrap();

        let state = Arc::new(WebState {
            node_id: "test".into(),
            version: "0.1.0".into(),
            start_time: std::time::Instant::now(),
            storage_mode: "ssd_only".to_string(),
            engine: Some(engine),
        });

        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["writes_total"], 1);
    }

    #[tokio::test]
    async fn test_index_endpoint() {
        let app = router(make_state());
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_unknown_static_returns_404() {
        let app = router(make_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/nonexistent-file.xyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

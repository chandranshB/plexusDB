//! Axum routes for the Web UI.

use axum::{
    Router,
    extract::State,
    http::{header, StatusCode, Uri},
    response::{Html, IntoResponse, Json},
    routing::get,
};
use serde_json::json;
use std::sync::Arc;

use crate::StaticAssets;

/// Shared state for the web UI.
pub struct WebState {
    pub node_id: String,
    pub version: String,
    pub start_time: std::time::Instant,
}

/// Build the web UI router.
pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_handler))
        .route("/api/health", get(health_handler))
        .fallback(static_handler)
        .with_state(state)
}

/// Serve the main index page.
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

/// API: cluster status.
async fn status_handler(State(state): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let uptime = state.start_time.elapsed().as_secs();
    Json(json!({
        "node_id": state.node_id,
        "version": state.version,
        "uptime_seconds": uptime,
        "status": "running"
    }))
}

/// API: health check.
async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

/// Serve embedded static files.
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

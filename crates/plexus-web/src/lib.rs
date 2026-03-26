//! # plexus-web
//!
//! Embedded Web UI for PlexusDB monitoring and administration.
//! Static assets are compiled into the binary via `rust-embed`.

pub mod routes;

use rust_embed::Embed;

/// Embedded static assets (HTML, CSS, JS).
#[derive(Embed)]
#[folder = "static/"]
pub struct StaticAssets;

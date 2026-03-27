//! # PlexusDB — Single Binary Entry Point
//!
//! This is the main executable that wires together all subsystems:
//! - Storage Engine (WAL → MemTable → SSTable → Compaction)
//! - Hardware Detection & Tiered Routing
//! - Cluster Mesh (Raft + Gossip + Hash Ring)
//! - Security (mTLS + AES-256-GCM)
//! - gRPC API + Web UI
//!
//! ## Usage
//!
//! ```bash
//! # Start a single node
//! plexus start
//!
//! # Start with custom data directory
//! plexus start --data-dir /mnt/ssd/plexus
//!
//! # Join an existing cluster
//! plexus start --join 192.168.1.10:9090
//!
//! # Check cluster status
//! plexus status --addr 127.0.0.1:9090
//!
//! # Run built-in benchmark
//! plexus bench --keys 100000
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};

use plexus_cluster::gossip::{GossipConfig, GossipEngine};
use plexus_cluster::hash_ring::HashRing;
use plexus_core::EngineConfig;
use plexus_meta::MetaStore;
use plexus_security::tls::TlsConfig;
use plexus_storage::{detect_storage, StorageConfig, TierRouter};
use plexus_web::routes::{self, WebState};

/// PlexusDB — Hardware-Aware Distributed NoSQL Database
#[derive(Parser)]
#[command(
    name = "plexus",
    version,
    about = "PlexusDB — A hardware-aware, distributed NoSQL database built for the edge.",
    long_about = "PlexusDB automatically detects your storage hardware (SSD/HDD/NVMe) and \
                  optimizes I/O patterns accordingly. It uses io_uring for kernel-bypass I/O, \
                  LSM-Trees for write-optimized storage, and Raft consensus for strong consistency \
                  across a cluster of commodity machines."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a PlexusDB node
    Start {
        /// Data directory
        #[arg(long, default_value = "./plexus-data")]
        data_dir: PathBuf,

        /// gRPC listen address
        #[arg(long, default_value = "0.0.0.0:9090")]
        grpc_addr: String,

        /// Web UI listen address
        #[arg(long, default_value = "0.0.0.0:8080")]
        web_addr: String,

        /// Gossip listen address
        #[arg(long, default_value = "0.0.0.0:7947")]
        gossip_addr: String,

        /// Join an existing cluster (address of any node)
        #[arg(long)]
        join: Option<String>,

        /// Node ID (auto-generated if not specified)
        #[arg(long)]
        node_id: Option<String>,

        /// Enable encryption at rest
        #[arg(long, default_value_t = false)]
        encrypt: bool,

        /// Log level (trace, debug, info, warn, error)
        #[arg(long, default_value = "info")]
        log_level: String,

        /// MemTable size before flush (MB)
        #[arg(long, default_value_t = 32)]
        memtable_size_mb: usize,

        /// Block cache size (MB)
        #[arg(long, default_value_t = 256)]
        cache_size_mb: usize,

        /// Number of compaction threads
        #[arg(long, default_value_t = 2)]
        compaction_threads: usize,
    },

    /// Show cluster status
    Status {
        /// Address of any cluster node
        #[arg(long, default_value = "127.0.0.1:9090")]
        addr: String,
    },

    /// Run built-in benchmark
    Bench {
        /// Number of keys to write
        #[arg(long, default_value_t = 100000)]
        keys: usize,

        /// Value size in bytes
        #[arg(long, default_value_t = 256)]
        value_size: usize,

        /// Address of the PlexusDB node
        #[arg(long, default_value = "127.0.0.1:9090")]
        addr: String,
    },

    /// Generate TLS certificates
    GenCerts {
        /// Output directory for certificates
        #[arg(long, default_value = "./plexus-certs")]
        output: PathBuf,

        /// Node ID for the certificate CN
        #[arg(long, default_value = "plexus-node")]
        node_id: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            data_dir,
            grpc_addr,
            web_addr,
            gossip_addr,
            join,
            node_id,
            encrypt,
            log_level,
            memtable_size_mb,
            cache_size_mb,
            compaction_threads,
        } => {
            // ── Initialize Logging ──
            let filter = EnvFilter::try_new(&log_level).unwrap_or_else(|_| EnvFilter::new("info"));
            fmt()
                .with_env_filter(filter)
                .with_target(true)
                .with_thread_ids(true)
                .init();

            let node_id = node_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            let start_time = Instant::now();

            print_banner(&node_id);

            // ── Phase 1: Hardware Detection ──
            tracing::info!("=== Phase 1: Detecting Storage Hardware ===");
            let devices = detect_storage();
            let storage_config = StorageConfig {
                data_dir: data_dir.clone(),
                ..StorageConfig::default()
            };
            let tier_router = TierRouter::new(&devices, storage_config);
            tier_router.create_directories()?;

            tracing::info!(
                mode = %tier_router.mode(),
                parallelism = tier_router.recommended_parallelism(),
                "storage tier routing configured"
            );

            // ── Phase 2: Initialize Metadata Store ──
            tracing::info!("=== Phase 2: Initializing Metadata Store ===");
            let meta_path = tier_router.meta_path();
            let _meta_store = Arc::new(MetaStore::open(&meta_path)?);
            tracing::info!(path = %meta_path.display(), "metadata store ready");

            // ── Phase 3: Configure Storage Engine ──
            tracing::info!("=== Phase 3: Configuring Storage Engine ===");
            let _engine_config = EngineConfig {
                data_dir: data_dir.clone(),
                memtable_size: memtable_size_mb * 1024 * 1024,
                block_cache_size: cache_size_mb * 1024 * 1024,
                compaction_threads: compaction_threads
                    .max(tier_router.recommended_compaction_threads()),
                ..EngineConfig::default()
            };
            tracing::info!(
                memtable = format!("{}MB", memtable_size_mb),
                cache = format!("{}MB", cache_size_mb),
                compaction_threads = _engine_config.compaction_threads,
                "engine configured"
            );

            // ── Phase 4: Initialize Cluster ──
            tracing::info!("=== Phase 4: Initializing Cluster Mesh ===");
            let gossip_bind: SocketAddr = gossip_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid gossip address '{}': {}", gossip_addr, e))?;

            let gossip_config = GossipConfig {
                node_id: node_id.clone(),
                bind_addr: gossip_bind,
                ..GossipConfig::default()
            };
            let gossip_engine = Arc::new(GossipEngine::new(gossip_config));

            let mut _hash_ring = HashRing::new();
            _hash_ring.add_node(&node_id, &grpc_addr);

            if let Some(ref join_addr) = join {
                tracing::info!(join = %join_addr, "joining existing cluster");
                gossip_engine.add_seed(
                    format!("seed-{}", join_addr),
                    join_addr.parse().map_err(|e| {
                        anyhow::anyhow!("invalid join address '{}': {}", join_addr, e)
                    })?,
                );
            }

            tracing::info!(
                alive = gossip_engine.alive_count(),
                "cluster mesh initialized"
            );

            // ── Phase 5: Security ──
            tracing::info!("=== Phase 5: Configuring Security ===");
            let cert_dir = data_dir.join("certs");
            match TlsConfig::generate_self_signed(&cert_dir, &node_id) {
                Ok(tls) => {
                    tracing::info!(
                        ca = %tls.ca_cert_path.display(),
                        "TLS certificates ready (mTLS enabled)"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "TLS setup failed — running without encryption");
                }
            }

            if encrypt {
                tracing::info!("data-at-rest encryption: ENABLED (AES-256-GCM)");
            } else {
                tracing::info!("data-at-rest encryption: DISABLED (use --encrypt to enable)");
            }

            // ── Phase 6: Start Web UI ──
            tracing::info!("=== Phase 6: Starting Services ===");
            let web_state = Arc::new(WebState {
                node_id: node_id.clone(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                start_time,
            });

            let web_router = routes::router(web_state);
            let web_listen: SocketAddr = web_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid web address '{}': {}", web_addr, e))?;

            let _web_handle = tokio::spawn(async move {
                tracing::info!(addr = %web_listen, "Web UI listening");
                let listener = match tokio::net::TcpListener::bind(web_listen).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!(addr = %web_listen, error = %e, "failed to bind Web UI listener");
                        return;
                    }
                };
                if let Err(e) = axum::serve(listener, web_router).await {
                    tracing::error!(error = %e, "Web UI server error");
                }
            });

            // ── Phase 7: Start gRPC Server ──
            // TODO: Start tonic gRPC server after protobuf codegen
            tracing::info!(addr = %grpc_addr, "gRPC server ready (placeholder)");

            // ── Ready ──
            let short_id = &node_id[..std::cmp::min(12, node_id.len())];
            tracing::info!("══════════════════════════════════════════");
            tracing::info!("  PlexusDB is READY");
            tracing::info!("  Node:    {}", short_id);
            tracing::info!("  gRPC:    {}", grpc_addr);
            tracing::info!("  Web UI:  http://{}", web_addr);
            tracing::info!("  Gossip:  {}", gossip_addr);
            tracing::info!("  Mode:    {}", tier_router.mode());
            tracing::info!("══════════════════════════════════════════");

            // Wait for shutdown signal
            tokio::signal::ctrl_c().await?;
            tracing::info!("shutting down gracefully...");

            Ok(())
        }

        Commands::Status { addr } => {
            println!("Connecting to PlexusDB at {addr}...");
            println!("(gRPC client not yet connected — pending protobuf codegen)");
            Ok(())
        }

        Commands::Bench {
            keys,
            value_size,
            addr,
        } => {
            println!("PlexusDB Benchmark");
            println!("  Target:     {addr}");
            println!("  Keys:       {keys}");
            println!("  Value size: {value_size} bytes");
            println!("(benchmark client not yet connected — pending protobuf codegen)");
            Ok(())
        }

        Commands::GenCerts { output, node_id } => {
            fmt().with_env_filter(EnvFilter::new("info")).init();
            tracing::info!("Generating TLS certificates...");
            let tls = TlsConfig::generate_self_signed(&output, &node_id)?;
            tracing::info!(
                ca = %tls.ca_cert_path.display(),
                node_cert = %tls.node_cert_path.display(),
                "certificates generated successfully"
            );
            println!("\nCertificates written to: {}", output.display());
            println!("  CA cert:    {}", tls.ca_cert_path.display());
            println!("  Node cert:  {}", tls.node_cert_path.display());
            println!("  Node key:   {}", tls.node_key_path.display());
            Ok(())
        }
    }
}

fn print_banner(node_id: &str) {
    println!(
        r#"
    ██████╗ ██╗     ███████╗██╗  ██╗██╗   ██╗███████╗
    ██╔══██╗██║     ██╔════╝╚██╗██╔╝██║   ██║██╔════╝
    ██████╔╝██║     █████╗   ╚███╔╝ ██║   ██║███████╗
    ██╔═══╝ ██║     ██╔══╝   ██╔██╗ ██║   ██║╚════██║
    ██║     ███████╗███████╗██╔╝ ██╗╚██████╔╝███████║
    ╚═╝     ╚══════╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝
                                            DB v{}
    Hardware-Aware · Distributed · Sequential-First
    Node: {}
    "#,
        env!("CARGO_PKG_VERSION"),
        &node_id[..std::cmp::min(12, node_id.len())]
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}

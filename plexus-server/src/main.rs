//! # PlexusDB — Single Binary Entry Point

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, Subcommand};
use parking_lot::RwLock as ParkingRwLock;
use tracing_subscriber::{fmt, EnvFilter};

use plexus_api::{PlexusDbServer, PlexusService};
use plexus_cluster::gossip::{GossipConfig, GossipEngine};
use plexus_cluster::hash_ring::HashRing;
use plexus_core::{Engine, EngineConfig};
use plexus_security::tls::TlsConfig;
use plexus_storage::{detect_storage, StorageConfig, TierRouter};
use plexus_web::routes::{self, BenchState, WebState};
use plexus_client;

/// PlexusDB — Hardware-Aware Distributed NoSQL Database
#[derive(Parser)]
#[command(
    name = "plexus",
    version,
    about = "PlexusDB — A hardware-aware, distributed NoSQL database.",
    long_about = "PlexusDB automatically detects your storage hardware (SSD/HDD/NVMe) and \
                  optimizes I/O patterns accordingly."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a PlexusDB node
    Start {
        #[arg(long, default_value = "./plexus-data")]
        data_dir: PathBuf,
        #[arg(long, default_value = "0.0.0.0:9090")]
        grpc_addr: String,
        #[arg(long, default_value = "0.0.0.0:8080")]
        web_addr: String,
        #[arg(long, default_value = "0.0.0.0:7947")]
        gossip_addr: String,
        #[arg(long)]
        join: Option<String>,
        #[arg(long)]
        node_id: Option<String>,
        #[arg(long, default_value_t = false)]
        encrypt: bool,
        #[arg(long, default_value = "info")]
        log_level: String,
        #[arg(long, default_value_t = 32)]
        memtable_size_mb: usize,
        #[arg(long, default_value_t = 256)]
        cache_size_mb: usize,
        #[arg(long, default_value_t = 2)]
        compaction_threads: usize,
    },

    /// Show cluster status via gRPC
    Status {
        #[arg(long, default_value = "127.0.0.1:9090")]
        addr: String,
    },

    /// Run built-in write/read benchmark
    Bench {
        #[arg(long, default_value_t = 100_000)]
        keys: usize,
        #[arg(long, default_value_t = 256)]
        value_size: usize,
        /// Benchmark against a local data directory (no gRPC needed)
        #[arg(long, default_value = "./plexus-bench")]
        data_dir: PathBuf,
    },

    /// Run a live benchmark against a running PlexusDB node (visible at localhost:8080)
    BenchLive {
        /// gRPC endpoint of the running node
        #[arg(long, default_value = "http://127.0.0.1:9090")]
        grpc_addr: String,
        /// Web UI address to serve live metrics on
        #[arg(long, default_value = "0.0.0.0:8081")]
        web_addr: String,
        /// Number of keys to write then read
        #[arg(long, default_value_t = 100_000)]
        keys: usize,
        /// Value size in bytes
        #[arg(long, default_value_t = 256)]
        value_size: usize,
        /// Number of concurrent writer tasks
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
    },

    /// Generate TLS certificates
    GenCerts {
        #[arg(long, default_value = "./plexus-certs")]
        output: PathBuf,
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
            let filter = EnvFilter::try_new(&log_level).unwrap_or_else(|_| EnvFilter::new("info"));
            fmt()
                .with_env_filter(filter)
                .with_target(true)
                .with_thread_ids(true)
                .init();

            let node_id = node_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let start_time = Instant::now();
            print_banner(&node_id);

            // ── Phase 1: Hardware Detection ──────────────────────────────────
            tracing::info!("=== Phase 1: Detecting Storage Hardware ===");
            let devices = detect_storage();
            // Read S3 config from environment variables (documented in README)
            let s3_bucket = std::env::var("PLEXUS_S3_BUCKET").ok();
            let s3_endpoint = std::env::var("PLEXUS_S3_ENDPOINT").ok();
            let s3_enabled = s3_bucket.is_some();
            if s3_enabled {
                tracing::info!(bucket = ?s3_bucket, endpoint = ?s3_endpoint, "S3 cold-tier enabled");
            }
            let storage_config = StorageConfig {
                data_dir: data_dir.clone(),
                s3_bucket,
                s3_endpoint,
                s3_enabled,
                ..StorageConfig::default()
            };
            let tier_router = TierRouter::new(&devices, storage_config);
            tier_router.create_directories()?;
            tracing::info!(mode = %tier_router.mode(), "storage tier routing configured");

            // ── Phase 2: Open Storage Engine ─────────────────────────────────
            tracing::info!("=== Phase 2: Opening Storage Engine ===");
            let engine_config = EngineConfig {
                data_dir: data_dir.clone(),
                memtable_size: memtable_size_mb * 1024 * 1024,
                block_cache_size: cache_size_mb * 1024 * 1024,
                compaction_threads: compaction_threads
                    .max(tier_router.recommended_compaction_threads()),
                ..EngineConfig::default()
            };
            let engine = Engine::open(engine_config)?;
            tracing::info!(
                memtable_mb = memtable_size_mb,
                cache_mb = cache_size_mb,
                "storage engine ready"
            );

            // ── Phase 3: Initialize Cluster ──────────────────────────────────
            tracing::info!("=== Phase 3: Initializing Cluster Mesh ===");
            let gossip_bind: SocketAddr = gossip_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid gossip address '{}': {}", gossip_addr, e))?;
            let gossip_config = GossipConfig {
                node_id: node_id.clone(),
                bind_addr: gossip_bind,
                ..GossipConfig::default()
            };
            let gossip_engine = Arc::new(GossipEngine::new(gossip_config));
            let mut hash_ring = HashRing::new();
            hash_ring.add_node(&node_id, &grpc_addr);
            let hash_ring = Arc::new(ParkingRwLock::new(hash_ring));

            // Start the real UDP gossip transport
            Arc::clone(&gossip_engine)
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("gossip start failed: {e}"))?;

            if let Some(ref join_addr) = join {
                let seed: SocketAddr = join_addr
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid join address '{}': {}", join_addr, e))?;
                tracing::info!(join = %join_addr, "joining existing cluster via gossip");
                // Add seed to local membership view
                gossip_engine.add_seed(format!("seed-{join_addr}"), seed);
                // Send a Join datagram so the seed node learns about us
                gossip_engine
                    .join_via_udp(seed)
                    .await
                    .map_err(|e| anyhow::anyhow!("gossip join failed: {e}"))?;
            }
            tracing::info!(
                alive = gossip_engine.alive_count(),
                "cluster mesh initialized"
            );

            // ── Phase 4: Security ────────────────────────────────────────────
            tracing::info!("=== Phase 4: Configuring Security ===");
            let cert_dir = data_dir.join("certs");
            match TlsConfig::generate_self_signed(&cert_dir, &node_id) {
                Ok(tls) => tracing::info!(ca = %tls.ca_cert_path.display(), "TLS ready"),
                Err(e) => tracing::warn!(error = %e, "TLS setup failed — running unencrypted"),
            }
            if encrypt {
                let key_path = data_dir.join("master.key");
                match plexus_security::encryption::EncryptionManager::load_or_generate(&key_path) {
                    Ok(_) => {
                        tracing::info!("at-rest encryption: ENABLED (AES-256-GCM, key persisted)")
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "encryption key setup failed — running unencrypted")
                    }
                }
            }

            // ── Phase 5: Web UI ──────────────────────────────────────────────
            tracing::info!("=== Phase 5: Starting Web UI ===");
            let web_state = Arc::new(WebState {
                node_id: node_id.clone(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                start_time,
                storage_mode: tier_router.mode().to_string(),
                engine: Some(Arc::clone(&engine)),
                bench: Arc::new(BenchState::new()),
            });
            let web_router = routes::router(web_state);
            let web_listen: SocketAddr = web_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid web address '{}': {}", web_addr, e))?;
            tokio::spawn(async move {
                tracing::info!(addr = %web_listen, "Web UI listening");
                let listener = match tokio::net::TcpListener::bind(web_listen).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to bind Web UI");
                        return;
                    }
                };
                if let Err(e) = axum::serve(listener, web_router).await {
                    tracing::error!(error = %e, "Web UI error");
                }
            });

            // ── Phase 6: gRPC Server ─────────────────────────────────────────
            tracing::info!("=== Phase 6: Starting gRPC Server ===");
            let grpc_listen: SocketAddr = grpc_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid gRPC address '{}': {}", grpc_addr, e))?;

            let svc = PlexusService::new(
                Arc::clone(&engine),
                Arc::clone(&gossip_engine),
                Arc::clone(&hash_ring),
                node_id.clone(),
                grpc_addr.clone(),
                tier_router.mode().to_string(),
            );
            let grpc_server = tonic::transport::Server::builder()
                .add_service(PlexusDbServer::new(svc))
                .serve(grpc_listen);

            let short_id = &node_id[..node_id.len().min(12)];
            tracing::info!("══════════════════════════════════════════");
            tracing::info!("  PlexusDB is READY");
            tracing::info!("  Node:    {short_id}");
            tracing::info!("  gRPC:    {grpc_addr}");
            tracing::info!("  Web UI:  http://{web_addr}");
            tracing::info!("  Gossip:  {gossip_addr}");
            tracing::info!("  Mode:    {}", tier_router.mode());
            tracing::info!("══════════════════════════════════════════");

            // Run gRPC server until Ctrl-C
            tokio::select! {
                result = grpc_server => {
                    if let Err(e) = result {
                        tracing::error!(error = %e, "gRPC server error");
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("received shutdown signal");
                }
            }

            tracing::info!("shutting down engine...");
            engine.shutdown()?;
            tracing::info!("shutdown complete");
            Ok(())
        }

        Commands::Status { addr } => {
            // Connect via gRPC and print node info
            let endpoint = format!("http://{addr}");
            let mut client = plexus_api::proto::plexus_db_client::PlexusDbClient::connect(endpoint)
                .await
                .map_err(|e| anyhow::anyhow!("cannot connect to {addr}: {e}"))?;

            let info = client
                .node_info(plexus_api::proto::NodeInfoRequest {})
                .await
                .map_err(|e| anyhow::anyhow!("node_info RPC failed: {e}"))?
                .into_inner();

            println!("PlexusDB Node");
            println!("  ID:      {}", info.node_id);
            println!("  Version: {}", info.version);
            println!("  Address: {}", info.address);
            if let Some(s) = info.storage {
                println!("  SSTables: {}", s.sstable_count);
                println!("  Disk:     {} bytes", s.ssd_used_bytes);
                println!("  Mode:     {}", s.storage_mode);
            }
            Ok(())
        }

        Commands::Bench {
            keys,
            value_size,
            data_dir,
        } => {
            fmt().with_env_filter(EnvFilter::new("warn")).init();

            println!("PlexusDB Benchmark");
            println!("  Keys:       {keys}");
            println!("  Value size: {value_size} bytes");
            println!("  Data dir:   {}", data_dir.display());
            println!();

            let engine = Engine::open(EngineConfig {
                data_dir,
                memtable_size: 64 * 1024 * 1024,
                ..EngineConfig::default()
            })?;

            let value = vec![0xABu8; value_size];

            // Write benchmark
            let t0 = std::time::Instant::now();
            for i in 0..keys {
                let key = format!("bench_key_{i:010}").into_bytes();
                engine.put(key, value.clone(), "bench")?;
            }
            engine.sync()?;
            let write_elapsed = t0.elapsed();
            let write_ops = keys as f64 / write_elapsed.as_secs_f64();
            println!(
                "Write: {keys} keys in {:.2}s = {write_ops:.0} ops/sec",
                write_elapsed.as_secs_f64()
            );

            // Read benchmark (sequential)
            let t1 = std::time::Instant::now();
            let mut hits = 0usize;
            for i in 0..keys {
                let key = format!("bench_key_{i:010}").into_bytes();
                if engine.get(&key, "bench")?.is_some() {
                    hits += 1;
                }
            }
            let read_elapsed = t1.elapsed();
            let read_ops = keys as f64 / read_elapsed.as_secs_f64();
            println!(
                "Read:  {keys} keys in {:.2}s = {read_ops:.0} ops/sec  ({hits} hits)",
                read_elapsed.as_secs_f64()
            );

            let m = engine.metrics();
            println!();
            println!("Engine metrics:");
            println!("  SSTables:   {}", m.sstable_count);
            println!("  Disk bytes: {}", m.total_disk_bytes);
            println!("  Cache hit%: {:.1}%", m.cache.hit_rate * 100.0);

            engine.shutdown()?;
            Ok(())
        }

        Commands::BenchLive {
            grpc_addr,
            web_addr,
            keys,
            value_size,
            concurrency,
        } => {
            use std::sync::atomic::Ordering;
            use std::time::{Instant, SystemTime, UNIX_EPOCH};

            fmt().with_env_filter(EnvFilter::new("warn")).init();

            let bench = Arc::new(BenchState::new());
            bench.total_keys.store(keys as u64, Ordering::Relaxed);
            bench.value_size.store(value_size as u64, Ordering::Relaxed);

            // Serve the web UI with live bench state (no engine needed)
            let web_state = Arc::new(WebState {
                node_id: "bench-runner".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                start_time: Instant::now(),
                storage_mode: "bench".to_string(),
                engine: None,
                bench: Arc::clone(&bench),
            });
            let web_router = routes::router(web_state);
            let web_listen: SocketAddr = web_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid web address '{}': {}", web_addr, e))?;
            tokio::spawn(async move {
                match tokio::net::TcpListener::bind(web_listen).await {
                    Ok(listener) => { axum::serve(listener, web_router).await.ok(); }
                    Err(e) => { eprintln!("bench-live: failed to bind {web_listen}: {e}"); }
                }
            });

            println!("PlexusDB Live Benchmark");
            println!("  Target:      {grpc_addr}");
            println!("  Keys:        {keys}");
            println!("  Value size:  {value_size} bytes");
            println!("  Concurrency: {concurrency}");
            println!("  Dashboard:   http://{web_addr}");
            println!();

            // Connect client
            let client = plexus_client::PlexusClient::connect(&grpc_addr)
                .await
                .map_err(|e| anyhow::anyhow!("cannot connect to {grpc_addr}: {e}"))?;

            let value = vec![0xABu8; value_size];
            let keys_per_task = keys / concurrency;

            // ── Write phase ──────────────────────────────────────────────────
            println!("Phase 1/2: Writing {keys} keys with {concurrency} concurrent tasks...");
            bench.running.store(true, Ordering::Relaxed);
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            bench.started_at_ms.store(now_ms, Ordering::Relaxed);

            let mut write_tasks = Vec::new();
            for t in 0..concurrency {
                let client = client.clone();
                let bench = Arc::clone(&bench);
                let value = value.clone();
                let start = t * keys_per_task;
                let end = if t == concurrency - 1 { keys } else { start + keys_per_task };
                write_tasks.push(tokio::spawn(async move {
                    for i in start..end {
                        let key = format!("bench_key_{i:010}").into_bytes();
                        let t0 = Instant::now();
                        match client.put(key, value.clone()).await {
                            Ok(_) => {
                                let ns = t0.elapsed().as_nanos() as u64;
                                bench.writes_done.fetch_add(1, Ordering::Relaxed);
                                bench.write_ns_total.fetch_add(ns, Ordering::Relaxed);
                            }
                            Err(_) => { bench.errors.fetch_add(1, Ordering::Relaxed); }
                        }
                        if !bench.running.load(Ordering::Relaxed) { break; }
                    }
                }));
            }
            for t in write_tasks { t.await.ok(); }

            let writes = bench.writes_done.load(Ordering::Relaxed);
            let write_ns = bench.write_ns_total.load(Ordering::Relaxed);
            let write_elapsed = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                - bench.started_at_ms.load(Ordering::Relaxed);
            let write_ops = if write_elapsed > 0 { writes * 1000 / write_elapsed } else { 0 };
            let write_avg_us = if writes > 0 { write_ns / writes / 1000 } else { 0 };
            println!("  Write: {writes} keys in {:.2}s = {write_ops} ops/sec  (avg {write_avg_us}µs/op)",
                write_elapsed as f64 / 1000.0);

            // ── Read phase ───────────────────────────────────────────────────
            println!("Phase 2/2: Reading {keys} keys with {concurrency} concurrent tasks...");
            let read_start_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

            let mut read_tasks = Vec::new();
            for t in 0..concurrency {
                let client = client.clone();
                let bench = Arc::clone(&bench);
                let start = t * keys_per_task;
                let end = if t == concurrency - 1 { keys } else { start + keys_per_task };
                read_tasks.push(tokio::spawn(async move {
                    for i in start..end {
                        let key = format!("bench_key_{i:010}").into_bytes();
                        let t0 = Instant::now();
                        match client.get(key).await {
                            Ok(_) => {
                                let ns = t0.elapsed().as_nanos() as u64;
                                bench.reads_done.fetch_add(1, Ordering::Relaxed);
                                bench.read_ns_total.fetch_add(ns, Ordering::Relaxed);
                            }
                            Err(_) => { bench.errors.fetch_add(1, Ordering::Relaxed); }
                        }
                        if !bench.running.load(Ordering::Relaxed) { break; }
                    }
                }));
            }
            for t in read_tasks { t.await.ok(); }

            let reads = bench.reads_done.load(Ordering::Relaxed);
            let read_ns = bench.read_ns_total.load(Ordering::Relaxed);
            let read_elapsed = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                - read_start_ms;
            let read_ops = if read_elapsed > 0 { reads * 1000 / read_elapsed } else { 0 };
            let read_avg_us = if reads > 0 { read_ns / reads / 1000 } else { 0 };
            println!("  Read:  {reads} keys in {:.2}s = {read_ops} ops/sec  (avg {read_avg_us}µs/op)",
                read_elapsed as f64 / 1000.0);

            bench.running.store(false, Ordering::Relaxed);
            println!();
            println!("Done. Dashboard still live at http://{web_addr} — Ctrl-C to exit.");
            tokio::signal::ctrl_c().await.ok();
            Ok(())
        }

        Commands::GenCerts { output, node_id } => {            fmt().with_env_filter(EnvFilter::new("info")).init();
            let tls = TlsConfig::generate_self_signed(&output, &node_id)?;
            println!("Certificates written to: {}", output.display());
            println!("  CA cert:   {}", tls.ca_cert_path.display());
            println!("  Node cert: {}", tls.node_cert_path.display());
            println!("  Node key:  {}", tls.node_key_path.display());
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
        &node_id[..node_id.len().min(12)]
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

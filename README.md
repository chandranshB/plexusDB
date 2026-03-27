<p align="center">
  <img src="assets/logo.png" alt="PlexusDB" width="120">
</p>

<h1 align="center">PlexusDB</h1>

<p align="center">
  <strong>A hardware-aware, distributed NoSQL database.</strong><br>
  Single binary. Zero config. Built to run on commodity hardware.
</p>

<p align="center">
  <a href="https://github.com/chandranshB/plexusDB/actions"><img src="https://img.shields.io/github/actions/workflow/status/chandranshB/plexusDB/ci.yml?branch=main&style=flat-square" alt="CI"></a>
  <a href="https://github.com/chandranshB/plexusDB/releases"><img src="https://img.shields.io/github/v/release/chandranshB/plexusDB?style=flat-square&color=06b6d4" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square" alt="License"></a>
  <a href="https://github.com/chandranshB/plexusDB/wiki"><img src="https://img.shields.io/badge/docs-wiki-8b5cf6?style=flat-square" alt="Docs"></a>
</p>

---

PlexusDB is a distributed key-value store written in Rust. It detects the underlying storage hardware at startup — NVMe, SATA SSD, or spinning disk — and automatically tunes its I/O strategy to extract maximum throughput from whatever you give it.

On Linux, it uses `io_uring` with `O_DIRECT` to bypass the kernel page cache entirely. On SSDs, it parallelizes aggressively. On HDDs, it forces strictly sequential writes to avoid seek penalties. On mixed hardware, it tiers data automatically: hot metadata on SSD, bulk storage on HDD, frozen archives on S3.

The result is a database that runs well on a $5/month VPS and scales to a multi-node cluster without changing a single configuration flag.

## Key Design Decisions

- **Single binary.** No JVM, no runtime dependencies, no sidecar processes. One static executable, ~15MB stripped. Deploy with `scp`.
- **Hardware-aware I/O.** Probes `/sys/block` on Linux, latency probing on Windows. Adapts write patterns to match the physical medium.
- **LSM-Tree storage engine.** Write-optimized with full leveled compaction (L0–L6), Bloom filters (1% FP rate), and zstd-compressed data blocks. Manages its own block cache since `O_DIRECT` bypasses the kernel's.
- **Strict namespace isolation.** Keys in different namespaces never collide, enforced at the MemTable level — not just a string prefix.
- **SWIM gossip protocol.** Membership discovery and failure detection with O(log n) convergence. No external coordination service required.
- **mTLS by default.** Auto-generates a CA and node certificates on first boot. Zero-config encryption in transit.
- **S3 cold-tier.** When local disk usage exceeds a threshold, frozen SSTables are compressed with zstd and uploaded to S3 (or any compatible store — MinIO, Cloudflare R2, etc.).
- **Cross-platform.** Production target is Linux (io_uring). Fully functional on Windows and macOS via the standard I/O fallback — develop on your laptop, deploy to Linux.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        plexus (binary)                           │
│  ┌──────────┐ ┌──────────┐ ┌───────────┐ ┌───────────────────┐  │
│  │ gRPC API │ │  Web UI  │ │    CLI    │ │  Gossip (UDP)     │  │
│  │  :9090   │ │  :8080   │ │  (clap)   │ │  :7947            │  │
│  └────┬─────┘ └────┬─────┘ └─────┬─────┘ └────────┬──────────┘  │
│       │            │             │                 │             │
│  ┌────┴────────────┴─────────────┴─────────────────┴──────────┐  │
│  │                     Storage Engine                         │  │
│  │  WAL ──→ MemTable ──→ SSTable L0 ──→ L1 ──→ ... ──→ L6   │  │
│  │          (SkipList)   (Bloom+zstd)   (Leveled Compaction)  │  │
│  └────────────────────────┬───────────────────────────────────┘  │
│                           │                                      │
│  ┌────────────────────────┴───────────────────────────────────┐  │
│  │                    Tier Router                             │  │
│  │  ┌─────────┐    ┌─────────┐    ┌─────────┐                │  │
│  │  │   SSD   │    │   HDD   │    │   S3    │                │  │
│  │  │ WAL,L0  │    │ L2+,bulk│    │ frozen  │                │  │
│  │  │ Bloom,  │    │ seq I/O │    │ archive │                │  │
│  │  │ index   │    │         │    │         │                │  │
│  │  └─────────┘    └─────────┘    └─────────┘                │  │
│  └────────────────────────────────────────────────────────────┘  │
│                           │                                      │
│  ┌────────────────────────┴───────────────────────────────────┐  │
│  │                     I/O Backend                            │  │
│  │           io_uring + O_DIRECT  (Linux)                     │  │
│  │           seek + read/write    (Windows/macOS)             │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### Crate Map

| Crate | Purpose |
|---|---|
| `plexus-io` | Aligned buffers, `IoBackend` trait, io_uring and fallback implementations |
| `plexus-meta` | Embedded SQLite for SSTable manifests, cluster state, config |
| `plexus-core` | WAL, MemTable, SSTable, Bloom filter, block cache, compaction, merge iterator |
| `plexus-storage` | Hardware detection (SSD/HDD/NVMe), tier routing, S3 upload/retrieval |
| `plexus-cluster` | SWIM gossip, consistent hash ring, Merkle-tree anti-entropy repair |
| `plexus-security` | AES-256-GCM encryption, mTLS cert generation, auth |
| `plexus-api` | gRPC service (tonic) — Put, Get, Delete, Batch, Scan, ClusterStatus |
| `plexus-web` | Embedded Axum dashboard + Prometheus `/metrics` endpoint |
| `plexus-client` | Ergonomic Rust client library |
| `plexus-server` | Binary entry point, CLI, startup orchestration |

## Quick Start

### From Source

```bash
git clone https://github.com/chandranshB/plexusDB.git
cd plexusDB

# Build (release mode, ~2 min first time)
cargo build --release

# Start a single node
./target/release/plexus start

# With custom options
./target/release/plexus start \
  --data-dir /var/lib/plexus \
  --grpc-addr 0.0.0.0:9090 \
  --web-addr 0.0.0.0:8080 \
  --memtable-size-mb 64 \
  --cache-size-mb 512
```

### Docker

Single node — data persisted to a named volume:

```bash
docker compose up
```

3-node cluster with gossip:

```bash
docker compose --profile cluster up
```

Or run directly:

```bash
docker run -d \
  -p 9090:9090 \
  -p 8080:8080 \
  -v plexus-data:/data \
  ghcr.io/chandranshb/plexusdb:latest \
  start --data-dir /data
```

### Cluster (3 Nodes, bare metal)

```bash
# Node 1 (bootstrap)
plexus start --node-id node-1 --gossip-addr 10.0.0.1:7947

# Node 2
plexus start --node-id node-2 --gossip-addr 10.0.0.2:7947 \
  --join 10.0.0.1:7947

# Node 3
plexus start --node-id node-3 --gossip-addr 10.0.0.3:7947 \
  --join 10.0.0.1:7947
```

Nodes discover each other via gossip. No configuration files, no Zookeeper, no etcd.

## Rust Client Library

Add to your `Cargo.toml`:

```toml
[dependencies]
plexus-client = { git = "https://github.com/chandranshB/plexusDB" }
tokio = { version = "1", features = ["full"] }
```

Basic usage:

```rust
use plexus_client::PlexusClient;

#[tokio::main]
async fn main() -> Result<(), plexus_client::Error> {
    let client = PlexusClient::connect("http://127.0.0.1:9090").await?;

    // Put / Get / Delete
    client.put("user:1001", b"{\"name\":\"alice\"}").await?;
    let val = client.get("user:1001").await?;
    client.delete("user:1001").await?;

    // Namespaces — strict isolation, keys never collide across namespaces
    let users = client.namespace("users");
    let sessions = client.namespace("sessions");

    users.put("alice", b"admin").await?;
    sessions.put("alice", b"tok_xyz").await?;

    // Scan a key range
    let results = users.scan("a".."z", 100).await?;

    // Batch operations
    users.batch_put([
        ("bob",   b"viewer".as_slice()),
        ("carol", b"editor".as_slice()),
    ]).await?;

    // Node info and metrics
    let info = client.node_info().await?;
    println!("node: {} | sstables: {} | cache hit: {:.1}%",
        info.node_id, info.sstable_count, info.cache_hit_rate * 100.0);

    Ok(())
}
```

The `ScanRange` trait is implemented for all standard Rust range types:

```rust
client.scan(.., 0).await?;          // full scan, unlimited
client.scan("a".., 50).await?;      // from "a" to end, limit 50
client.scan("a".."z", 0).await?;    // ["a", "z")
client.scan("a"..="z", 0).await?;   // ["a", "z"]
```

## Configuration

PlexusDB is configured entirely through CLI flags. No YAML/TOML files to manage.

| Flag | Default | Description |
|---|---|---|
| `--data-dir` | `./plexus-data` | Data directory |
| `--grpc-addr` | `0.0.0.0:9090` | gRPC listen address |
| `--web-addr` | `0.0.0.0:8080` | Web UI / metrics listen address |
| `--gossip-addr` | `0.0.0.0:7947` | Gossip protocol listen address |
| `--join` | — | Address of an existing node to join |
| `--node-id` | *(auto UUID)* | Unique node identifier |
| `--encrypt` | `false` | Enable AES-256-GCM encryption at rest |
| `--memtable-size-mb` | `32` | MemTable size before flush |
| `--cache-size-mb` | `256` | Block cache size |
| `--compaction-threads` | `2` | Number of compaction threads |
| `--log-level` | `info` | Log level (trace/debug/info/warn/error) |

### Environment Variables

```bash
AWS_ACCESS_KEY_ID=...          # AWS credentials for S3 cold-tier
AWS_SECRET_ACCESS_KEY=...
PLEXUS_S3_BUCKET=my-bucket     # S3 bucket name
PLEXUS_S3_ENDPOINT=http://...  # S3-compatible endpoint (MinIO, R2, etc.)
```

## Data Model

All operations are namespaced. The default namespace is `default`. Namespaces are free to create and don't need to be declared upfront. Keys in different namespaces are completely isolated — enforced at the MemTable skip list level, not just as a string prefix.

```
PUT   namespace  key  →  bytes
GET   namespace  key  →  bytes | not found
DEL   namespace  key
SCAN  namespace  start..end  →  stream<(key, bytes)>
```

Batch variants (`BatchPut`, `BatchGet`, `BatchDelete`) are available for throughput-sensitive workloads.

## Storage Engine Internals

### Write Path

1. Entry is appended to the **WAL** (CRC32-checksummed, fsync'd)
2. Entry is inserted into the **MemTable** (lock-free skip list, keyed by `(namespace, key, timestamp)`)
3. When MemTable reaches threshold → frozen → background flush to **SSTable L0**
4. WAL is sealed and recycled after flush

### Read Path

1. Check **active MemTable** — O(log n), namespace-isolated
2. Check **frozen MemTables** (newest first)
3. For **L0**: scan all files, pick the entry with the highest timestamp (L0 files may overlap)
4. For **L1+**: binary search — files are non-overlapping, first match is authoritative
5. **Bloom filter** (1% FP rate) skips files that definitely don't contain the key
6. **Block cache** (LRU) avoids re-reading hot blocks from disk

### Compaction

Full leveled compaction across all 7 levels with a 10x size ratio:

| Level | Max Size | Notes |
|---|---|---|
| L0 | 4 files trigger | Fresh flushes, may overlap |
| L1 | 10 MB | Sorted, non-overlapping |
| L2 | 100 MB | Sorted, non-overlapping |
| L3 | 1 GB | Sorted, non-overlapping |
| L4 | 10 GB | Sorted, non-overlapping |
| L5 | 100 GB | Sorted, non-overlapping |
| L6 | 1 TB | Sorted, non-overlapping |

Tombstones are dropped during compaction to L2+ once they've propagated through all higher levels. Compaction runs in a background thread and does not block reads or writes.

### SSTable Format

```
┌─────────────────────────────────┐
│ Data Blocks (zstd compressed)   │  ← 4KB aligned, xxh3 checksummed
├─────────────────────────────────┤
│ Bloom Filter                    │  ← 1% false positive rate
├─────────────────────────────────┤
│ Block Index                     │  ← sparse, binary-searchable
├─────────────────────────────────┤
│ Footer (48 bytes)               │  ← magic, version, offsets, blake3
└─────────────────────────────────┘
```

Every block is independently checksummed (xxh3). Corruption is detected on read and logged with the expected vs actual checksum.

## Monitoring

### Web Dashboard

PlexusDB ships with an embedded monitoring dashboard at `http://localhost:8080`. Compiled into the binary — no external files, no npm, no CDN.

### Prometheus

Metrics are available in Prometheus text format at `http://localhost:8080/metrics`:

```
plexus_writes_total{node="..."}
plexus_reads_total{node="..."}
plexus_memtable_size_bytes{node="..."}
plexus_sstable_count{node="..."}
plexus_cache_hit_rate{node="..."}
plexus_sstable_level_count{node="...",level="L0"}
...
```

### gRPC Status

```bash
# Check node info via CLI
plexus status --addr 127.0.0.1:9090
```

## Security

| Layer | Implementation |
|---|---|
| Transport | mTLS 1.3 (rustls), auto-generated certificates |
| At Rest | AES-256-GCM with HKDF-SHA256 per-block key derivation |
| Auth | Connection string auth (`plexus://user:pass@host:port`) |

```bash
# Generate certificates manually
plexus gen-certs --output ./certs --node-id my-node

# Start with at-rest encryption
plexus start --encrypt
```

On first boot without existing certificates, PlexusDB generates a self-signed CA and node certificate pair. For production, replace these with certificates from your PKI.

## Platform Support

| Platform | I/O Backend | Status |
|---|---|---|
| Linux x86_64 | io_uring + O_DIRECT | **Production** |
| Linux aarch64 | io_uring + O_DIRECT | **Production** |
| Windows x86_64 | Standard I/O | Development |
| macOS (Apple Silicon) | Standard I/O | Development |
| macOS (Intel) | Standard I/O | Development |

The standard I/O backend is functionally equivalent — it passes all the same tests, handles the same data format, and supports the same API. The difference is throughput: io_uring avoids syscall overhead and page cache copies, which matters at high write volumes.

## Building

### Requirements

- Rust 1.85+ (stable)
- C compiler (for SQLite bundled build)

`protoc` is **not required** — gRPC stubs are pre-generated and committed. Install it only if you modify `proto/plexus.proto`.

### Build Commands

```bash
# Debug build
cargo build

# Release build (what you deploy — LTO, stripped)
cargo build --release

# Run all tests
cargo test --workspace

# Run integration tests only
cargo test -p plexus-integration-tests

# Run the built-in benchmark
cargo run --release --bin plexus -- bench --keys 100000 --value-size 256

# Run specific crate tests
cargo test -p plexus-core
cargo test -p plexus-cluster
```

### Build Profile

```toml
[profile.release]
lto = "fat"          # Full link-time optimization
codegen-units = 1    # Single codegen unit for maximum optimization
strip = true         # Strip debug symbols (~15MB final binary)
opt-level = 3
panic = "abort"
```

## Testing

The test suite covers unit tests, property tests, and end-to-end integration tests.

```bash
# Everything
cargo test --workspace

# Integration tests (WAL recovery, namespace isolation, flush/compaction)
cargo test -p plexus-integration-tests

# A specific integration test
cargo test -p plexus-integration-tests test_wal_recovery_after_restart

# With logging output
RUST_LOG=debug cargo test -p plexus-integration-tests -- --nocapture

# Built-in write/read benchmark (no server needed)
cargo run --release --bin plexus -- bench \
  --keys 500000 \
  --value-size 512 \
  --data-dir /tmp/plexus-bench
```

Key integration test scenarios:

| Test | What it verifies |
|---|---|
| `test_wal_recovery_after_restart` | Data survives a crash (WAL replay) |
| `test_data_survives_clean_shutdown` | Data survives a clean shutdown |
| `test_namespace_isolation` | Keys in different namespaces never collide |
| `test_delete_visible_after_flush` | Tombstones correctly shadow SSTable values |
| `test_scan_excludes_tombstones` | Deleted keys don't appear in scan results |
| `test_concurrent_writes_and_reads` | No data races under concurrent access |
| `test_data_readable_after_flush_to_sstable` | Flush is synchronous and durable |

## Project Structure

```
plexusdb/
├── crates/
│   ├── plexus-io/          # I/O abstraction (io_uring / fallback)
│   ├── plexus-meta/        # SQLite metadata store
│   ├── plexus-core/        # Storage engine (WAL, MemTable, SSTable)
│   ├── plexus-storage/     # Hardware detection, tier routing, S3
│   ├── plexus-cluster/     # Gossip, hash ring, Merkle repair
│   ├── plexus-security/    # Encryption, TLS, auth
│   ├── plexus-api/         # gRPC service layer
│   ├── plexus-web/         # Embedded web dashboard
│   └── plexus-client/      # Rust client library
├── plexus-server/          # Binary entry point
├── tests/                  # Integration tests
├── proto/                  # Protobuf service definitions
├── Dockerfile              # Multi-stage build (builder + slim runtime)
├── docker-compose.yml      # Single-node and 3-node cluster profiles
└── assets/                 # Logo, docs assets
```

## Roadmap

### v0.1 — Foundation (current)
- [x] LSM-Tree engine (WAL, MemTable, SSTable, Bloom, full L0–L6 compaction)
- [x] Strict namespace isolation (enforced at MemTable level)
- [x] Hardware detection and tiered storage routing
- [x] io_uring backend (Linux) + fallback (Windows/macOS)
- [x] SWIM gossip protocol and consistent hash ring
- [x] AES-256-GCM encryption at rest
- [x] mTLS certificate auto-generation
- [x] gRPC API (Put, Get, Delete, Batch, Scan, ClusterStatus, NodeInfo)
- [x] S3 cold-tier upload and retrieval
- [x] Prometheus metrics endpoint
- [x] Embedded web dashboard
- [x] Rust client library (`plexus-client`)
- [x] Docker image + Docker Compose (single-node and cluster)
- [x] Integration test suite (WAL recovery, namespace isolation, flush races)
- [x] CI with cross-platform release binaries (Linux musl, Windows, macOS)

### v0.2 — Distributed Consistency
- [ ] Raft consensus (openraft) for linearizable writes
- [ ] Read replicas with configurable consistency levels
- [ ] Actual gossip UDP transport (currently in-process only)
- [ ] Client libraries (Python, Go, Node.js)

### v0.3 — Production Hardening
- [ ] Snapshot and point-in-time restore
- [ ] Online schema-less secondary indexes
- [ ] Helm chart for Kubernetes
- [ ] Rate limiting and per-namespace quotas

### v0.4 — Document Store
- [ ] JSON document storage with field-level access
- [ ] Query language (subset of SQL for filtering/projection)
- [ ] Change data capture (CDC) streams

## Contributing

Contributions are welcome. PlexusDB is a young project and there's a lot of surface area to cover.

**Before opening a PR**, please:

1. Run `cargo test --workspace` — all tests must pass
2. Run `cargo clippy --workspace -- -D warnings` — no warnings
3. Format with `cargo fmt --all`
4. If you're adding a new feature, open an issue first to discuss the design

### Good First Issues

- [ ] Add `cargo bench` benchmarks for MemTable insert/lookup
- [ ] Implement `Display` for `EngineConfig` (pretty-print on startup)
- [ ] Improve Windows hardware detection with WMI queries
- [ ] Add TTL/expiry support (proto field already defined)
- [ ] Python client library using the gRPC stubs

### Development Tips

```bash
# Watch mode (recompile on save)
cargo watch -x "test -p plexus-core"

# Run with trace logging
RUST_LOG=trace cargo run -- start

# Test a single module
cargo test -p plexus-core sstable::reader::tests
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

```
Copyright 2026 PlexusDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```

---

<p align="center">
  <sub>Built with Rust, stubbornness, and an unreasonable amount of <code>unsafe</code> blocks.</sub>
</p>

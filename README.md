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

PlexusDB is a distributed key-value and document store written in Rust. It detects the underlying storage hardware at startup — NVMe, SATA SSD, or spinning disk — and automatically tunes its I/O strategy to extract maximum throughput from whatever you give it.

On Linux, it uses `io_uring` with `O_DIRECT` to bypass the kernel page cache entirely. On SSDs, it parallelizes aggressively. On HDDs, it forces strictly sequential writes to avoid seek penalties. On mixed hardware, it tiers data automatically: hot metadata on SSD, bulk storage on HDD, frozen archives on S3.

The result is a database that runs well on a $5/month VPS and scales to a multi-node cluster without changing a single configuration flag.

## Key Design Decisions

- **Single binary.** No JVM, no runtime dependencies, no sidecar processes. One static executable, ~15MB stripped. Deploy with `scp`.
- **Hardware-aware I/O.** Probes `/sys/block` on Linux, `GetDiskFreeSpaceEx` + latency probing on Windows. Adapts write patterns to match the physical medium.
- **LSM-Tree storage engine.** Write-optimized with leveled compaction, Bloom filters (1% FP rate), and zstd-compressed data blocks. Manages its own block cache since `O_DIRECT` bypasses the kernel's.
- **Raft consensus.** Linearizable reads and writes across the cluster via `openraft`. Tunable consistency — switch to eventual mode for higher throughput when you don't need strong guarantees.
- **SWIM gossip protocol.** Membership discovery and failure detection with O(log n) convergence. No external coordination service required.
- **mTLS by default.** Auto-generates a CA and node certificates on first boot. Zero-config encryption in transit.
- **Cross-platform.** Production target is Linux (io_uring). Fully functional on Windows and macOS via the standard I/O fallback backend — develop on your laptop, deploy to Linux.

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
│  │  WAL ──→ MemTable ──→ SSTable L0 ──→ L1 ──→ ... ──→ LN   │  │
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
| `plexus-storage` | Hardware detection (SSD/HDD/NVMe), tier routing, S3 agent |
| `plexus-cluster` | SWIM gossip, consistent hash ring, Merkle-tree anti-entropy repair |
| `plexus-security` | AES-256-GCM encryption, mTLS cert generation, auth |
| `plexus-api` | gRPC service (tonic) — Put, Get, Delete, BatchWrite, Scan |
| `plexus-web` | Embedded Axum dashboard for monitoring and admin |
| `plexus-server` | Binary entry point, CLI, startup orchestration |

## Quick Start

### From Source

```bash
# Clone
git clone https://github.com/chandranshB/plexusDB.git
cd plexusDB

# Build (release mode, ~2 min)
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

```bash
docker run -d \
  -p 9090:9090 \
  -p 8080:8080 \
  -v plexus-data:/data \
  ghcr.io/chandranshb/plexusdb:latest \
  start --data-dir /data
```

### Cluster (3 Nodes)

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

## Configuration

PlexusDB is configured entirely through CLI flags. No YAML/TOML files to manage.

| Flag | Default | Description |
|---|---|---|
| `--data-dir` | `./plexus-data` | Data directory |
| `--grpc-addr` | `0.0.0.0:9090` | gRPC listen address |
| `--web-addr` | `0.0.0.0:8080` | Web UI listen address |
| `--gossip-addr` | `0.0.0.0:7947` | Gossip protocol listen address |
| `--join` | — | Address of an existing node to join |
| `--node-id` | *(auto)* | Unique node identifier |
| `--encrypt` | `false` | Enable AES-256-GCM encryption at rest |
| `--memtable-size-mb` | `32` | MemTable size before flush |
| `--cache-size-mb` | `256` | Block cache size |
| `--compaction-threads` | `2` | Number of compaction threads |
| `--log-level` | `info` | Log level (trace/debug/info/warn/error) |

### Environment Variables

```bash
PLEXUS_ENCRYPTION_KEY=<hex>    # 256-bit master key for at-rest encryption
PLEXUS_S3_BUCKET=my-bucket     # S3 bucket for cold-tier storage
PLEXUS_S3_ENDPOINT=http://...  # S3-compatible endpoint (MinIO, etc.)
```

## Data Model

PlexusDB supports two access patterns through the same gRPC API:

**Key-Value** — raw bytes in, raw bytes out. No schema, no overhead.

```
PUT   /namespace/key  →  bytes
GET   /namespace/key  →  bytes
DEL   /namespace/key
SCAN  /namespace/start..end  →  stream<bytes>
```

**Document** — JSON values with field-level access (planned, v0.2).

All operations are namespaced. The default namespace is `default`. Namespaces are free to create and don't need to be declared upfront.

## Storage Engine Internals

### Write Path

1. Entry is appended to the **WAL** (fsync'd for durability)
2. Entry is inserted into the **MemTable** (lock-free skip list)
3. When MemTable reaches threshold → frozen → background flush to **SSTable L0**
4. WAL is sealed and recycled

### Read Path

1. Check **MemTable** (newest data, in-memory, O(log n))
2. Check **Bloom filter** for each SSTable (skip files that definitely don't contain the key)
3. Binary search the SSTable **index** to find the right data block
4. Decompress and scan the block (zstd, ~3:1 ratio)
5. **Block cache** stores hot blocks to avoid repeated decompression

### Compaction

Leveled compaction with a 10x size ratio between levels:

| Level | Max Size | Typical Content |
|---|---|---|
| L0 | 4 files | Fresh flushes (may overlap) |
| L1 | 10 MB | Sorted, non-overlapping |
| L2 | 100 MB | Sorted, non-overlapping |
| L3 | 1 GB | Sorted, non-overlapping |
| L4 | 10 GB | Sorted, non-overlapping |
| L5 | 100 GB | Sorted, non-overlapping |
| L6 | 1 TB | Sorted, non-overlapping |

Compaction runs in dedicated threads and does not block reads or writes.

### SSTable Format

```
┌─────────────────────────────────┐
│ Data Blocks (zstd compressed)   │  ← 4KB aligned, checksummed
├─────────────────────────────────┤
│ Bloom Filter                    │  ← 1% false positive rate
├─────────────────────────────────┤
│ Block Index                     │  ← sparse, binary-searchable
├─────────────────────────────────┤
│ Footer (48 bytes)               │  ← magic, version, offsets, blake3
└─────────────────────────────────┘
```

Every block is independently checksummed (xxh3) and verifiable. Corruption is detected on read and logged.

## Web Dashboard

PlexusDB ships with an embedded monitoring dashboard at `http://localhost:8080`. It's compiled into the binary — no external files, no npm, no CDN dependencies.

The dashboard shows:
- Node status and uptime
- Storage tier utilization (SSD / HDD / S3)
- Cluster membership and health
- SSTable level distribution
- Block cache hit rate
- Write throughput

## Security

| Layer | Implementation |
|---|---|
| Transport | mTLS 1.3 (rustls), auto-generated certificates |
| At Rest | AES-256-GCM with HKDF-SHA256 per-block key derivation |
| Auth | Connection string auth (`plexus://user:pass@host:port`) |

```bash
# Generate certificates manually
plexus gen-certs --output ./certs --node-id my-node

# Start with encryption
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

- Rust 1.75+ (stable)
- C compiler (for SQLite bundled build)
- `protoc` (optional, for gRPC codegen — builds without it)

### Build Commands

```bash
# Debug build (fast compile, slower runtime)
cargo build

# Release build (slow compile, optimized — what you deploy)
cargo build --release

# Run tests
cargo test --workspace

# Run specific crate tests
cargo test -p plexus-core
cargo test -p plexus-cluster

# Build with all optimizations (LTO, single codegen unit)
# This is what CI produces for releases
cargo build --release  # profile already configured in Cargo.toml
```

### Build Profile

The release profile is tuned for production:

```toml
[profile.release]
lto = "fat"          # Full link-time optimization
codegen-units = 1    # Single codegen unit for maximum optimization
strip = true         # Strip debug symbols
opt-level = 3        # Maximum optimization
panic = "abort"      # No unwinding overhead
```

## Project Structure

```
plexusdb/
├── crates/
│   ├── plexus-io/          # I/O abstraction (io_uring / fallback)
│   ├── plexus-meta/        # SQLite metadata store
│   ├── plexus-core/        # Storage engine (WAL, MemTable, SSTable)
│   ├── plexus-storage/     # Hardware detection, tier routing
│   ├── plexus-cluster/     # Gossip, hash ring, repair
│   ├── plexus-security/    # Encryption, TLS, auth
│   ├── plexus-api/         # gRPC service layer
│   └── plexus-web/         # Embedded web dashboard
├── plexus-server/          # Binary entry point
├── proto/                  # Protobuf service definitions
└── assets/                 # Logo, docs assets
```

## Roadmap

### v0.1 — Foundation (current)
- [x] LSM-Tree engine (WAL, MemTable, SSTable, Bloom, compaction)
- [x] Hardware detection and tiered storage
- [x] io_uring backend (Linux) + fallback (Windows/macOS)
- [x] SWIM gossip protocol
- [x] Consistent hash ring
- [x] AES-256-GCM encryption at rest
- [x] mTLS certificate auto-generation
- [x] Embedded web dashboard
- [x] CLI with clap

### v0.2 — Distributed Reads/Writes
- [ ] Full gRPC API implementation (Put, Get, Delete, Scan)
- [ ] Raft integration (openraft) for linearizable writes
- [ ] Read replicas with configurable consistency
- [ ] Client libraries (Rust, Python, Go, Node.js)

### v0.3 — Production Hardening
- [ ] S3 cold-tier upload/retrieval
- [ ] Snapshot and restore
- [ ] Online schema-less secondary indexes
- [ ] Prometheus metrics export
- [ ] Helm chart for Kubernetes

### v0.4 — Document Store
- [ ] JSON document storage with field-level access
- [ ] Query language (subset of SQL for filtering/projection)
- [ ] Change data capture (CDC) streams
- [ ] Point-in-time recovery (PITR)

## Contributing

Contributions are welcome. PlexusDB is a young project and there's a lot of surface area to cover.

**Before opening a PR**, please:

1. Run `cargo test --workspace` and make sure everything passes
2. Run `cargo clippy --workspace` with no warnings
3. Format with `cargo fmt --all`
4. If you're adding a new feature, open an issue first to discuss the design

### Good First Issues

- [ ] Add `cargo bench` benchmarks for MemTable insert/lookup
- [ ] Implement `Display` for `EngineConfig` (pretty-print on startup)
- [ ] Add `/api/metrics` endpoint to the web dashboard
- [ ] Write integration tests for WAL replay after crash simulation
- [ ] Improve Windows hardware detection with WMI queries

### Development Tips

```bash
# Watch mode (recompile on save)
cargo watch -x "test -p plexus-core"

# Run with trace logging
RUST_LOG=trace cargo run -- start

# Test a single module
cargo test -p plexus-core bloom::tests
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

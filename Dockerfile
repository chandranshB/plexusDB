# ── Stage 1: Builder ──────────────────────────────────────────────────────────
# Uses the official Rust image. We install protoc so the gRPC stubs can be
# regenerated from source — no pre-generated files needed.
FROM rust:1.91-bookworm AS builder

# Install build dependencies:
#   protobuf-compiler  — for tonic-build gRPC codegen
#   pkg-config + libssl-dev — for TLS (rustls uses ring which needs a C compiler)
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ── Cache dependencies before copying source ──────────────────────────────────
# Copy only manifests first so Docker layer-caches the dep compile step.
COPY Cargo.toml Cargo.lock ./
COPY crates/plexus-io/Cargo.toml        crates/plexus-io/Cargo.toml
COPY crates/plexus-meta/Cargo.toml      crates/plexus-meta/Cargo.toml
COPY crates/plexus-core/Cargo.toml      crates/plexus-core/Cargo.toml
COPY crates/plexus-storage/Cargo.toml   crates/plexus-storage/Cargo.toml
COPY crates/plexus-cluster/Cargo.toml   crates/plexus-cluster/Cargo.toml
COPY crates/plexus-security/Cargo.toml  crates/plexus-security/Cargo.toml
COPY crates/plexus-api/Cargo.toml       crates/plexus-api/Cargo.toml
COPY crates/plexus-web/Cargo.toml       crates/plexus-web/Cargo.toml
COPY crates/plexus-client/Cargo.toml    crates/plexus-client/Cargo.toml
COPY plexus-server/Cargo.toml           plexus-server/Cargo.toml
COPY tests/Cargo.toml                   tests/Cargo.toml

# Create stub lib/main files so `cargo fetch` resolves without real source
RUN mkdir -p \
    crates/plexus-io/src \
    crates/plexus-meta/src \
    crates/plexus-core/src \
    crates/plexus-storage/src \
    crates/plexus-cluster/src \
    crates/plexus-security/src \
    crates/plexus-api/src/generated \
    crates/plexus-web/src \
    crates/plexus-client/src \
    plexus-server/src \
    tests && \
    for d in crates/plexus-io crates/plexus-meta crates/plexus-core \
              crates/plexus-storage crates/plexus-cluster crates/plexus-security \
              crates/plexus-web crates/plexus-client; do \
        echo "pub fn _stub() {}" > $d/src/lib.rs; \
    done && \
    echo "fn main() {}" > plexus-server/src/main.rs && \
    touch crates/plexus-api/src/lib.rs && \
    touch crates/plexus-api/src/generated/.gitkeep && \
    touch tests/engine_integration.rs

# Fetch and compile all dependencies (cached layer)
RUN cargo fetch
RUN cargo build --release --bin plexus 2>/dev/null || true

# ── Copy real source and build ─────────────────────────────────────────────────
COPY . .

# Touch source files to force recompile (stubs were replaced)
RUN find . -name "*.rs" -not -path "*/target/*" | xargs touch

RUN cargo build --release --bin plexus

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# Minimal Debian image — no Rust toolchain, no build tools.
FROM debian:bookworm-slim AS runtime

# ca-certificates: needed for S3 TLS connections
# libgcc-s1: needed by some Rust binaries on Debian
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN useradd -r -s /bin/false -d /data plexus

# Copy the binary
COPY --from=builder /build/target/release/plexus /usr/local/bin/plexus

# Data directory — mount a volume here for persistence
RUN mkdir -p /data && chown plexus:plexus /data

USER plexus
WORKDIR /data

# Ports:
#   9090 — gRPC API
#   8080 — Web UI / Prometheus metrics
#   7947 — Gossip (UDP + TCP)
EXPOSE 9090 8080 7947

VOLUME ["/data"]

ENTRYPOINT ["plexus"]
CMD ["start", "--data-dir", "/data"]

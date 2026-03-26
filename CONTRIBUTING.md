# Contributing to PlexusDB

Thanks for your interest in contributing. This document covers the basics.

## Development Setup

```bash
# Install Rust (if you haven't)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone the repo
git clone https://github.com/chandranshB/plexusDB.git
cd plexusDB

# Build
cargo build

# Run tests
cargo test --workspace

# Run with logging
RUST_LOG=debug cargo run -- start
```

### Windows

Install [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) with the "Desktop development with C++" workload, then install Rust via [rustup](https://rustup.rs).

```powershell
cargo build
cargo test --workspace
```

## Code Style

- Run `cargo fmt --all` before committing
- Run `cargo clippy --workspace` and fix all warnings
- No `unwrap()` in library code — use `?` or return `Result`
- `unwrap()` is acceptable in tests and in `main.rs` for CLI argument parsing
- Document public APIs with `///` doc comments
- Use `tracing::info!` / `tracing::debug!` for logging, not `println!`

## Architecture

PlexusDB is organized as a Cargo workspace with several crates. Dependencies flow in one direction:

```
plexus-server
├── plexus-web        (dashboard)
├── plexus-api        (gRPC handlers)
├── plexus-security   (TLS, encryption)
├── plexus-cluster    (gossip, hash ring)
├── plexus-storage    (hardware detection, tiering)
├── plexus-core       (storage engine)
│   ├── plexus-meta   (SQLite metadata)
│   └── plexus-io     (I/O abstraction)
```

**Rule:** Lower crates must never depend on higher crates. `plexus-io` knows nothing about `plexus-core`. `plexus-core` knows nothing about `plexus-cluster`.

## Pull Request Process

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Add tests if applicable
4. Run the full test suite: `cargo test --workspace`
5. Run clippy: `cargo clippy --workspace -- -D warnings`
6. Open a PR with a clear description of what and why

## Commit Messages

Use conventional-ish commits. We're not strict about it, but a prefix helps:

```
feat(core): add range delete tombstone support
fix(cluster): handle gossip packet corruption gracefully
perf(io): batch io_uring submissions for WAL writes
docs: update README with new CLI flags
test(meta): add migration rollback test
```

## Testing

- **Unit tests** live in the same file as the code they test (`#[cfg(test)] mod tests`)
- **Integration tests** go in `tests/` at the crate root
- Use `tempfile::tempdir()` for tests that need filesystem access
- Tests must not leave files behind — clean up in `Drop` or use temp dirs

## Reporting Issues

Open an issue on GitHub. Include:

- PlexusDB version (`plexus --version`)
- OS and architecture
- Steps to reproduce
- Relevant log output (run with `RUST_LOG=debug` or `RUST_LOG=trace`)

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

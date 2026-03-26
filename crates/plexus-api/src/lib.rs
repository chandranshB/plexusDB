//! # plexus-api
//!
//! gRPC service implementation for PlexusDB.

pub mod service;

// The protobuf-generated code will be here after running `cargo build`.
// The build.rs script invokes tonic-build to generate from plexus.proto.
// For now, we conditionally include it if the file exists.
#[cfg(feature = "codegen")]
pub mod proto {
    include!("generated/plexus.v1.rs");
}

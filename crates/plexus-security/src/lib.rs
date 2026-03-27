//! # plexus-security
//!
//! Encryption, TLS, and authentication for PlexusDB.

pub mod auth;
pub mod encryption;
pub mod tls;

/// Security errors.
#[derive(Debug, thiserror::Error)]
pub enum SecurityError {
    #[error("encryption error: {0}")]
    Encryption(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("authentication error: {0}")]
    Auth(String),

    #[error("key derivation error: {0}")]
    KeyDerivation(String),

    #[error("certificate error: {0}")]
    Certificate(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

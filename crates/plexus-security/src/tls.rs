//! mTLS 1.3 configuration and auto-generated certificates.
//!
//! On first boot, PlexusDB generates a self-signed CA and a node certificate
//! signed by that CA. For production, replace with certificates from your PKI.

use std::fs;
use std::path::{Path, PathBuf};
use crate::SecurityError;

/// TLS configuration paths for PlexusDB.
pub struct TlsConfig {
    /// Path to CA certificate.
    pub ca_cert_path: PathBuf,
    /// Path to node certificate.
    pub node_cert_path: PathBuf,
    /// Path to node private key.
    pub node_key_path: PathBuf,
    /// Whether mTLS is required for peer connections.
    pub require_client_cert: bool,
}

impl TlsConfig {
    /// Generate self-signed CA and node certificates on first boot.
    ///
    /// If certificates already exist at the expected paths, this is a no-op.
    pub fn generate_self_signed(cert_dir: &Path, node_id: &str) -> Result<Self, SecurityError> {
        fs::create_dir_all(cert_dir).map_err(SecurityError::Io)?;

        let ca_cert_path = cert_dir.join("ca.crt");
        let ca_key_path = cert_dir.join("ca.key");
        let node_cert_path = cert_dir.join("node.crt");
        let node_key_path = cert_dir.join("node.key");

        // Generate CA + node cert if they don't already exist
        if !ca_cert_path.exists() {
            generate_certificates(
                node_id,
                &ca_cert_path,
                &ca_key_path,
                &node_cert_path,
                &node_key_path,
            )?;
        }

        Ok(TlsConfig {
            ca_cert_path,
            node_cert_path,
            node_key_path,
            require_client_cert: true,
        })
    }
}

/// Generate a CA keypair and a node certificate signed by that CA.
fn generate_certificates(
    node_id: &str,
    ca_cert_path: &Path,
    ca_key_path: &Path,
    node_cert_path: &Path,
    node_key_path: &Path,
) -> Result<(), SecurityError> {
    use rcgen::{CertificateParams, DnType, KeyPair};

    // ── Generate CA ──
    let ca_key = KeyPair::generate()
        .map_err(|e| SecurityError::Certificate(e.to_string()))?;

    let mut ca_params = CertificateParams::new(vec!["PlexusDB CA".to_string()])
        .map_err(|e| SecurityError::Certificate(e.to_string()))?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.distinguished_name.push(DnType::CommonName, "PlexusDB Internal CA");
    ca_params.distinguished_name.push(DnType::OrganizationName, "PlexusDB");

    let ca_cert = ca_params.self_signed(&ca_key)
        .map_err(|e| SecurityError::Certificate(e.to_string()))?;

    fs::write(ca_cert_path, ca_cert.pem()).map_err(SecurityError::Io)?;
    fs::write(ca_key_path, ca_key.serialize_pem()).map_err(SecurityError::Io)?;

    tracing::info!(path = %ca_cert_path.display(), "generated CA certificate");

    // ── Generate Node Certificate ──
    let node_key = KeyPair::generate()
        .map_err(|e| SecurityError::Certificate(e.to_string()))?;

    let mut node_params = CertificateParams::new(vec![
        node_id.to_string(),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ]).map_err(|e| SecurityError::Certificate(e.to_string()))?;
    node_params.distinguished_name.push(DnType::CommonName, node_id);
    node_params.distinguished_name.push(DnType::OrganizationName, "PlexusDB");

    // Sign the node cert with the CA
    let node_cert = node_params
        .signed_by(&node_key, &ca_cert, &ca_key)
        .map_err(|e| SecurityError::Certificate(e.to_string()))?;

    fs::write(node_cert_path, node_cert.pem()).map_err(SecurityError::Io)?;
    fs::write(node_key_path, node_key.serialize_pem()).map_err(SecurityError::Io)?;

    tracing::info!(
        node_id,
        path = %node_cert_path.display(),
        "generated node certificate (signed by CA)"
    );

    Ok(())
}

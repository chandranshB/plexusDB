//! mTLS 1.3 configuration and auto-generated certificates.
//!
//! On first boot, PlexusDB generates a self-signed CA and a node certificate
//! signed by that CA. For production, replace with certificates from your PKI.

use crate::SecurityError;
use std::fs;
use std::path::{Path, PathBuf};

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
    let ca_key = KeyPair::generate().map_err(|e| SecurityError::Certificate(e.to_string()))?;

    let mut ca_params = CertificateParams::new(vec!["PlexusDB CA".to_string()])
        .map_err(|e| SecurityError::Certificate(e.to_string()))?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "PlexusDB Internal CA");
    ca_params
        .distinguished_name
        .push(DnType::OrganizationName, "PlexusDB");

    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|e| SecurityError::Certificate(e.to_string()))?;

    fs::write(ca_cert_path, ca_cert.pem()).map_err(SecurityError::Io)?;
    fs::write(ca_key_path, ca_key.serialize_pem()).map_err(SecurityError::Io)?;

    tracing::info!(path = %ca_cert_path.display(), "generated CA certificate");

    // ── Generate Node Certificate ──
    let node_key = KeyPair::generate().map_err(|e| SecurityError::Certificate(e.to_string()))?;

    let mut node_params = CertificateParams::new(vec![
        node_id.to_string(),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .map_err(|e| SecurityError::Certificate(e.to_string()))?;
    node_params
        .distinguished_name
        .push(DnType::CommonName, node_id);
    node_params
        .distinguished_name
        .push(DnType::OrganizationName, "PlexusDB");

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_generate_self_signed_creates_files() {
        let tmp = TempDir::new().unwrap();
        let tls = TlsConfig::generate_self_signed(tmp.path(), "test-node").unwrap();

        assert!(tls.ca_cert_path.exists(), "CA cert should exist");
        assert!(tls.node_cert_path.exists(), "node cert should exist");
        assert!(tls.node_key_path.exists(), "node key should exist");
        assert!(tls.require_client_cert);
    }

    #[test]
    fn test_generate_self_signed_idempotent() {
        let tmp = TempDir::new().unwrap();

        // First call generates certs
        let tls1 = TlsConfig::generate_self_signed(tmp.path(), "node-1").unwrap();
        let ca_content_1 = std::fs::read(&tls1.ca_cert_path).unwrap();

        // Second call should be a no-op (certs already exist)
        let tls2 = TlsConfig::generate_self_signed(tmp.path(), "node-1").unwrap();
        let ca_content_2 = std::fs::read(&tls2.ca_cert_path).unwrap();

        assert_eq!(
            ca_content_1, ca_content_2,
            "second call should not regenerate certs"
        );
    }

    #[test]
    fn test_generated_cert_is_valid_pem() {
        let tmp = TempDir::new().unwrap();
        let tls = TlsConfig::generate_self_signed(tmp.path(), "pem-test-node").unwrap();

        let ca_pem = std::fs::read_to_string(&tls.ca_cert_path).unwrap();
        assert!(
            ca_pem.contains("-----BEGIN CERTIFICATE-----"),
            "CA cert should be PEM"
        );

        let node_pem = std::fs::read_to_string(&tls.node_cert_path).unwrap();
        assert!(
            node_pem.contains("-----BEGIN CERTIFICATE-----"),
            "node cert should be PEM"
        );

        let key_pem = std::fs::read_to_string(&tls.node_key_path).unwrap();
        assert!(key_pem.contains("-----BEGIN"), "node key should be PEM");
    }

    #[test]
    fn test_different_node_ids_produce_different_certs() {
        let tmp1 = TempDir::new().unwrap();
        let tmp2 = TempDir::new().unwrap();

        let tls1 = TlsConfig::generate_self_signed(tmp1.path(), "node-alpha").unwrap();
        let tls2 = TlsConfig::generate_self_signed(tmp2.path(), "node-beta").unwrap();

        let cert1 = std::fs::read(&tls1.node_cert_path).unwrap();
        let cert2 = std::fs::read(&tls2.node_cert_path).unwrap();

        assert_ne!(
            cert1, cert2,
            "different node IDs should produce different certs"
        );
    }
}

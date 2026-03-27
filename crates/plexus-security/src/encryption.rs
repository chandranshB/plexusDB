//! AES-256-GCM data-at-rest encryption.
//!
//! Encrypts individual SSTable data blocks, enabling random-access
//! reads without decrypting the entire file.
//!
//! ## Key Persistence
//!
//! The master key is persisted to `<data_dir>/master.key` with mode 0600
//! (owner-read-only). On restart the same key is loaded so existing
//! encrypted SSTables remain readable. If the file is absent a new key
//! is generated and saved.

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use hkdf::Hkdf;
use rand::RngCore;
use sha2::Sha256;
use std::path::Path;

use crate::SecurityError;

/// Size of the AES-256-GCM key (32 bytes).
const KEY_SIZE: usize = 32;
/// Size of the GCM nonce (12 bytes).
const NONCE_SIZE: usize = 12;
/// Size of the GCM auth tag (16 bytes).
const TAG_SIZE: usize = 16;

/// Encryption manager for data-at-rest.
pub struct EncryptionManager {
    master_key: [u8; KEY_SIZE],
    enabled: bool,
}

impl EncryptionManager {
    /// Create an encryption manager with the given master key.
    pub fn new(master_key: [u8; KEY_SIZE]) -> Self {
        Self {
            master_key,
            enabled: true,
        }
    }

    /// Create a disabled encryption manager (plaintext mode).
    pub fn disabled() -> Self {
        Self {
            master_key: [0u8; KEY_SIZE],
            enabled: false,
        }
    }

    /// Generate a random master key.
    pub fn generate_key() -> [u8; KEY_SIZE] {
        let mut key = [0u8; KEY_SIZE];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }

    /// Load the master key from `key_path`, or generate and persist a new one.
    ///
    /// The key file is created with restrictive permissions (0600 on Unix) so
    /// only the process owner can read it. On Windows the file is created
    /// normally — consider using DPAPI or a secrets manager in production.
    pub fn load_or_generate(key_path: &Path) -> Result<Self, SecurityError> {
        if key_path.exists() {
            let bytes = std::fs::read(key_path).map_err(SecurityError::Io)?;
            if bytes.len() != KEY_SIZE {
                return Err(SecurityError::KeyDerivation(format!(
                    "master key file has wrong length: {} (expected {KEY_SIZE})",
                    bytes.len()
                )));
            }
            let mut key = [0u8; KEY_SIZE];
            key.copy_from_slice(&bytes);
            tracing::info!(path = %key_path.display(), "loaded existing master key");
            return Ok(Self::new(key));
        }

        // Generate a new key and persist it
        let key = Self::generate_key();

        if let Some(parent) = key_path.parent() {
            std::fs::create_dir_all(parent).map_err(SecurityError::Io)?;
        }

        // Write key file
        std::fs::write(key_path, key).map_err(SecurityError::Io)?;

        // Restrict permissions to owner-read-only on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(key_path, perms).map_err(SecurityError::Io)?;
        }

        tracing::info!(path = %key_path.display(), "generated and persisted new master key");
        Ok(Self::new(key))
    }

    /// Whether encryption is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Derive a per-block key from the master key and a block identifier.
    ///
    /// Uses HKDF-SHA256 for key derivation, ensuring that each block
    /// has a unique encryption key even with the same master key.
    fn derive_block_key(&self, block_id: &[u8]) -> Result<[u8; KEY_SIZE], SecurityError> {
        let hk = Hkdf::<Sha256>::new(Some(block_id), &self.master_key);
        let mut okm = [0u8; KEY_SIZE];
        hk.expand(b"plexus-block-key", &mut okm)
            .map_err(|e| SecurityError::KeyDerivation(e.to_string()))?;
        Ok(okm)
    }

    /// Encrypt a data block.
    ///
    /// Returns: [nonce (12 bytes)][ciphertext][tag (16 bytes)]
    pub fn encrypt_block(
        &self,
        plaintext: &[u8],
        block_id: &[u8],
    ) -> Result<Vec<u8>, SecurityError> {
        if !self.enabled {
            return Ok(plaintext.to_vec());
        }

        let block_key = self.derive_block_key(block_id)?;
        let key = Key::<Aes256Gcm>::from_slice(&block_key);
        let cipher = Aes256Gcm::new(key);

        // Generate random nonce
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| SecurityError::Encryption(e.to_string()))?;

        // Prepend nonce to ciphertext
        let mut output = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);

        Ok(output)
    }

    /// Decrypt a data block.
    ///
    /// Input format: [nonce (12 bytes)][ciphertext][tag (16 bytes)]
    pub fn decrypt_block(
        &self,
        encrypted: &[u8],
        block_id: &[u8],
    ) -> Result<Vec<u8>, SecurityError> {
        if !self.enabled {
            return Ok(encrypted.to_vec());
        }

        if encrypted.len() < NONCE_SIZE + TAG_SIZE {
            return Err(SecurityError::Encryption("encrypted data too short".into()));
        }

        let block_key = self.derive_block_key(block_id)?;
        let key = Key::<Aes256Gcm>::from_slice(&block_key);
        let cipher = Aes256Gcm::new(key);

        let nonce = Nonce::from_slice(&encrypted[..NONCE_SIZE]);
        let ciphertext = &encrypted[NONCE_SIZE..];

        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| SecurityError::Encryption(format!("decryption failed: {e}")))?;

        Ok(plaintext)
    }

    /// Size overhead per encrypted block (nonce + auth tag).
    pub const fn overhead() -> usize {
        NONCE_SIZE + TAG_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = EncryptionManager::generate_key();
        let mgr = EncryptionManager::new(key);

        let plaintext = b"hello, plexusdb! this is secret data.";
        let block_id = b"sst_001_block_0";

        let encrypted = mgr.encrypt_block(plaintext, block_id).unwrap();
        assert_ne!(&encrypted, plaintext);

        let decrypted = mgr.decrypt_block(&encrypted, block_id).unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[test]
    fn test_different_block_ids_produce_different_ciphertext() {
        let key = EncryptionManager::generate_key();
        let mgr = EncryptionManager::new(key);

        let plaintext = b"same data";
        let enc1 = mgr.encrypt_block(plaintext, b"block_1").unwrap();
        let enc2 = mgr.encrypt_block(plaintext, b"block_2").unwrap();

        // Due to random nonce, even same plaintext+key produces different output
        assert_ne!(enc1, enc2);
    }

    #[test]
    fn test_disabled_mode_passthrough() {
        let mgr = EncryptionManager::disabled();
        let data = b"unencrypted data";

        let result = mgr.encrypt_block(data, b"any").unwrap();
        assert_eq!(&result, data);

        let decrypted = mgr.decrypt_block(&result, b"any").unwrap();
        assert_eq!(&decrypted, data);
    }

    #[test]
    fn test_tampered_data_fails() {
        let key = EncryptionManager::generate_key();
        let mgr = EncryptionManager::new(key);

        let encrypted = mgr.encrypt_block(b"secret", b"block").unwrap();

        // Tamper with the ciphertext
        let mut tampered = encrypted.clone();
        if let Some(byte) = tampered.last_mut() {
            *byte ^= 0xFF;
        }

        let result = mgr.decrypt_block(&tampered, b"block");
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_block_id_fails() {
        let key = EncryptionManager::generate_key();
        let mgr = EncryptionManager::new(key);

        let encrypted = mgr.encrypt_block(b"secret data", b"block_1").unwrap();

        // Try decrypting with wrong block_id — should fail because
        // per-block key derivation produces a different key
        let result = mgr.decrypt_block(&encrypted, b"block_2");
        assert!(
            result.is_err(),
            "decryption with wrong block_id should fail"
        );
    }

    #[test]
    fn test_encrypted_data_too_short() {
        let key = EncryptionManager::generate_key();
        let mgr = EncryptionManager::new(key);

        // Data shorter than nonce + tag minimum
        let result = mgr.decrypt_block(b"short", b"block");
        assert!(result.is_err());
    }

    #[test]
    fn test_encrypt_empty_plaintext() {
        let key = EncryptionManager::generate_key();
        let mgr = EncryptionManager::new(key);

        let encrypted = mgr.encrypt_block(b"", b"block_0").unwrap();
        let decrypted = mgr.decrypt_block(&encrypted, b"block_0").unwrap();
        assert!(decrypted.is_empty());
    }
}

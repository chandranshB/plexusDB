//! User authentication and authorization.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// User role for authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserRole {
    Admin,
    ReadWrite,
    ReadOnly,
}

impl std::fmt::Display for UserRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserRole::Admin => write!(f, "admin"),
            UserRole::ReadWrite => write!(f, "read_write"),
            UserRole::ReadOnly => write!(f, "read_only"),
        }
    }
}

/// Hash a password with a salt.
pub fn hash_password(password: &str, salt: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update(password.as_bytes());
    hasher.update(b"plexusdb-auth-v1");
    let result = hasher.finalize();
    format!("{result:x}")
}

/// Verify a password against a hash.
pub fn verify_password(password: &str, salt: &str, expected_hash: &str) -> bool {
    hash_password(password, salt) == expected_hash
}

/// Parse a connection string: plexus://[user]:[pass]@[host]:[port]
pub fn parse_connection_string(conn_str: &str) -> Result<ConnectionInfo, &'static str> {
    let s = conn_str
        .strip_prefix("plexus://")
        .ok_or("connection string must start with plexus://")?;

    let (auth, host_port) = if let Some(at_pos) = s.rfind('@') {
        let auth = &s[..at_pos];
        let host = &s[at_pos + 1..];
        (Some(auth), host)
    } else {
        (None, s)
    };

    let (username, password) = if let Some(auth) = auth {
        if let Some(colon) = auth.find(':') {
            (Some(auth[..colon].to_string()), Some(auth[colon + 1..].to_string()))
        } else {
            (Some(auth.to_string()), None)
        }
    } else {
        (None, None)
    };

    let (host, port) = if let Some(colon) = host_port.rfind(':') {
        let port = host_port[colon + 1..]
            .parse::<u16>()
            .map_err(|_| "invalid port number")?;
        (host_port[..colon].to_string(), port)
    } else {
        (host_port.to_string(), 9090)
    };

    Ok(ConnectionInfo {
        host,
        port,
        username,
        password,
    })
}

/// Parsed connection information.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl ConnectionInfo {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_connection_string() {
        let info = parse_connection_string("plexus://admin:secret@192.168.1.1:9090").unwrap();
        assert_eq!(info.host, "192.168.1.1");
        assert_eq!(info.port, 9090);
        assert_eq!(info.username, Some("admin".into()));
        assert_eq!(info.password, Some("secret".into()));
    }

    #[test]
    fn test_parse_no_auth() {
        let info = parse_connection_string("plexus://localhost:8080").unwrap();
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 8080);
        assert!(info.username.is_none());
    }

    #[test]
    fn test_password_hashing() {
        let hash = hash_password("password123", "random_salt");
        assert!(verify_password("password123", "random_salt", &hash));
        assert!(!verify_password("wrong_pass", "random_salt", &hash));
    }

    #[test]
    fn test_password_hash_deterministic() {
        let hash1 = hash_password("pass", "salt");
        let hash2 = hash_password("pass", "salt");
        assert_eq!(hash1, hash2, "same input should always produce same hash");
    }

    #[test]
    fn test_different_salt_different_hash() {
        let hash1 = hash_password("pass", "salt_a");
        let hash2 = hash_password("pass", "salt_b");
        assert_ne!(hash1, hash2, "different salts should produce different hashes");
    }

    #[test]
    fn test_parse_invalid_prefix() {
        let result = parse_connection_string("http://localhost:9090");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_default_port() {
        let info = parse_connection_string("plexus://localhost").unwrap();
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 9090); // default
        assert!(info.username.is_none());
    }

    #[test]
    fn test_parse_bad_port() {
        let result = parse_connection_string("plexus://localhost:notanumber");
        assert!(result.is_err());
    }
}

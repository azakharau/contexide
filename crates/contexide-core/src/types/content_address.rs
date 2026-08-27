use serde::{Deserialize, Serialize};

/// Minimal content-address info for assets.
/// Not a DB entity by itself — carried in API/event payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentAddress {
    /// Hash algorithm identifier, e.g. "blake3-256"
    pub algo: String,
    /// Lowercase hex of the content hash
    pub hash_hex: String,
    /// Original payload size in bytes
    pub size_bytes: u64,
    /// MIME type string, e.g. "application/pdf"
    pub mime: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_address_roundtrip() {
        let ca = ContentAddress {
            algo: "blake3-256".to_string(),
            hash_hex: "abc123".into(),
            size_bytes: 42,
            mime: "application/pdf".into(),
        };
        let s = serde_json::to_string(&ca).unwrap();
        let back: ContentAddress = serde_json::from_str(&s).unwrap();
        assert_eq!(back, ca);
    }
}

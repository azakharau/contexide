//! `profiles` — stable hashing for chunking/cleaning profiles.
//!
//! The goal is to ensure that logically equivalent JSON profiles map to the same
//! `profile_hash`, regardless of key order or whitespace. We do that by:
//! 1) canonicalizing JSON (sort object keys recursively, compact encode),
//! 2) hashing the result with BLAKE3-256 (hex).

use serde_json::Value;

/// Return canonical (compact, sorted-keys) JSON string of a profile.
pub fn profile_canonical_json(v: &Value) -> String {
    crate::utils::canon::canonicalize_value(v)
}

/// Compute BLAKE3-hex over the canonical JSON of a profile.
pub fn profile_hash(v: &Value) -> String {
    let canon = profile_canonical_json(v);
    crate::utils::hashing::blake3_hex_str(&canon)
}

/// Parse a JSON string, canonicalize, then return the BLAKE3-hex hash.
pub fn profile_hash_from_str(s: &str) -> Result<String, serde_json::Error> {
    let canon = crate::utils::canon::canonicalize_str(s)?;
    Ok(crate::utils::hashing::blake3_hex_str(&canon))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn equal_semantics_equal_hash() {
        let a = json!({
            "tokenizer": "tiktoken:cl100k_base",
            "window": 800,
            "overlap": 120,
            "rules": { "normalize_unicode": true, "collapse_ws": true }
        });
        let b = json!({
            "rules": { "collapse_ws": true, "normalize_unicode": true },
            "overlap": 120,
            "tokenizer": "tiktoken:cl100k_base",
            "window": 800
        });
        assert_eq!(profile_canonical_json(&a), profile_canonical_json(&b));
        assert_eq!(profile_hash(&a), profile_hash(&b));
    }

    #[test]
    fn different_semantics_different_hash() {
        let a = json!({ "window": 800, "overlap": 120 });
        let b = json!({ "window": 900, "overlap": 120 });
        assert_ne!(profile_hash(&a), profile_hash(&b));
    }

    #[test]
    fn from_str_is_consistent() {
        let s = r#"{ "b": 2, "a": 1 }"#;
        let h1 = profile_hash_from_str(s).unwrap();
        let v: Value = serde_json::from_str(s).unwrap();
        let h2 = profile_hash(&v);
        assert_eq!(h1, h2);
    }
}

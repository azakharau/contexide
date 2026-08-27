//! `errors` — common error type and Result alias for the core crate.
//!
//! Keep it lightweight and dependency-friendly. We intentionally avoid DB- or
//! feature-specific variants at this stage to keep `core` reusable.

use thiserror::Error;

/// Canonical result type for `core`.
pub type Result<T> = std::result::Result<T, Error>;

/// Unified error enum used across the crate and its dependents.
#[derive(Debug, Error)]
pub enum Error {
    /// Wrapper for standard I/O errors.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Wrapper for JSON (de)serialization errors.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A simple typed not-found signal for common lookups.
    #[error("not found: {0}")]
    NotFound(&'static str),

    #[cfg(feature = "db")]
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    /// Fallback for any other error type (preserves backtrace with `anyhow`).
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_formats_are_sane() {
        let e = Error::NotFound("document");
        let s = e.to_string();
        assert!(s.contains("not found"));
        assert!(s.contains("document"));
    }

    #[test]
    fn from_conversions_work() {
        // std::io::Error
        let ioe = std::fs::File::open("__definitely_missing__").err().unwrap();
        let _: Error = ioe.into();

        // serde_json::Error
        let je = serde_json::from_str::<serde_json::Value>("not json")
            .err()
            .unwrap();
        let _: Error = je.into();
    }

    #[test]
    fn anyhow_is_transparent() {
        let a = anyhow::anyhow!("boom");
        let e: Error = a.into();
        assert!(e.to_string().contains("boom"));
    }
}

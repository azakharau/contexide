#![allow(dead_code)]

pub(crate) mod html;
pub(crate) mod markdown;
pub(crate) mod text;
pub(crate) mod traits;

/// Result of normalization with a few handy stats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedText {
    pub text: String,
    /// Input byte length.
    pub bytes_in: usize,
    /// Output byte length.
    pub bytes_out: usize,
    /// Quick flag: did we actually change anything.
    pub changed: bool,
    /// Rough line count after normalization.
    pub lines: usize,
    /// Rough word count after normalization (split_whitespace).
    pub words: usize,
}

impl NormalizedText {
    /// Convenience accessor.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.text
    }
}

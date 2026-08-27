//! `traits` — contracts for pipeline stages and shared data structs.
//!
//! This module defines the “language” between workers: what extractors/cleaners/
//! chunkers/vector-index implementations must provide, and which data structures
//! are exchanged across the pipeline. Concrete implementations live in other crates.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{AssetId, ChunkId};
use crate::types::BlockModality;

/// Stable set of extractor kinds (used for logs/metrics/idempotency keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtractorKind {
    Pdf,
    Html,
    Docx,
    ImageOcr,
    AudioAsr,
    Video,
    File,
}

impl ExtractorKind {
    /// Stable kebab-case identifier used in idempotency keys and metadata.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pdf => "pdf",
            Self::Html => "html",
            Self::Docx => "docx",
            Self::ImageOcr => "image-ocr",
            Self::AudioAsr => "audio-asr",
            Self::Video => "video",
            Self::File => "file",
        }
    }
}

/// Output unit of extraction for a single asset (“block” granularity).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockOut {
    pub asset_id: AssetId,
    pub modality: BlockModality,
    /// Logical order within the source document (page/section, then intra-page order).
    pub order_idx: i32,
    /// Plain text content if available (Text/ASR/OCR). `None` for non-text modalities.
    pub text: Option<String>,
    /// Arbitrary metadata (page number, bbox, section titles, etc.).
    pub meta: Value,
}

/// Span of a chunk within the original block text.
/// Useful for deterministic reassembly and precise source attribution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkSpan {
    pub start_char: usize,
    pub end_char: usize,
    pub token_count: usize,
    /// Number of characters overlapped with the previous chunk.
    pub overlap_from_prev: usize,
}

/// Contract for content extractors (PDF/HTML/DOCX/OCR/ASR/etc.).
#[async_trait::async_trait]
pub trait Extractor {
    /// Stable extractor kind identifier.
    fn kind(&self) -> ExtractorKind;
    /// Implementation version (e.g. semantic version like "1.0.0").
    fn version(&self) -> &'static str;

    /// Whether this extractor supports the given MIME type.
    async fn supports(&self, mime: &str) -> bool;

    /// Extract blocks from the given asset.
    async fn extract(&self, asset: &AssetId) -> anyhow::Result<Vec<BlockOut>>;
}

/// Contract for text cleaners/normalizers.
#[async_trait::async_trait]
pub trait Cleaner {
    /// Clean/normalize text; `lang` may guide language-specific rules.
    async fn clean(&self, lang: Option<&str>, text: &str) -> String;
}

/// Item used when upserting embeddings into a vector index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingUpsert {
    pub chunk_id: ChunkId,
    /// Embedding vector; length is validated at runtime (e.g., 1024).
    pub vec: Vec<f32>,
}

/// Single search hit with similarity score (higher is better).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredHit {
    pub chunk_id: ChunkId,
    pub score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extractor_kind_as_str_is_stable() {
        assert_eq!(ExtractorKind::Pdf.as_str(), "pdf");
        assert_eq!(ExtractorKind::ImageOcr.as_str(), "image-ocr");
        assert_eq!(ExtractorKind::AudioAsr.as_str(), "audio-asr");
    }
}

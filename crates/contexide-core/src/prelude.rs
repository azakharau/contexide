//! Canonical prelude for `contexide_core`.
//!
//! Goal: ergonomic imports for the most common types/traits used across workers
//! and services, without over-polluting the namespace. Prefer explicit imports
//! elsewhere; this prelude is opt-in via `use contexide_core::prelude::*;`.

/// ID newtypes over `Uuid` (time-ordered v7 via `::new()`).
pub use crate::ids::{
    AssetId, BlockId, ChunkId, ChunkSetId, DagId, DagRunId, DocumentId, EmbeddingSetId, JobId,
    TaskId, TaskRunId, TenantId,
};

/// Shared domain enums and small value objects.
pub use crate::types::{AssetSource, BlockModality, ContentAddress, DocumentStatus, Stage};

pub use crate::blob::{BlobStore, ObjectMeta as BlobObjectMeta};
pub use crate::chunker::{ChunkInput, ChunkPiece, ChunkSpec, Chunker, Tokenizer};
pub use crate::embeddings::{Embedding, EmbeddingsProvider, ModelInfo};
pub use crate::extractor::{AssetInput, ExtractContext, ExtractedBlock, Extractor};
pub use crate::message_bus::{IncomingMessage, MessageBus};
pub use crate::storage::entities::*;
pub use crate::storage::traits::*;
/// Pipeline contracts and shared structs between stages.
pub use crate::traits::{BlockOut, ChunkSpan, Cleaner, EmbeddingUpsert, ExtractorKind, ScoredHit};
pub use crate::vector::{FilterExpr, HnswParams, Metric, SearchHit, VPoint, VPointId, VectorStore};

/// Event envelope and standard NATS subjects.
pub use crate::events::{Event, subjects};

/// Generic utilities: canonical JSON and BLAKE3 hashing.
pub use crate::utils::{self, canon, hashing};

/// Idempotency key builders for pipeline stages.
pub use crate::idempo;

/// Crate-wide error and result alias.
pub use crate::errors::{Error, Result};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prelude_works_for_basic_use() {
        // IDs
        let _doc = DocumentId::new();
        let asset = AssetId::new();

        // Types
        let _stage = Stage::Extract;
        let _modality = BlockModality::Text;

        // Utils
        let canon = canon::canonicalize_str(r#"{ "b":2, "a":1 }"#).unwrap();
        let _hash = hashing::blake3_hex_str(&canon);

        // Idempotency
        let _key = idempo::fetch(asset, "1.0.0");

        // Events
        let ev = Event::new(
            JobId::new(),
            TenantId::new(),
            Stage::Fetch,
            "fetch:key",
            serde_json::json!({"ok": true}),
        );
        let _ = serde_json::to_string(&ev).unwrap();

        // Traits (compile-time check for names)
        let _k = ExtractorKind::Pdf.as_str();
    }
}

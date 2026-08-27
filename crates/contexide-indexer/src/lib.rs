// crates/contexide-indexer/src/lib.rs
//! Indexing facade: chunks -> embeddings -> vector storage.
//!
//! Goals (MVP):
//! - Keep I/O surfaces behind traits: `EmbeddingsProvider` and `VectorSink`.
//! - Provide a tiny `Indexer` that batches texts, gets embeddings and upserts them.
//! - Be agnostic to the actual vector DB (Qdrant, pgvector, Milvus, ...).
//!
//! Design notes:
//! - This crate depends on `contexide-embeddings` for the provider trait.
//! - `VectorSink` is defined here to decouple from concrete vector backends.
//!   You can add adapters in separate modules/crates to wrap Qdrant/pgvector/etc.
//! - We use trait objects (`dyn`) for I/O/pluggable layers (vtable) per our project rules.

use std::sync::Arc;

use anyhow::anyhow;
use contexide_core::errors::{Error, Result};
use contexide_core::prelude::{ChunkId, DocumentId, TenantId};
use serde_json::{Map as JsonMap, Value as JsonValue};
use uuid::Uuid;

use contexide_core::embeddings::EmbeddingsProvider;
use contexide_embeddings::ModelInfo;

/// Minimal information required to index one chunk.
#[derive(Debug, Clone)]
pub struct ChunkForIndex {
    pub chunk_id: ChunkId,
    pub text: String,
    /// Optional payload to persist alongside the vector (filters/metadata).
    /// Typical keys: tenant_id, document_id, chunk_id, section, page, etc.
    pub payload: Option<JsonMap<String, JsonValue>>,
}

impl ChunkForIndex {
    pub fn new(chunk_id: ChunkId, text: impl Into<String>) -> Self {
        Self {
            chunk_id,
            text: text.into(),
            payload: None,
        }
    }
}

/// Batch request to index chunks for a document/tenant.
#[derive(Debug, Clone)]
pub struct IndexRequest {
    pub tenant_id: TenantId,
    pub document_id: DocumentId,
    pub collection: String,
    pub inputs: Vec<ChunkForIndex>,
}

/// Outcome summary for an indexing operation.
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    pub requested: usize,
    pub embedded: usize,
    pub upserted: usize,
    pub dim: usize,
}

/// Abstract sink for vector databases.
///
/// Implement this for Qdrant, pgvector, Milvus, etc. Keep the surface small
/// and domain-oriented (collection + points + payload).
#[async_trait::async_trait]
pub trait VectorSink: Send + Sync {
    /// Ensure the collection exists with a given dimension. If it exists,
    /// validate the dimension (implementations may no-op or error on mismatch).
    async fn ensure_collection(&self, collection: &str, dim: usize) -> Result<()>;

    /// Upsert a batch of points into the collection.
    async fn upsert(&self, collection: &str, points: Vec<VectorPoint>) -> Result<usize>;
}

/// One vector point with a stable id and optional payload.
#[derive(Debug, Clone)]
pub struct VectorPoint {
    pub id: Uuid,
    pub vector: Vec<f32>,
    pub payload: JsonMap<String, JsonValue>,
}

/// Indexer contract.
#[async_trait::async_trait]
pub trait Indexer: Send + Sync {
    async fn index(&self, req: IndexRequest) -> Result<IndexStats>;
}

/// Simple indexer implementation:
/// - Uses `EmbeddingsProvider` to embed texts,
/// - Ensures collection and upserts vectors via `VectorSink`.
pub struct SimpleIndexer {
    provider: Arc<dyn EmbeddingsProvider>,
    sink: Arc<dyn VectorSink>,
}

impl SimpleIndexer {
    pub fn new(provider: Arc<dyn EmbeddingsProvider>, sink: Arc<dyn VectorSink>) -> Self {
        Self { provider, sink }
    }

    #[inline]
    fn build_payload(
        base: Option<&JsonMap<String, JsonValue>>,
        tenant_id: TenantId,
        document_id: DocumentId,
        chunk_id: ChunkId,
    ) -> JsonMap<String, JsonValue> {
        let mut p = JsonMap::new();
        p.insert(
            "tenant_id".into(),
            JsonValue::String(tenant_id.0.to_string()),
        );
        p.insert(
            "document_id".into(),
            JsonValue::String(document_id.0.to_string()),
        );
        p.insert("chunk_id".into(), JsonValue::String(chunk_id.0.to_string()));
        if let Some(extra) = base {
            for (k, v) in extra.iter() {
                // do not overwrite the canonical ids
                if !matches!(k.as_str(), "tenant_id" | "document_id" | "chunk_id") {
                    p.insert(k.clone(), v.clone());
                }
            }
        }
        p
    }
}

#[async_trait::async_trait]
impl Indexer for SimpleIndexer {
    async fn index(&self, req: IndexRequest) -> Result<IndexStats> {
        let requested = req.inputs.len();
        if requested == 0 {
            return Ok(IndexStats::default());
        }

        // 1) Ensure collection exists with expected dimension.
        let ModelInfo { dims, .. } = self.provider.info();
        if dims == 0 {
            return Err(Error::Other(anyhow!(
                "provider dimension is unknown (dim=0)"
            )));
        }
        self.sink.ensure_collection(&req.collection, dims).await?;

        // 2) Prepare batch of texts (&str) for provider.
        let texts: Vec<&str> = req.inputs.iter().map(|c| c.text.as_str()).collect();

        // 3) Embed.
        let vectors = self.provider.embed(&texts).await?;
        if vectors.len() != requested {
            return Err(Error::Other(anyhow!(
                "embed_batch returned {} vectors for {} inputs",
                vectors.len(),
                requested
            )));
        }
        // Validate dimensions early.
        for (i, v) in vectors.iter().enumerate() {
            if v.vector.len() != dims {
                return Err(Error::Other(anyhow!(
                    "vector {} has dim {}, expected {}",
                    i,
                    v.vector.len(),
                    dims
                )));
            }
        }

        // 4) Map to VectorPoint (stable ids = chunk_id).
        let mut points = Vec::with_capacity(requested);
        for (c, vec) in req.inputs.iter().zip(vectors.into_iter()) {
            points.push(VectorPoint {
                id: c.chunk_id.0, // stable UUID per chunk
                vector: vec.vector,
                payload: Self::build_payload(
                    c.payload.as_ref(),
                    req.tenant_id,
                    req.document_id,
                    c.chunk_id,
                ),
            });
        }

        // 5) Upsert in sink.
        let upserted = self.sink.upsert(&req.collection, points).await?;

        Ok(IndexStats {
            requested,
            embedded: requested,
            upserted,
            dim: dims,
        })
    }
}

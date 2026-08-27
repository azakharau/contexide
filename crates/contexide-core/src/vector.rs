//! Vector store contracts and DTOs (transport-agnostic).
//!
//! Concrete backends (Qdrant, pgvector, Milvus, etc.) live in adapter crates.

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use uuid::Uuid;

use crate::errors::Result;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Metric {
    Cosine,
    Dot,
    Euclid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HnswParams {
    pub m: u64,
    pub ef_construct: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum VPointId {
    Uuid(Uuid),
    Integer(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VPoint {
    pub id: VPointId,
    pub vector: Vec<f32>,
    pub payload: Option<JsonMap<String, JsonValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpr {
    Eq { key: String, value: JsonValue },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub id: VPointId,
    pub score: f32,
    pub payload: Option<JsonMap<String, JsonValue>>,
}

#[async_trait::async_trait]
pub trait VectorStore: Send + Sync {
    async fn ensure_collection(
        &self,
        name: &str,
        dim: usize,
        metric: Metric,
        hnsw: Option<HnswParams>,
    ) -> Result<()>;

    async fn drop_collection(&self, name: &str) -> Result<()>;

    async fn upsert(&self, collection: &str, points: Vec<VPoint>) -> Result<usize>;

    async fn search(
        &self,
        collection: &str,
        query: &[f32],
        top_k: usize,
        filter: Option<FilterExpr>,
        with_payload: bool,
    ) -> Result<Vec<SearchHit>>;

    async fn delete_by_ids(&self, collection: &str, ids: &[VPointId]) -> Result<usize>;

    async fn count(&self, collection: &str, filter: Option<FilterExpr>) -> Result<u64>;
}

//! Workflow task messaging model for `contexide-messaging`.
//!
//! Transport-level contracts for executor ↔ workers:
//! - task domains/kinds
//! - request/result payloads
//! - generic envelope carrying workflow metadata
//!
//! IDs in this module are plain `Uuid` (workflow) plus strongly typed
//! domain IDs from `contexide-core`.

use std::collections::HashMap;

use crate::prelude::{AssetId, BlockId, ChunkId, ChunkSetId, DocumentId, EmbeddingSetId, TenantId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Workflow-level domain of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskDomain {
    Extract,
    Normalize,
    Chunk,
    Embed,
    Index,
}

/// High-level kind of a task within a domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    ExtractText,
    ExtractPdf,
    ExtractFile,
    NormalizeBlocks,
    ChunkBlocks,
    EmbedChunks,
    IndexChunks,
}

/// Generic message envelope used for task requests and results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnvelope<P> {
    pub schema: String,
    pub domain: TaskDomain,
    pub kind: TaskKind,
    pub tenant_id: TenantId,
    pub dag_run_id: Option<Uuid>,
    pub task_id: Uuid,
    pub task_run_id: Uuid,
    pub attempt_no: u32,
    pub priority: i32,
    pub created_at: String,
    pub trace_id: Option<String>,
    pub payload: P,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractRequestPayload {
    pub document_id: DocumentId,
    pub asset_id: AssetId,
    pub origin_uri: Option<String>,
    pub mime: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractResultPayload {
    pub success: bool,
    pub block_ids: Vec<BlockId>,
    pub error_message: Option<String>,
    pub metrics: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizeRequestPayload {
    pub document_id: DocumentId,
    pub block_ids: Vec<BlockId>,
    pub profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizeResultPayload {
    pub success: bool,
    pub normalized_block_ids: Vec<BlockId>,
    pub error_message: Option<String>,
    pub metrics: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRequestPayload {
    pub document_id: DocumentId,
    pub block_ids: Vec<BlockId>,
    pub chunk_set_id: Option<ChunkSetId>,
    pub profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkResultPayload {
    pub success: bool,
    pub chunk_set_id: ChunkSetId,
    pub chunk_ids: Vec<ChunkId>,
    pub error_message: Option<String>,
    pub metrics: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbedRequestPayload {
    pub document_id: DocumentId,
    pub chunk_ids: Vec<ChunkId>,
    pub embedding_set_id: EmbeddingSetId,
    pub profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbedResultPayload {
    pub success: bool,
    pub embedding_set_id: EmbeddingSetId,
    pub embedded_chunk_ids: Vec<ChunkId>,
    pub error_message: Option<String>,
    pub metrics: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRequestPayload {
    pub document_id: DocumentId,
    pub chunk_set_id: Option<ChunkSetId>,
    pub index_profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResultPayload {
    pub success: bool,
    pub updated_points: u64,
    pub error_message: Option<String>,
    pub metrics: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "domain", content = "data", rename_all = "snake_case")]
pub enum WorkflowRequestPayload {
    Extract(ExtractRequestPayload),
    Normalize(NormalizeRequestPayload),
    Chunk(ChunkRequestPayload),
    Embed(EmbedRequestPayload),
    Index(IndexRequestPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "domain", content = "data", rename_all = "snake_case")]
pub enum WorkflowResultPayload {
    Extract(ExtractResultPayload),
    Normalize(NormalizeResultPayload),
    Chunk(ChunkResultPayload),
    Embed(EmbedResultPayload),
    Index(IndexResultPayload),
}

pub type WorkflowRequestMessage = TaskEnvelope<WorkflowRequestPayload>;
pub type WorkflowResultMessage = TaskEnvelope<WorkflowResultPayload>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip_json() {
        let payload = WorkflowRequestPayload::Extract(ExtractRequestPayload {
            document_id: DocumentId::new(),
            asset_id: AssetId::new(),
            origin_uri: Some("s3://bucket/key".to_string()),
            mime: "application/pdf".to_string(),
        });

        let env = WorkflowRequestMessage {
            schema: "contexide.workflow.v1".to_string(),
            domain: TaskDomain::Extract,
            kind: TaskKind::ExtractPdf,
            tenant_id: TenantId::new(),
            dag_run_id: Some(Uuid::now_v7()),
            task_id: Uuid::now_v7(),
            task_run_id: Uuid::now_v7(),
            attempt_no: 1,
            priority: 0,
            created_at: "2025-01-01T00:00:00Z".to_string(),
            trace_id: Some("trace-123".to_string()),
            payload,
        };

        let json = serde_json::to_string(&env).expect("serialize");
        let back: WorkflowRequestMessage = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back.schema, "contexide.workflow.v1");
        assert!(matches!(back.payload, WorkflowRequestPayload::Extract(_)));
    }
}

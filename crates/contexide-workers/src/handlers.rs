//! Minimal domain handlers for worker binaries.
//!
//! These handlers currently act as thin validation and status emitters so
//! that the control plane can progress through the workflow. Real data plane
//! work (fetching assets, chunking, embedding, indexing) should replace the
//! placeholders over time.

use std::sync::Arc;

use contexide_core::ids::{ChunkId, ChunkSetId};
use contexide_messaging_nats::{TaskResultMeta, TaskRunOutcome, WorkerRequest};
use contexide_worker_runtime::{DynWorkerHandler, WorkerContext};
use serde_json::{Map, Value};

/// Create a handler for a given worker kind.
pub fn handler_for(kind: &str) -> DynWorkerHandler {
    Arc::new(GenericHandler {
        kind: kind.to_string(),
    })
}

struct GenericHandler {
    kind: String,
}

#[async_trait::async_trait]
impl contexide_worker_runtime::WorkerHandler for GenericHandler {
    async fn handle(
        &self,
        _ctx: &WorkerContext,
        req: WorkerRequest,
    ) -> contexide_core::errors::Result<contexide_messaging_nats::WorkerStatus> {
        // Route by kind; keep behaviour predictable even with unknown payloads.
        let status = match self.kind.as_str() {
            "extractor" => handle_extract(&req),
            "normalizer" => handle_normalize(&req),
            "chunker" => handle_chunk(&req),
            "embedder" => handle_embed(&req),
            "indexer" => handle_index(&req),
            _ => failure_status(&req, "unsupported_worker_kind", "Unsupported worker kind"),
        };

        Ok(status)
    }
}

fn base_status(req: &WorkerRequest) -> contexide_messaging_nats::WorkerStatus {
    contexide_messaging_nats::WorkerStatus {
        tenant_id: req.tenant_id,
        dag_run_id: req.dag_run_id,
        task_id: req.task_id,
        task_run_id: req.task_run_id,
        kind: req.kind.clone(),
        success: true,
        output: None,
        error: None,
        error_kind: None,
        result_meta: None,
    }
}

fn failure_status(
    req: &WorkerRequest,
    error_kind: &str,
    message: impl Into<String>,
) -> contexide_messaging_nats::WorkerStatus {
    contexide_messaging_nats::WorkerStatus {
        success: false,
        error: Some(message.into()),
        error_kind: Some(error_kind.to_string()),
        result_meta: Some(TaskResultMeta {
            outcome: TaskRunOutcome::PermanentFailure,
            error_code: Some(error_kind.to_string()),
            error_message: None,
        }),
        ..base_status(req)
    }
}

fn handle_extract(req: &WorkerRequest) -> contexide_messaging_nats::WorkerStatus {
    let mut status = base_status(req);
    // MVP: simply acknowledge receipt; future work will call real extractor.
    status.output = Some(Value::Object(Map::from_iter([(
        "block_ids".to_string(),
        Value::Array(Vec::new()),
    )])));
    status
}

fn handle_normalize(req: &WorkerRequest) -> contexide_messaging_nats::WorkerStatus {
    let mut status = base_status(req);
    status.output = Some(Value::Object(Map::from_iter([(
        "normalized".to_string(),
        Value::Bool(true),
    )])));
    status
}

fn handle_chunk(req: &WorkerRequest) -> contexide_messaging_nats::WorkerStatus {
    let mut status = base_status(req);

    // Try to reuse chunk_set_id from payload if provided.
    let chunk_set_id = extract_chunk_set(req).unwrap_or_default();

    let chunk_ids: Vec<ChunkId> = Vec::new();
    let payload = serde_json::json!({
        "chunk_set_ids": [uuid::Uuid::from(chunk_set_id).to_string()],
        "chunk_ids": chunk_ids.iter().map(|c| uuid::Uuid::from(*c).to_string()).collect::<Vec<_>>(),
    });
    status.output = Some(payload);
    status
}

fn handle_embed(req: &WorkerRequest) -> contexide_messaging_nats::WorkerStatus {
    let mut status = base_status(req);
    status.output = Some(serde_json::json!({
        "embedded": true,
    }));
    status
}

fn handle_index(req: &WorkerRequest) -> contexide_messaging_nats::WorkerStatus {
    let mut status = base_status(req);
    status.output = Some(serde_json::json!({
        "indexed": true,
    }));
    status
}

fn extract_chunk_set(req: &WorkerRequest) -> Option<ChunkSetId> {
    req.payload
        .get("chunk_set_id")
        .and_then(Value::as_str)
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
        .map(ChunkSetId::from)
}

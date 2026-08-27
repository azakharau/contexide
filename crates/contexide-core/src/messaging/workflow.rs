//! Workflow-domain message payloads.
//!
//! These types describe *what* we send for workflow orchestration, but not
//! *how* we send it. They are designed to be wrapped in `Envelope<T>` and
//! transported via any messaging backend (NATS JetStream, Kafka, etc.).
//!
//! This module intentionally uses only core IDs and primitives, so that it
//! does not depend on the workflow engine implementation details.

use crate::prelude::TenantId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::messaging::Envelope;

/// Kinds of workflow commands.
///
/// This is intentionally small and generic. More variants can be added later
/// as the workflow engine evolves.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowCommandKind {
    /// Start a new workflow/DAG run using a named workflow profile.
    ///
    /// Example: "default_ingest", "pdf_only", "async_ingest_v2".
    StartDagRun,

    /// Resume an existing DagRun (e.g., after manual intervention or fix).
    ResumeDagRun,
}

/// Command sent *into* the workflow control plane.
///
/// It is typically produced by API/frontends and consumed by the workflow
/// planner/executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowCommand {
    /// Logical name / profile of the workflow to execute.
    ///
    /// This is a high-level identifier, not a physical DAG id.
    pub workflow: String,

    /// Tenant that owns the workflow run.
    pub tenant_id: TenantId,

    /// Command kind (start, resume, etc.).
    pub kind: WorkflowCommandKind,

    /// Opaque input parameters (JSON) understood by the workflow engine.
    ///
    /// This is where you pass things like:
    /// - document URIs
    /// - ingest profiles
    /// - feature flags
    /// - etc.
    pub params: serde_json::Value,

    /// Optional id of a DagRun to resume (only meaningful for `ResumeDagRun`).
    pub dag_run_id: Option<Uuid>,
}

impl WorkflowCommand {
    /// Helper constructor for a "start new DagRun" command.
    pub fn start(
        workflow: impl Into<String>,
        tenant_id: TenantId,
        params: serde_json::Value,
    ) -> Self {
        Self {
            workflow: workflow.into(),
            tenant_id,
            kind: WorkflowCommandKind::StartDagRun,
            params,
            dag_run_id: None,
        }
    }

    /// Helper constructor for "resume existing DagRun".
    pub fn resume(
        workflow: impl Into<String>,
        tenant_id: TenantId,
        dag_run_id: Uuid,
        params: serde_json::Value,
    ) -> Self {
        Self {
            workflow: workflow.into(),
            tenant_id,
            kind: WorkflowCommandKind::ResumeDagRun,
            params,
            dag_run_id: Some(dag_run_id),
        }
    }
}

/// Events emitted *from* the workflow engine.
///
/// These are meant to be consumed by:
/// - internal services (logging, metrics, indexing).
/// - external clients (UI, webhooks).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowEvent {
    /// A new DagRun has been created and accepted for execution.
    DagRunStarted {
        /// Logical name/profile of the workflow.
        workflow: String,
        /// Tenant that owns the run.
        tenant_id: TenantId,
        /// Unique id of the run.
        dag_run_id: Uuid,
    },

    /// DagRun has finished (either successfully or with failure).
    DagRunCompleted {
        workflow: String,
        tenant_id: TenantId,
        dag_run_id: Uuid,
        /// Whether the run completed successfully.
        success: bool,
        /// Optional human-readable error summary.
        error: Option<String>,
    },

    /// Single task inside the DagRun has completed.
    ///
    /// This is useful for fine-grained progress tracking and debugging.
    TaskCompleted {
        workflow: String,
        tenant_id: TenantId,
        dag_run_id: Uuid,
        /// Logical task identifier (e.g. "chunker", "embedder", "indexer").
        task: String,
        /// Unique id of this task within the run (opaque to the outside world).
        task_id: Uuid,
        /// Whether the task completed successfully.
        success: bool,
        /// Optional error description if the task failed.
        error: Option<String>,
    },
}

/// Convenient type alias for envelopes carrying workflow commands.
pub type WorkflowCommandEnvelope = Envelope<WorkflowCommand>;

/// Convenient type alias for envelopes carrying workflow events.
pub type WorkflowEventEnvelope = Envelope<WorkflowEvent>;

/// Result status for a task execution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunOutcome {
    #[default]
    Success,
    TransientFailure,
    PermanentFailure,
}

/// Minimal fields required from workers to drive retries & quotas.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskResultMeta {
    pub outcome: TaskRunOutcome,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

/// Request sent from executor to a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRequest {
    pub tenant_id: TenantId,
    pub dag_run_id: Uuid,
    pub task_id: Uuid,
    pub task_run_id: Uuid,
    pub kind: String,
    pub payload: serde_json::Value,
}

/// Status message emitted by a worker back to the executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatus {
    pub tenant_id: TenantId,
    pub dag_run_id: Uuid,
    pub task_id: Uuid,
    pub task_run_id: Uuid,
    pub kind: String,
    pub success: bool,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub error_kind: Option<String>,
    /// Optional structured result meta to drive retries/quotas.
    #[serde(default)]
    pub result_meta: Option<TaskResultMeta>,
}

/// Subject helper: request channel for a worker kind.
pub fn worker_request_subject(prefix: &str, kind: &str) -> String {
    format!("{}.{}.request", prefix.trim_end_matches('.'), kind)
}

/// Subject helper: done channel for a worker kind.
pub fn worker_done_subject(prefix: &str, kind: &str) -> String {
    format!("{}.{}.done", prefix.trim_end_matches('.'), kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::TenantId;
    use serde_json::json;

    #[test]
    fn start_command_envelope_roundtrip() {
        let tenant = TenantId::new();
        let cmd = WorkflowCommand::start(
            "default_ingest",
            tenant,
            json!({"doc_uri": "s3://bucket/key"}),
        );

        let env = WorkflowCommandEnvelope::new(cmd, 1);
        let json_str = serde_json::to_string(&env).expect("serialize");
        let back: WorkflowCommandEnvelope = serde_json::from_str(&json_str).expect("deserialize");

        assert_eq!(back.payload.workflow, "default_ingest");
        assert_eq!(back.payload.tenant_id, tenant);
        assert!(back.payload.dag_run_id.is_none());
        assert_eq!(back.meta.schema_version, 1);
    }

    #[test]
    fn event_envelope_roundtrip() {
        let tenant = TenantId::new();
        let dag_run_id = Uuid::now_v7();

        let ev = WorkflowEvent::DagRunCompleted {
            workflow: "default_ingest".to_string(),
            tenant_id: tenant,
            dag_run_id,
            success: false,
            error: Some("boom".to_string()),
        };

        let env = WorkflowEventEnvelope::new(ev, 1);
        let json_str = serde_json::to_string(&env).expect("serialize");
        let back: WorkflowEventEnvelope = serde_json::from_str(&json_str).expect("deserialize");

        match back.payload {
            WorkflowEvent::DagRunCompleted {
                workflow,
                tenant_id: t,
                dag_run_id: id,
                success,
                error,
            } => {
                assert_eq!(workflow, "default_ingest");
                assert_eq!(t, tenant);
                assert_eq!(id, dag_run_id);
                assert!(!success);
                assert_eq!(error.as_deref(), Some("boom"));
            }
            _ => panic!("expected DagRunCompleted event"),
        }
    }
}

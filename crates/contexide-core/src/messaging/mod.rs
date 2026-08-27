//! Messaging contracts (transport-agnostic).
//!
//! This module holds envelope types and workflow/worker message payloads.
//! Transport bindings (NATS, Kafka, etc.) must live in adapter crates.

pub mod envelope;
pub mod workflow;
pub mod workflow_tasks;

pub use envelope::{Envelope, MessageMeta};
pub use workflow::{
    TaskResultMeta, TaskRunOutcome, WorkerRequest, WorkerStatus, WorkflowCommand,
    WorkflowCommandEnvelope, WorkflowCommandKind, WorkflowEvent, WorkflowEventEnvelope,
    worker_done_subject, worker_request_subject,
};
pub use workflow_tasks::{
    ChunkRequestPayload, ChunkResultPayload, EmbedRequestPayload, EmbedResultPayload,
    ExtractRequestPayload, ExtractResultPayload, IndexRequestPayload, IndexResultPayload,
    TaskDomain, TaskEnvelope, TaskKind, WorkflowRequestMessage, WorkflowRequestPayload,
    WorkflowResultMessage, WorkflowResultPayload,
};

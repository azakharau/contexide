//! Opinionated workflow profile templates (ingest-only, full RAG index).
//!
//! Profiles build DAG definitions using `contexide-workflow-core` without
//! depending on concrete executors or transports. A caller injects a small
//! `DagStarter` to persist/launch runs.

pub mod dto;
pub mod errors;
pub mod profiles;

pub use dto::{FullRagIndexInput, IngestOnlyInput, WorkflowStartCommon};
pub use errors::ProfileError;
pub use profiles::{
    DagStarter, WorkflowProfileKind, WorkflowProfiles, full_rag_index, ingest_only,
};

//! Workflow core domain model.
//!
//! This crate defines the core types for describing and tracking workflow
//! execution:
//! - DAG, DagRun, Task, TaskRun
//! - status enums and basic transition helpers
//!
//! It is intentionally decoupled from any transport (NATS) or runtime
//! environment (Kubernetes). Other crates (e.g. `contexide-workflow-executor`)
//! will build on top of these types.

pub mod dag;
pub mod limits;
pub mod policy;
pub mod priority;
pub mod retry;
pub mod run;
pub mod state;

pub use dag::{Dag, DagEdge, DagNode, DagValidationError, TaskKind};
pub use limits::{
    AdmissionDecision, DomainLimits, GlobalLimits, LimitsView, TenantLimits, UsageSnapshot,
};
pub use policy::{ExecutionPolicy, QuotaBucketRef, QuotaConfig, RetryPolicy, RetryStrategyKind};
pub use priority::Priority;
pub use retry::{RetryContext, RetryDecision, RetryPolicyKind};
pub use run::{DagRun, Task, TaskRun};
pub use state::{DagRunStatus, TaskRunStatus, TaskStatus};

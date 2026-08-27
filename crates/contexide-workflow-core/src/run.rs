//! Runtime-level domain objects:
//! - `DagRun`  — one execution of a DAG definition
//! - `Task`    — logical node instance within a DagRun
//! - `TaskRun` — concrete execution attempt of a Task
//!
//! These structs are **in-memory** domain DTOs. Persistence (sqlx models) and
//! messaging are defined in other crates.

use serde::{Deserialize, Serialize};

use contexide_core::prelude::{DagRunId, TaskId, TaskRunId, TenantId};

use crate::dag::TaskKind;
use crate::state::{DagRunStatus, TaskRunStatus, TaskStatus};

/// One execution of a DAG definition for a given tenant/input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagRun {
    /// Unique id of this run.
    pub id: DagRunId,
    /// Tenant context (used for quotas, routing, multi-tenancy).
    pub tenant_id: TenantId,
    /// Reference to DAG definition (name + version).
    pub dag_name: String,
    pub dag_version: i32,
    /// Current status of this run.
    pub status: DagRunStatus,
    /// Arbitrary input parameters (JSON document).
    pub params: serde_json::Value,
}

impl DagRun {
    /// Create a new DagRun in `Created` state.
    pub fn new(
        tenant_id: TenantId,
        dag_name: impl Into<String>,
        dag_version: i32,
        params: serde_json::Value,
    ) -> Self {
        Self {
            id: DagRunId::new(),
            tenant_id,
            dag_name: dag_name.into(),
            dag_version,
            status: DagRunStatus::Created,
            params,
        }
    }

    /// Helper to mark the run as running.
    pub fn mark_running(&mut self) {
        if !self.status.is_terminal() {
            self.status = DagRunStatus::Running;
        }
    }

    /// Helper to mark run as finished with a specific terminal state.
    pub fn finish(&mut self, final_status: DagRunStatus) {
        debug_assert!(final_status.is_terminal());
        self.status = final_status;
    }
}

/// Logical task (node instance) inside a DagRun.
///
/// This is what Executor tracks when deciding which work to schedule next.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Task id (unique per DagRun).
    pub id: TaskId,
    /// Owning DagRun.
    pub dag_run_id: DagRunId,
    /// Key of the corresponding node in the DAG (e.g. "chunk", "embed").
    pub node_key: String,
    /// High-level kind (ingest, parse, chunk, embed, ...).
    pub kind: TaskKind,
    /// Current status of the logical task.
    pub status: TaskStatus,
    /// How many attempts have been created so far.
    pub attempt_count: u32,
    /// Opaque input parameters for this task (usually derived from DagRun params
    /// and outputs of upstream tasks).
    pub params: serde_json::Value,
}

impl Task {
    /// New task is created in `Pending` state with zero attempts.
    pub fn new(
        dag_run_id: DagRunId,
        node_key: impl Into<String>,
        kind: TaskKind,
        params: serde_json::Value,
    ) -> Self {
        Self {
            id: TaskId::new(),
            dag_run_id,
            node_key: node_key.into(),
            kind,
            status: TaskStatus::Pending,
            attempt_count: 0,
            params,
        }
    }

    /// Mark task as ready for execution (all dependencies satisfied).
    pub fn mark_ready(&mut self) {
        if matches!(self.status, TaskStatus::Pending) {
            self.status = TaskStatus::Ready;
        }
    }

    /// Mark as running (used when scheduling a new attempt).
    pub fn mark_running(&mut self) {
        if matches!(self.status, TaskStatus::Ready | TaskStatus::Pending) {
            self.status = TaskStatus::Running;
        }
    }

    pub fn mark_success(&mut self) {
        self.status = TaskStatus::Success;
    }

    pub fn mark_failed(&mut self) {
        self.status = TaskStatus::Failed;
    }

    pub fn mark_skipped(&mut self) {
        self.status = TaskStatus::Skipped;
    }
}

/// Concrete execution attempt of a `Task`.
///
/// This is what we tie to at-least-once semantics, NATS messages, and worker
/// pods/containers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRun {
    /// Attempt id.
    pub id: TaskRunId,
    /// Logical task this attempt belongs to.
    pub task_id: TaskId,
    /// Monotonic attempt number (1, 2, 3…).
    pub attempt_no: u32,
    /// Current status of this attempt.
    pub status: TaskRunStatus,
    /// Optional error description / debug info (for failed/aborted attempts).
    pub error: Option<String>,
}

impl TaskRun {
    /// Create a new attempt in `Created` state.
    pub fn new(task_id: TaskId, attempt_no: u32) -> Self {
        Self {
            id: TaskRunId::new(),
            task_id,
            attempt_no,
            status: TaskRunStatus::Created,
            error: None,
        }
    }

    pub fn mark_running(&mut self) {
        if matches!(self.status, TaskRunStatus::Created) {
            self.status = TaskRunStatus::Running;
        }
    }

    pub fn mark_success(&mut self) {
        self.status = TaskRunStatus::Success;
        self.error = None;
    }

    pub fn mark_failed(&mut self, msg: impl Into<String>) {
        self.status = TaskRunStatus::Failed;
        self.error = Some(msg.into());
    }

    pub fn mark_aborted(&mut self, msg: impl Into<String>) {
        self.status = TaskRunStatus::Aborted;
        self.error = Some(msg.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dag_run_lifecycle() {
        let tenant = TenantId::new();
        let mut run = DagRun::new(tenant, "default", 1, serde_json::json!({}));
        assert_eq!(run.status, DagRunStatus::Created);
        run.mark_running();
        assert_eq!(run.status, DagRunStatus::Running);
        run.finish(DagRunStatus::Success);
        assert!(run.status.is_terminal());
    }

    #[test]
    fn task_and_task_run_flow() {
        let tenant = TenantId::new();
        let mut run = DagRun::new(tenant, "default", 1, serde_json::json!({}));
        let mut task = Task::new(run.id, "chunk", TaskKind::Chunk, serde_json::json!({}));

        assert_eq!(task.status, TaskStatus::Pending);
        task.mark_ready();
        assert_eq!(task.status, TaskStatus::Ready);

        let mut tr = TaskRun::new(task.id, 1);
        assert_eq!(tr.status, TaskRunStatus::Created);
        tr.mark_running();
        assert_eq!(tr.status, TaskRunStatus::Running);
        tr.mark_success();
        assert_eq!(tr.status, TaskRunStatus::Success);
    }
}

//! Pure domain helpers for the workflow executor.
//!
//! This module stays free of I/O so it can be exhaustively unit-tested.
//! It provides small value types passed between scheduler and messaging layers
//! plus helper logic for retry decisions and aggregate status derivation.

use contexide_core::ids::{DagRunId, TaskId, TaskRunId, TenantId};
use contexide_workflow_core::{DagRunStatus, TaskStatus};
use serde_json::Value;

/// A task that the scheduler decided is ready to be dispatched to a worker.
#[derive(Debug, Clone)]
pub struct ReadyTask {
    pub tenant_id: TenantId,
    pub dag_run_id: DagRunId,
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    /// Domain kind, e.g. "extractor", "normalizer", "chunker", "embedder", "indexer".
    pub kind: String,
    /// Domain-specific input; may contain references to storage/MinIO objects.
    pub payload: Value,
}

/// Status message coming back from a worker.
#[derive(Debug, Clone)]
pub enum WorkerStatus {
    Success {
        tenant_id: TenantId,
        dag_run_id: DagRunId,
        task_id: TaskId,
        task_run_id: TaskRunId,
        output: Option<Value>,
    },
    Failed {
        tenant_id: TenantId,
        dag_run_id: DagRunId,
        task_id: TaskId,
        task_run_id: TaskRunId,
        error: String,
        /// Optional machine-readable category; may or may not exist in MVP.
        error_kind: Option<String>,
    },
}

/// Simple global retry policy.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
}

impl RetryPolicy {
    pub fn should_retry(&self, attempts: u32) -> bool {
        attempts < self.max_attempts
    }
}

/// Compute DAG run status from task statuses.
///
/// Rules (MVP):
/// - Any `Failed` task → `DagRunStatus::Failed`.
/// - All tasks `Success` or `Skipped` → `DagRunStatus::Success`.
/// - Otherwise → `DagRunStatus::Running`.
pub fn dag_status_from_tasks(statuses: &[TaskStatus]) -> DagRunStatus {
    if statuses.iter().any(|s| matches!(s, TaskStatus::Failed)) {
        return DagRunStatus::Failed;
    }

    if statuses
        .iter()
        .all(|s| matches!(s, TaskStatus::Success | TaskStatus::Skipped))
    {
        return DagRunStatus::Success;
    }

    DagRunStatus::Running
}

/// Helper to check if a task status is terminal for convenience in scheduler logic.
#[inline]
pub fn is_task_terminal(status: TaskStatus) -> bool {
    matches!(
        status,
        TaskStatus::Success | TaskStatus::Failed | TaskStatus::Skipped
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_limits_attempts() {
        let policy = RetryPolicy { max_attempts: 3 };
        assert!(policy.should_retry(0));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
    }

    #[test]
    fn dag_status_derivation() {
        let sts = [
            TaskStatus::Success,
            TaskStatus::Success,
            TaskStatus::Skipped,
        ];
        assert_eq!(dag_status_from_tasks(&sts), DagRunStatus::Success);

        let sts = [TaskStatus::Success, TaskStatus::Failed];
        assert_eq!(dag_status_from_tasks(&sts), DagRunStatus::Failed);

        let sts = [TaskStatus::Running, TaskStatus::Pending];
        assert_eq!(dag_status_from_tasks(&sts), DagRunStatus::Running);
    }
}

//! Status enums for DAG runs, tasks, and task attempts.
//!
//! These are the canonical states used by the workflow engine. Executor and
//! workers communicate using these values (over NATS, DB, etc.).
//!
//! Design goals:
//! - Simple, explicit finite state machines.
//! - Easy to serialize (Serde).
//! - Helper methods for common checks (is_terminal, is_success, etc.).

use serde::{Deserialize, Serialize};

/// Overall status of a specific DAG run (a concrete pipeline execution).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DagRunStatus {
    /// Created but not yet started (no tasks running).
    Created,
    /// At least one task is running or queued.
    Running,
    /// All terminal tasks succeeded.
    Success,
    /// At least one critical task failed and no more progress is possible.
    Failed,
    /// Execution was explicitly cancelled.
    Cancelled,
    /// Mixed outcome: some tasks failed or were skipped, but pipeline produced
    /// a partial result. MVP: optional, can be used later.
    PartialFailed,
}

impl From<&str> for DagRunStatus {
    fn from(v: &str) -> Self {
        match v {
            "created" => DagRunStatus::Created,
            "running" => DagRunStatus::Running,
            "success" => DagRunStatus::Success,
            "failed" => DagRunStatus::Failed,
            "cancelled" => DagRunStatus::Cancelled,
            "partial_failed" => DagRunStatus::PartialFailed,
            _ => DagRunStatus::Created, // Fallback to a safe default
        }
    }
}

impl From<DagRunStatus> for &'static str {
    fn from(status: DagRunStatus) -> Self {
        match status {
            DagRunStatus::Created => "created",
            DagRunStatus::Running => "running",
            DagRunStatus::Success => "success",
            DagRunStatus::Failed => "failed",
            DagRunStatus::Cancelled => "cancelled",
            DagRunStatus::PartialFailed => "partial_failed",
        }
    }
}

impl DagRunStatus {
    /// Returns `true` if the DAG run is in any terminal state.
    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            DagRunStatus::Success
                | DagRunStatus::Failed
                | DagRunStatus::Cancelled
                | DagRunStatus::PartialFailed
        )
    }

    /// Returns `true` if the DAG run is considered a success.
    #[inline]
    pub fn is_success(self) -> bool {
        matches!(self, DagRunStatus::Success)
    }
}

/// Logical status of a task within a DAG run.
///
/// Task is a *logical* unit (e.g. "chunk document X"), independent of
/// particular attempts (TaskRun).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Created but not yet ready (waiting for dependencies or planning).
    #[default]
    Pending,
    /// All dependencies are satisfied; the task may be scheduled for execution.
    Ready,
    /// At least one TaskRun is currently executing.
    Running,
    /// Task completed successfully (at least one TaskRun succeeded).
    Success,
    /// All attempts exhausted or unrecoverable failure.
    Failed,
    /// Task was intentionally skipped (e.g. branch not taken).
    Skipped,
}

impl From<&str> for TaskStatus {
    fn from(v: &str) -> Self {
        match v {
            "pending" => TaskStatus::Pending,
            "ready" => TaskStatus::Ready,
            "running" => TaskStatus::Running,
            "success" => TaskStatus::Success,
            "failed" => TaskStatus::Failed,
            "skipped" => TaskStatus::Skipped,
            _ => TaskStatus::Pending, // Fallback to a safe default
        }
    }
}

impl From<TaskStatus> for &'static str {
    fn from(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Pending => "pending",
            TaskStatus::Ready => "ready",
            TaskStatus::Running => "running",
            TaskStatus::Success => "success",
            TaskStatus::Failed => "failed",
            TaskStatus::Skipped => "skipped",
        }
    }
}

impl TaskStatus {
    /// Returns `true` if the task is in a terminal state.
    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskStatus::Success | TaskStatus::Failed | TaskStatus::Skipped
        )
    }

    /// Returns `true` if the task completed successfully.
    #[inline]
    pub fn is_success(self) -> bool {
        matches!(self, TaskStatus::Success)
    }

    /// Returns `true` if the task is eligible for scheduling.
    ///
    /// Executor may still apply quotas/limits on top of this.
    #[inline]
    pub fn is_schedulable(self) -> bool {
        matches!(self, TaskStatus::Ready)
    }
}

/// Status of a concrete attempt to execute a task.
///
/// Multiple TaskRun entries may exist for the same Task (retries).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunStatus {
    /// Attempt has been created but not yet started.
    #[default]
    Created,
    /// Attempt is currently executing.
    Running,
    /// Attempt finished successfully.
    Success,
    /// Attempt failed (transient or permanent).
    Failed,
    /// Attempt was aborted (e.g. cancelled by operator or timeout).
    Aborted,
}

impl From<&str> for TaskRunStatus {
    fn from(value: &str) -> Self {
        match value {
            "created" => TaskRunStatus::Created,
            "running" => TaskRunStatus::Running,
            "success" => TaskRunStatus::Success,
            "failed" => TaskRunStatus::Failed,
            "aborted" => TaskRunStatus::Aborted,
            _ => TaskRunStatus::Created, // Fallback to a safe default
        }
    }
}

impl From<TaskRunStatus> for &'static str {
    fn from(status: TaskRunStatus) -> Self {
        match status {
            TaskRunStatus::Created => "created",
            TaskRunStatus::Running => "running",
            TaskRunStatus::Success => "success",
            TaskRunStatus::Failed => "failed",
            TaskRunStatus::Aborted => "aborted",
        }
    }
}

impl TaskRunStatus {
    /// Returns `true` if this attempt will not transition to any other state.
    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskRunStatus::Success | TaskRunStatus::Failed | TaskRunStatus::Aborted
        )
    }

    /// Returns `true` if this attempt completed successfully.
    #[inline]
    pub fn is_success(self) -> bool {
        matches!(self, TaskRunStatus::Success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dagrun_terminal_flags() {
        assert!(DagRunStatus::Success.is_terminal());
        assert!(DagRunStatus::Failed.is_terminal());
        assert!(!DagRunStatus::Running.is_terminal());
    }

    #[test]
    fn task_terminal_flags() {
        assert!(TaskStatus::Success.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Skipped.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
        assert!(TaskStatus::Ready.is_schedulable());
    }

    #[test]
    fn taskrun_terminal_flags() {
        assert!(TaskRunStatus::Success.is_terminal());
        assert!(TaskRunStatus::Failed.is_terminal());
        assert!(TaskRunStatus::Aborted.is_terminal());
        assert!(!TaskRunStatus::Created.is_terminal());
    }
}

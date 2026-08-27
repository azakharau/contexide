//! Workflow domain module.
//!
//! DTOs for workflow entities stored in Postgres (via `contexide-storage`):
//! - `DagRun`    — a concrete execution of a workflow/DAG.
//! - `Task`      — a logical task node within a `DagRun`.
//! - `TaskRun`   — a concrete attempt to execute a `Task`.
//!
//! This module is DB-agnostic at the type level: plain structs + `sqlx::FromRow`
//! for Postgres. Repository traits & implementations live in sibling modules
//! (`mem` and `pg`).

use std::result::Result as StdResult;

use serde_json::Value as JsonValue;
use sqlx::types::Json;
use sqlx::{Row, postgres::PgRow, prelude::FromRow};

use contexide_core::prelude::{DagRunId, TaskId, TaskRunId, TenantId};

// Assuming workflow-core exposes ids & status enums via a prelude.
// Adjust paths here if your crate layout is different.
use contexide_workflow_core::{DagRunStatus, ExecutionPolicy, TaskRunStatus, TaskStatus};

pub mod mem;
pub mod pg;

/// A single workflow/DAG execution.
///
/// Minimal MVP fields:
/// - `workflow_key`: human-/system-readable key for the workflow definition
///   (e.g. "rag_ingest_v1").
/// - `status`: lifecycle state of this run.
/// - `params`: JSON payload with input parameters for the run.
/// - `error`: optional error message if the run failed.
#[derive(Debug, Clone)]
pub struct DagRun {
    pub id: DagRunId,
    pub tenant_id: TenantId,
    pub workflow_key: String,
    pub status: DagRunStatus,
    pub params: JsonValue,
    pub error: Option<String>,
    pub execution_policy: Option<ExecutionPolicy>,
    pub execution_policy_version: i16,
}

impl<'r> FromRow<'r, PgRow> for DagRun {
    fn from_row(row: &'r PgRow) -> StdResult<Self, sqlx::Error> {
        let id: uuid::Uuid = row.try_get("id")?;
        let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
        let workflow_key: String = row.try_get("workflow_key")?;
        let status_raw: String = row.try_get("status")?;
        let params: JsonValue = row.try_get("params")?;
        let error: Option<String> = row.try_get("error")?;
        let execution_policy: Option<Json<ExecutionPolicy>> = row.try_get("execution_policy")?;
        let execution_policy_version: i16 = row.try_get("execution_policy_version").unwrap_or(1);

        // Be conservative: unknown value falls back to a "created"/initial state.
        let status = DagRunStatus::from(status_raw.as_str());

        Ok(DagRun {
            id: DagRunId(id),
            tenant_id: TenantId(tenant_id),
            workflow_key,
            status,
            params,
            error,
            execution_policy: execution_policy.map(|j| j.0),
            execution_policy_version,
        })
    }
}

/// Logical task within a `DagRun`.
///
/// - `kind` describes what this task does (e.g. "chunk_text", "embed_chunks").
///   In many setups this will mirror an enum from `workflow-core`.
/// - `status` is high-level lifecycle (pending/ready/running/success/failed/…).
/// - `payload` carries task-specific input (JSON).
/// - `result` carries task-specific output (JSON), if any.
#[derive(Debug, Clone)]
pub struct Task {
    pub id: TaskId,
    pub dag_run_id: DagRunId,
    pub tenant_id: TenantId,
    pub kind: String,
    pub status: TaskStatus,
    pub payload: JsonValue,
    pub result: Option<JsonValue>,
    pub max_attempts: Option<i32>,
    pub retry_policy: String,
    pub retry_params: JsonValue,
    pub priority: i16,
    pub execution_policy_override: Option<ExecutionPolicy>,
}

impl<'r> FromRow<'r, PgRow> for Task {
    fn from_row(row: &'r PgRow) -> StdResult<Self, sqlx::Error> {
        let id: uuid::Uuid = row.try_get("id")?;
        let dag_run_id: uuid::Uuid = row.try_get("dag_run_id")?;
        let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
        let kind: String = row.try_get("kind")?;
        let status_raw: String = row.try_get("status")?;
        let payload: JsonValue = row.try_get("payload")?;
        let result: Option<JsonValue> = row.try_get("result")?;
        let max_attempts: Option<i32> = row.try_get("max_attempts").unwrap_or(None);
        let retry_policy: String = row
            .try_get("retry_policy")
            .unwrap_or_else(|_| "never".into());
        let retry_params: JsonValue = row
            .try_get("retry_params")
            .unwrap_or_else(|_| JsonValue::Object(Default::default()));
        let priority: i16 = row.try_get("priority").unwrap_or(0);
        let execution_policy_override: Option<Json<ExecutionPolicy>> =
            row.try_get("execution_policy_override").unwrap_or(None);

        let status = TaskStatus::from(status_raw.as_str());

        Ok(Task {
            id: TaskId(id),
            dag_run_id: DagRunId(dag_run_id),
            tenant_id: TenantId(tenant_id),
            kind,
            status,
            payload,
            result,
            max_attempts,
            retry_policy,
            retry_params,
            priority,
            execution_policy_override: execution_policy_override.map(|j| j.0),
        })
    }
}

/// Concrete attempt to execute a `Task`.
///
/// - Multiple `TaskRun` rows may exist per `Task` due to retries.
/// - `attempt_no` is a monotonic counter per task (0,1,2,…).
/// - `error` is a human-readable error message for failed attempts.
/// - `worker_label` can store worker identity / pod name for observability.
#[derive(Debug, Clone)]
pub struct TaskRun {
    pub id: TaskRunId,
    pub task_id: TaskId,
    pub tenant_id: TenantId,
    pub status: TaskRunStatus,
    pub attempt_no: i32,
    pub error: Option<String>,
    pub worker_label: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub transient_error: Option<bool>,
}

impl<'r> FromRow<'r, PgRow> for TaskRun {
    fn from_row(row: &'r PgRow) -> StdResult<Self, sqlx::Error> {
        let id: uuid::Uuid = row.try_get("id")?;
        let task_id: uuid::Uuid = row.try_get("task_id")?;
        let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
        let status_raw: String = row.try_get("status")?;
        let attempt_no: i32 = row.try_get("attempt_no")?;
        let error: Option<String> = row.try_get("error")?;
        let worker_label: Option<String> = row.try_get("worker_label")?;
        let error_code: Option<String> = row.try_get("error_code").unwrap_or(None);
        let error_message: Option<String> = row.try_get("error_message").unwrap_or(None);
        let transient_error: Option<bool> = row.try_get("transient_error").unwrap_or(None);

        let status = TaskRunStatus::from(status_raw.as_str());

        Ok(TaskRun {
            id: TaskRunId(id),
            task_id: TaskId(task_id),
            tenant_id: TenantId(tenant_id),
            status,
            attempt_no,
            error,
            worker_label,
            error_code,
            error_message,
            transient_error,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::Error as CoreError;

    // Very lightweight compile-time sanity: just check types compile with sqlx macros.
    //
    // You can wire this up to a real test Postgres later if you want to validate
    // actual queries against a live schema (similar to other storage tests).
    #[tokio::test]
    async fn compile_time_check_with_sqlx() -> StdResult<(), CoreError> {
        // Use an in-memory Pg connection string or a test URL via env if desired.
        // Here we just ensure the generic shape compiles; we don't actually connect.
        let _ = (
            std::any::type_name::<DagRun>(),
            std::any::type_name::<Task>(),
            std::any::type_name::<TaskRun>(),
        );
        Ok(())
    }
}

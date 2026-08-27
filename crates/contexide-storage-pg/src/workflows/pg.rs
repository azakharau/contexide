//! Postgres-backed repositories for workflow domain.
//!
//! Covers three core entities:
//! - `DagRun`
//! - `Task`
//! - `TaskRun`
//!
//! Each repo implements the generic `Repository` trait for basic CRUD,
//! plus a few domain-specific helpers like `list_by_tenant` or
//! `list_by_dag_run` / `list_by_task`.
//!
//! Notes:
//! - Tables are assumed to be: `dag_runs`, `tasks`, `task_runs`.
//! - Status fields are stored as TEXT using `Into<&'static str>` / `TryFrom<&str>`
//!   for the enum ↔ string conversions.

use contexide_core::prelude::{DagRunId, Result, TaskId, TaskRunId, TenantId};
use contexide_workflow_core::{DagRunStatus, ExecutionPolicy, TaskRunStatus, TaskStatus};
use sqlx::types::Json;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

use crate::traits::Repository;

use super::{DagRun, Task, TaskRun};

/* =============================================================================
PgDagRunRepo
============================================================================= */

/// Postgres-backed `DagRun` repository.
///
/// Uses simple `INSERT .. ON CONFLICT (id) DO UPDATE` semantics for `save`.
pub struct PgDagRunRepo {
    pool: Pool<Postgres>,
}

impl PgDagRunRepo {
    /// Build from an existing Postgres pool (cheap clone).
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    /// Access underlying pool (for transactions / manual queries if needed).
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    /// List all DAG runs for a given tenant, ordered by creation time (newest first).
    pub async fn list_by_tenant(&self, tenant_id: TenantId) -> Result<Vec<DagRun>> {
        let rows = sqlx::query(
            r#"
            SELECT id, tenant_id, workflow_key, status, params, error,
                   execution_policy, execution_policy_version
            FROM dag_runs
            WHERE tenant_id = $1
            ORDER BY created_at DESC, id DESC
            "#,
        )
        .bind::<Uuid>(tenant_id.into())
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(map_dag_run_row(row)?);
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl Repository for PgDagRunRepo {
    type Key = DagRunId;
    type Entity = DagRun;

    async fn get(&self, id: Self::Key) -> Result<Option<Self::Entity>> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, tenant_id, workflow_key, status, params, error
                   , execution_policy, execution_policy_version
            FROM dag_runs
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.into())
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row_opt {
            Ok(Some(map_dag_run_row(row)?))
        } else {
            Ok(None)
        }
    }

    async fn save(&self, entity: Self::Entity) -> Result<Self::Entity> {
        let status_str: &'static str = entity.status.into();

        let row = sqlx::query(
            r#"
            INSERT INTO dag_runs (id, tenant_id, workflow_key, status, params, error,
                                  execution_policy, execution_policy_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id) DO UPDATE
            SET tenant_id   = EXCLUDED.tenant_id,
                workflow_key = EXCLUDED.workflow_key,
                status      = EXCLUDED.status,
                params      = EXCLUDED.params,
                error       = EXCLUDED.error,
                execution_policy = EXCLUDED.execution_policy,
                execution_policy_version = EXCLUDED.execution_policy_version
            RETURNING id, tenant_id, workflow_key, status, params, error,
                      execution_policy, execution_policy_version
            "#,
        )
        .bind::<Uuid>(entity.id.into())
        .bind::<Uuid>(entity.tenant_id.into())
        .bind(&entity.workflow_key)
        .bind(status_str)
        .bind(&entity.params)
        .bind(&entity.error)
        .bind(entity.execution_policy.as_ref().map(Json))
        .bind(entity.execution_policy_version)
        .fetch_one(&self.pool)
        .await?;

        map_dag_run_row(row)
    }

    async fn delete(&self, id: Self::Key) -> Result<bool> {
        let affected = sqlx::query(
            r#"
            DELETE FROM dag_runs
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.into())
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(affected == 1)
    }
}

/// Map a raw Postgres row into `DagRun`.
///
/// Kept local to this module; if you later add `impl FromRow for DagRun`
/// in `workflow/mod.rs`, you can switch to `query_as` instead.
fn map_dag_run_row(row: sqlx::postgres::PgRow) -> Result<DagRun> {
    let id: Uuid = row.try_get("id")?;
    let tenant_id: Uuid = row.try_get("tenant_id")?;
    let workflow_key: String = row.try_get("workflow_key")?;
    let status_raw: String = row.try_get("status")?;
    let params: serde_json::Value = row.try_get("params")?;
    let error: Option<String> = row.try_get("error")?;
    let execution_policy: Option<Json<ExecutionPolicy>> = row.try_get("execution_policy")?;
    let execution_policy_version: i16 = row.try_get("execution_policy_version").unwrap_or(1);

    let status = DagRunStatus::from(status_raw.as_str());

    Ok(DagRun {
        id: DagRunId::from(id),
        tenant_id: TenantId::from(tenant_id),
        workflow_key,
        status,
        params,
        error,
        execution_policy: execution_policy.map(|j| j.0),
        execution_policy_version,
    })
}

/* =============================================================================
PgTaskRepo
============================================================================= */

/// Postgres-backed `Task` repository.
pub struct PgTaskRepo {
    pool: Pool<Postgres>,
}

impl PgTaskRepo {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    /// List all tasks for a given `DagRun`, ordered by creation time.
    pub async fn list_by_dag_run(&self, dag_run_id: DagRunId) -> Result<Vec<Task>> {
        let rows = sqlx::query(
            r#"
            SELECT id, dag_run_id, tenant_id, kind, status, payload, result,
                   max_attempts, retry_policy, retry_params, priority, execution_policy_override
            FROM tasks
            WHERE dag_run_id = $1
            ORDER BY created_at ASC, id ASC
            "#,
        )
        .bind::<Uuid>(dag_run_id.into())
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(map_task_row(row)?);
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl Repository for PgTaskRepo {
    type Key = TaskId;
    type Entity = Task;

    async fn get(&self, id: Self::Key) -> Result<Option<Self::Entity>> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, dag_run_id, tenant_id, kind, status, payload, result,
                   max_attempts, retry_policy, retry_params, priority, execution_policy_override
            FROM tasks
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.into())
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row_opt {
            Ok(Some(map_task_row(row)?))
        } else {
            Ok(None)
        }
    }

    async fn save(&self, entity: Self::Entity) -> Result<Self::Entity> {
        let status_str: &'static str = entity.status.into();

        let row = sqlx::query(
            r#"
            INSERT INTO tasks (id, dag_run_id, tenant_id, kind, status, payload, result,
                               max_attempts, retry_policy, retry_params, priority, execution_policy_override)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO UPDATE
            SET dag_run_id = EXCLUDED.dag_run_id,
                tenant_id  = EXCLUDED.tenant_id,
                kind       = EXCLUDED.kind,
                status     = EXCLUDED.status,
                payload    = EXCLUDED.payload,
                result     = EXCLUDED.result,
                max_attempts = EXCLUDED.max_attempts,
                retry_policy = EXCLUDED.retry_policy,
                retry_params = EXCLUDED.retry_params,
                priority = EXCLUDED.priority,
                execution_policy_override = EXCLUDED.execution_policy_override
            RETURNING id, dag_run_id, tenant_id, kind, status, payload, result,
                      max_attempts, retry_policy, retry_params, priority, execution_policy_override
            "#,
        )
        .bind::<Uuid>(entity.id.into())
        .bind::<Uuid>(entity.dag_run_id.into())
        .bind::<Uuid>(entity.tenant_id.into())
        .bind(&entity.kind)
        .bind(status_str)
        .bind(&entity.payload)
        .bind(&entity.result)
        .bind(entity.max_attempts)
        .bind(&entity.retry_policy)
        .bind(&entity.retry_params)
        .bind(entity.priority)
        .bind(entity.execution_policy_override.as_ref().map(Json))
        .fetch_one(&self.pool)
        .await?;

        map_task_row(row)
    }

    async fn delete(&self, id: Self::Key) -> Result<bool> {
        let affected = sqlx::query(
            r#"
            DELETE FROM tasks
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.into())
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(affected == 1)
    }
}

fn map_task_row(row: sqlx::postgres::PgRow) -> Result<Task> {
    let id: Uuid = row.try_get("id")?;
    let dag_run_id: Uuid = row.try_get("dag_run_id")?;
    let tenant_id: Uuid = row.try_get("tenant_id")?;
    let kind: String = row.try_get("kind")?;
    let status_raw: String = row.try_get("status")?;
    let payload: serde_json::Value = row.try_get("payload")?;
    let result: Option<serde_json::Value> = row.try_get("result")?;
    let max_attempts: Option<i32> = row.try_get("max_attempts").unwrap_or(None);
    let retry_policy: String = row
        .try_get("retry_policy")
        .unwrap_or_else(|_| "never".into());
    let retry_params: serde_json::Value = row
        .try_get("retry_params")
        .unwrap_or_else(|_| serde_json::json!({}));
    let priority: i16 = row.try_get("priority").unwrap_or(0);
    let execution_policy_override: Option<Json<ExecutionPolicy>> =
        row.try_get("execution_policy_override").unwrap_or(None);

    let status = TaskStatus::from(status_raw.as_str());

    Ok(Task {
        id: TaskId::from(id),
        dag_run_id: DagRunId::from(dag_run_id),
        tenant_id: TenantId::from(tenant_id),
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

/* =============================================================================
PgTaskRunRepo
============================================================================= */

/// Postgres-backed `TaskRun` repository.
pub struct PgTaskRunRepo {
    pool: Pool<Postgres>,
}

impl PgTaskRunRepo {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    /// List all attempts for a given task, ordered by `attempt_no` ascending.
    pub async fn list_by_task(&self, task_id: TaskId) -> Result<Vec<TaskRun>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_id, tenant_id, status, attempt_no, error, worker_label,
                   error_code, error_message, transient_error
            FROM task_runs
            WHERE task_id = $1
            ORDER BY attempt_no ASC, id ASC
            "#,
        )
        .bind::<Uuid>(task_id.into())
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(map_task_run_row(row)?);
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl Repository for PgTaskRunRepo {
    type Key = TaskRunId;
    type Entity = TaskRun;

    async fn get(&self, id: Self::Key) -> Result<Option<Self::Entity>> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, task_id, tenant_id, status, attempt_no, error, worker_label,
                   error_code, error_message, transient_error
            FROM task_runs
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.into())
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row_opt {
            Ok(Some(map_task_run_row(row)?))
        } else {
            Ok(None)
        }
    }

    async fn save(&self, entity: Self::Entity) -> Result<Self::Entity> {
        let status_str: &'static str = entity.status.into();

        let row = sqlx::query(
            r#"
            INSERT INTO task_runs (id, task_id, tenant_id, status, attempt_no, error, worker_label,
                                   error_code, error_message, transient_error)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO UPDATE
            SET task_id      = EXCLUDED.task_id,
                tenant_id    = EXCLUDED.tenant_id,
                status       = EXCLUDED.status,
                attempt_no   = EXCLUDED.attempt_no,
                error        = EXCLUDED.error,
                worker_label = EXCLUDED.worker_label,
                error_code   = EXCLUDED.error_code,
                error_message = EXCLUDED.error_message,
                transient_error = EXCLUDED.transient_error
            RETURNING id, task_id, tenant_id, status, attempt_no, error, worker_label,
                      error_code, error_message, transient_error
            "#,
        )
        .bind::<Uuid>(entity.id.into())
        .bind::<Uuid>(entity.task_id.into())
        .bind::<Uuid>(entity.tenant_id.into())
        .bind(status_str)
        .bind(entity.attempt_no)
        .bind(&entity.error)
        .bind(&entity.worker_label)
        .bind(&entity.error_code)
        .bind(&entity.error_message)
        .bind(entity.transient_error)
        .fetch_one(&self.pool)
        .await?;

        map_task_run_row(row)
    }

    async fn delete(&self, id: Self::Key) -> Result<bool> {
        let affected = sqlx::query(
            r#"
            DELETE FROM task_runs
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.into())
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(affected == 1)
    }
}

fn map_task_run_row(row: sqlx::postgres::PgRow) -> Result<TaskRun> {
    let id: Uuid = row.try_get("id")?;
    let task_id: Uuid = row.try_get("task_id")?;
    let tenant_id: Uuid = row.try_get("tenant_id")?;
    let status_raw: String = row.try_get("status")?;
    let attempt_no: i32 = row.try_get("attempt_no")?;
    let error: Option<String> = row.try_get("error")?;
    let worker_label: Option<String> = row.try_get("worker_label")?;
    let error_code: Option<String> = row.try_get("error_code").unwrap_or(None);
    let error_message: Option<String> = row.try_get("error_message").unwrap_or(None);
    let transient_error: Option<bool> = row.try_get("transient_error").unwrap_or(None);

    let status: TaskRunStatus = status_raw.as_str().into();

    Ok(TaskRun {
        id: TaskRunId::from(id),
        task_id: TaskId::from(task_id),
        tenant_id: TenantId::from(tenant_id),
        status,
        attempt_no,
        error,
        worker_label,
        error_code,
        error_message,
        transient_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Just compile-time checks that repos implement `Repository`.
    #[test]
    fn repos_implement_repository_trait() {
        fn assert_repo<R: Repository>() {}
        assert_repo::<PgDagRunRepo>();
        assert_repo::<PgTaskRepo>();
        assert_repo::<PgTaskRunRepo>();
    }
}

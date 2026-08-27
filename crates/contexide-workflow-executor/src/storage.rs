//! Thin abstraction layer over workflow repositories exposed by `contexide-storage`.
//!
//! The storage crate exposes concrete Postgres and in-memory repos with helper
//! methods (`list_by_dag_run`, `list_by_task`, etc.) but no shared trait that
//! groups them. The executor needs a trait-object-friendly surface, so we define
//! minimal traits here and implement them for existing repos.

//! Storage-facing adapters.
//!
//! `contexide-storage` exposes concrete repos, but the executor needs
//! trait-object-friendly interfaces. This module defines minimal traits used by
//! planner/scheduler and implements them for Postgres and in-memory repos.

use async_trait::async_trait;
use contexide_core::prelude::{DagRunId, Result, TaskId, TaskRunId, TenantId};
use contexide_storage_pg::{
    traits::Repository,
    workflows::{
        DagRun, Task, TaskRun,
        mem::{MemDagRunRepo, MemTaskRepo, MemTaskRunRepo},
        pg::{PgDagRunRepo, PgTaskRepo, PgTaskRunRepo},
    },
};

/// Repository contract for DagRuns used by the executor.
#[async_trait]
pub trait DagRunRepo: Repository<Key = DagRunId, Entity = DagRun> + Send + Sync {
    async fn list_by_tenant(&self, tenant_id: TenantId) -> Result<Vec<DagRun>>;
}

/// Repository contract for Tasks used by the executor.
#[async_trait]
pub trait TaskRepo: Repository<Key = TaskId, Entity = Task> + Send + Sync {
    async fn list_by_dag_run(&self, dag_run_id: DagRunId) -> Result<Vec<Task>>;

    /// List tasks that are currently pending (eligible for scheduling).
    async fn list_pending(&self, limit: Option<usize>) -> Result<Vec<Task>>;
}

/// Repository contract for TaskRuns used by the executor.
#[async_trait]
pub trait TaskRunRepo: Repository<Key = TaskRunId, Entity = TaskRun> + Send + Sync {
    async fn list_by_task(&self, task_id: TaskId) -> Result<Vec<TaskRun>>;
}

#[async_trait]
impl DagRunRepo for PgDagRunRepo {
    async fn list_by_tenant(&self, tenant_id: TenantId) -> Result<Vec<DagRun>> {
        PgDagRunRepo::list_by_tenant(self, tenant_id).await
    }
}

#[async_trait]
impl TaskRepo for PgTaskRepo {
    async fn list_by_dag_run(&self, dag_run_id: DagRunId) -> Result<Vec<Task>> {
        PgTaskRepo::list_by_dag_run(self, dag_run_id).await
    }

    async fn list_pending(&self, limit: Option<usize>) -> Result<Vec<Task>> {
        // Keep SQL explicit and avoid macros to stay offline-friendly.
        // `created_at` column exists in the storage schema; fall back to id ordering if absent.
        let base = r#"
            SELECT id, dag_run_id, tenant_id, kind, status, payload, result
            FROM tasks
            WHERE status = 'pending'
            ORDER BY created_at ASC, id ASC
        "#;

        let rows = if let Some(lim) = limit {
            sqlx::query_as::<_, Task>(&format!("{base} LIMIT $1"))
                .bind(lim as i64)
                .fetch_all(PgTaskRepo::pool(self))
                .await?
        } else {
            sqlx::query_as::<_, Task>(base)
                .fetch_all(PgTaskRepo::pool(self))
                .await?
        };

        Ok(rows)
    }
}

#[async_trait]
impl TaskRunRepo for PgTaskRunRepo {
    async fn list_by_task(&self, task_id: TaskId) -> Result<Vec<TaskRun>> {
        PgTaskRunRepo::list_by_task(self, task_id).await
    }
}

#[async_trait]
impl DagRunRepo for MemDagRunRepo {
    async fn list_by_tenant(&self, tenant_id: TenantId) -> Result<Vec<DagRun>> {
        Ok(MemDagRunRepo::list_by_tenant(self, tenant_id))
    }
}

#[async_trait]
impl TaskRepo for MemTaskRepo {
    async fn list_by_dag_run(&self, dag_run_id: DagRunId) -> Result<Vec<Task>> {
        Ok(MemTaskRepo::list_by_dag_run(self, dag_run_id))
    }

    async fn list_pending(&self, limit: Option<usize>) -> Result<Vec<Task>> {
        let mut tasks: Vec<Task> = MemTaskRepo::list_all(self)
            .into_iter()
            .filter(|t| matches!(t.status, contexide_workflow_core::TaskStatus::Pending))
            .collect();

        tasks.sort_by_key(|t| t.id.0);
        if let Some(lim) = limit {
            tasks.truncate(lim);
        }
        Ok(tasks)
    }
}

#[async_trait]
impl TaskRunRepo for MemTaskRunRepo {
    async fn list_by_task(&self, task_id: TaskId) -> Result<Vec<TaskRun>> {
        Ok(MemTaskRunRepo::list_by_task(self, task_id))
    }
}

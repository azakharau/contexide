//! Planner builds DagRuns and initial Tasks for a given workflow profile.
//!
//! The planner is intentionally simple in this MVP: it supports a single
//! static ingest DAG and defers all scheduling decisions to the scheduler.
//! It operates purely through storage repositories, keeping orchestration
//! separate from persistence.

use std::sync::Arc;

use async_trait::async_trait;
use contexide_core::prelude::{DagRunId, Result, TenantId};
use contexide_storage_pg::workflows::{DagRun, Task};
use contexide_workflow_core::{DagRunStatus, TaskStatus};
use serde_json::Value;

use crate::storage::{DagRunRepo, TaskRepo};

/// Planner creates DagRuns and their logical Tasks for a given workflow profile.
#[async_trait]
pub trait Planner: Send + Sync {
    /// Create a new DagRun and its tasks based on a high-level profile name and input payload.
    async fn create_dag_run(
        &self,
        tenant_id: TenantId,
        profile: &str,
        input: Value,
    ) -> Result<DagRunId>;
}

/// Database-backed planner using workflow repositories.
pub struct DbPlanner<D: DagRunRepo, T: TaskRepo> {
    dag_runs: Arc<D>,
    tasks: Arc<T>,
}

impl<D: DagRunRepo, T: TaskRepo> DbPlanner<D, T> {
    pub fn new(dag_runs: Arc<D>, tasks: Arc<T>) -> Self {
        Self { dag_runs, tasks }
    }

    fn profile_sequence(profile: &str) -> &'static [&'static str] {
        // MVP: single static DAG profile
        let _ = profile;
        &["extractor", "normalizer", "chunker", "embedder", "indexer"]
    }
}

#[async_trait]
impl<D, T> Planner for DbPlanner<D, T>
where
    D: DagRunRepo + 'static,
    T: TaskRepo + 'static,
{
    async fn create_dag_run(
        &self,
        tenant_id: TenantId,
        profile: &str,
        input: Value,
    ) -> Result<DagRunId> {
        // TODO: wrap in a DB transaction once shared transaction helpers exist.
        let dag_run = DagRun {
            id: DagRunId::new(),
            tenant_id,
            workflow_key: profile.to_string(),
            status: DagRunStatus::Created,
            params: input.clone(),
            error: None,
            execution_policy: None,
            execution_policy_version: 1,
        };

        self.dag_runs.save(dag_run.clone()).await?;

        let mut tasks = Vec::new();
        for (idx, kind) in Self::profile_sequence(profile).iter().enumerate() {
            let payload = if idx == 0 { input.clone() } else { Value::Null };
            tasks.push(Task {
                id: contexide_core::prelude::TaskId::new(),
                dag_run_id: dag_run.id,
                tenant_id,
                kind: (*kind).to_string(),
                status: TaskStatus::Pending,
                payload,
                result: None,
                max_attempts: None,
                retry_policy: "never".into(),
                retry_params: serde_json::json!({}),
                priority: 0,
                execution_policy_override: None,
            });
        }

        for task in tasks {
            self.tasks.save(task).await?;
        }

        Ok(dag_run.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_storage_pg::traits::Repository;
    use contexide_storage_pg::workflows::mem::{MemDagRunRepo, MemTaskRepo};
    use contexide_workflow_core::TaskStatus;

    #[tokio::test]
    async fn creates_dag_run_and_tasks() {
        let dag_runs = Arc::new(MemDagRunRepo::new());
        let tasks = Arc::new(MemTaskRepo::new());
        let planner = DbPlanner::new(Arc::clone(&dag_runs), Arc::clone(&tasks));

        let tenant = TenantId::new();
        let input = serde_json::json!({"doc": "s3://bucket/key"});
        let dag_run_id = planner
            .create_dag_run(tenant, "ingest_default", input.clone())
            .await
            .expect("plan dag");

        let dag_run = dag_runs.get(dag_run_id).await.unwrap().unwrap();
        assert_eq!(dag_run.workflow_key, "ingest_default");
        assert_eq!(dag_run.status, DagRunStatus::Created);

        let created_tasks = tasks.list_all();
        assert_eq!(created_tasks.len(), 5);
        assert!(
            created_tasks
                .iter()
                .any(|t| t.kind == "extractor" && t.payload == input)
        );
        assert!(
            created_tasks
                .iter()
                .all(|t| t.status == TaskStatus::Pending)
        );
    }
}

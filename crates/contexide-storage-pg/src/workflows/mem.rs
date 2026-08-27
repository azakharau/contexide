//! In-memory repositories for workflow domain.
//!
//! These implementations are meant for tests and local demos only.
//! They keep all data in a `Mutex<HashMap<..>>` and are **not** meant for
//! concurrent production traffic.
//!
//! They implement the generic `Repository` trait so you can reuse them
//! wherever a `Repository<Key = .., Entity = ..>` is expected.

use std::{collections::HashMap, sync::Mutex};

use contexide_core::prelude::{DagRunId, Result, TaskId, TaskRunId, TenantId};

use crate::traits::Repository;

use super::{DagRun, Task, TaskRun};

/// In-memory `DagRun` repository.
///
/// Keyed by `DagRunId`. Caller is responsible for generating ids
/// (e.g. via `DagRunId::new()`) before calling `save`.
pub struct MemDagRunRepo {
    map: Mutex<HashMap<DagRunId, DagRun>>,
}

impl MemDagRunRepo {
    /// Build an empty in-memory repo.
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// List all runs regardless of tenant (useful for tests and lightweight schedulers).
    pub fn list_all(&self) -> Vec<DagRun> {
        self.map.lock().unwrap().values().cloned().collect()
    }

    /// List all runs for a given tenant.
    pub fn list_by_tenant(&self, tenant_id: TenantId) -> Vec<DagRun> {
        let guard = self.map.lock().unwrap();
        guard
            .values()
            .filter(|r| r.tenant_id == tenant_id)
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl Repository for MemDagRunRepo {
    type Key = DagRunId;
    type Entity = DagRun;

    async fn get(&self, id: Self::Key) -> Result<Option<Self::Entity>> {
        let guard = self.map.lock().unwrap();
        Ok(guard.get(&id).cloned())
    }

    async fn save(&self, entity: Self::Entity) -> Result<Self::Entity> {
        let mut guard = self.map.lock().unwrap();
        guard.insert(entity.id, entity.clone());
        Ok(entity)
    }

    async fn delete(&self, id: Self::Key) -> Result<bool> {
        let mut guard = self.map.lock().unwrap();
        Ok(guard.remove(&id).is_some())
    }
}

impl Default for MemDagRunRepo {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory `Task` repository.
///
/// Keyed by `TaskId`. Provides a couple of helpers for typical workflows
/// (listing by `DagRunId`).
pub struct MemTaskRepo {
    map: Mutex<HashMap<TaskId, Task>>,
}

impl MemTaskRepo {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// List all tasks across all DAG runs.
    pub fn list_all(&self) -> Vec<Task> {
        self.map.lock().unwrap().values().cloned().collect()
    }

    /// List all tasks belonging to a given `DagRun`.
    pub fn list_by_dag_run(&self, dag_run_id: DagRunId) -> Vec<Task> {
        let guard = self.map.lock().unwrap();
        guard
            .values()
            .filter(|t| t.dag_run_id == dag_run_id)
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl Repository for MemTaskRepo {
    type Key = TaskId;
    type Entity = Task;

    async fn get(&self, id: Self::Key) -> Result<Option<Self::Entity>> {
        let guard = self.map.lock().unwrap();
        Ok(guard.get(&id).cloned())
    }

    async fn save(&self, entity: Self::Entity) -> Result<Self::Entity> {
        let mut guard = self.map.lock().unwrap();
        guard.insert(entity.id, entity.clone());
        Ok(entity)
    }

    async fn delete(&self, id: Self::Key) -> Result<bool> {
        let mut guard = self.map.lock().unwrap();
        Ok(guard.remove(&id).is_some())
    }
}

impl Default for MemTaskRepo {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory `TaskRun` repository.
///
/// Keyed by `TaskRunId`. For tests it is often useful to list runs by
/// `TaskId` and inspect attempts.
pub struct MemTaskRunRepo {
    map: Mutex<HashMap<TaskRunId, TaskRun>>,
}

impl MemTaskRunRepo {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// List all attempts across all tasks.
    pub fn list_all(&self) -> Vec<TaskRun> {
        let mut runs: Vec<_> = self.map.lock().unwrap().values().cloned().collect();
        runs.sort_by_key(|tr| tr.attempt_no);
        runs
    }

    /// List all attempts for a given task (sorted by `attempt_no` ascending).
    pub fn list_by_task(&self, task_id: TaskId) -> Vec<TaskRun> {
        let mut runs: Vec<_> = self
            .map
            .lock()
            .unwrap()
            .values()
            .filter(|tr| tr.task_id == task_id)
            .cloned()
            .collect();

        runs.sort_by_key(|tr| tr.attempt_no);
        runs
    }
}

impl Default for MemTaskRunRepo {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Repository for MemTaskRunRepo {
    type Key = TaskRunId;
    type Entity = TaskRun;

    async fn get(&self, id: Self::Key) -> Result<Option<Self::Entity>> {
        let guard = self.map.lock().unwrap();
        Ok(guard.get(&id).cloned())
    }

    async fn save(&self, entity: Self::Entity) -> Result<Self::Entity> {
        let mut guard = self.map.lock().unwrap();
        guard.insert(entity.id, entity.clone());
        Ok(entity)
    }

    async fn delete(&self, id: Self::Key) -> Result<bool> {
        let mut guard = self.map.lock().unwrap();
        Ok(guard.remove(&id).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::TenantId;
    use contexide_workflow_core::{DagRunStatus, TaskRunStatus, TaskStatus};

    #[tokio::test]
    async fn dagrun_repo_roundtrip() {
        let repo = MemDagRunRepo::new();
        let tenant = TenantId::new();
        let id = DagRunId::new();

        let run = DagRun {
            id,
            tenant_id: tenant,
            workflow_key: "test".to_string(),
            status: DagRunStatus::Created,
            params: serde_json::json!({"k": "v"}),
            error: None,
            execution_policy: None,
            execution_policy_version: 1,
        };

        let saved = repo.save(run.clone()).await.unwrap();
        assert_eq!(saved.id, id);

        let fetched = repo.get(id).await.unwrap().unwrap();
        assert_eq!(fetched.workflow_key, "test");

        let listed = repo.list_by_tenant(tenant);
        assert_eq!(listed.len(), 1);

        assert!(repo.delete(id).await.unwrap());
        assert!(repo.get(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn task_repo_roundtrip() {
        let tasks = MemTaskRepo::new();
        let dag_id = DagRunId::new();
        let tenant = TenantId::new();
        let id = TaskId::new();

        let task = Task {
            id,
            dag_run_id: dag_id,
            tenant_id: tenant,
            kind: "chunk".to_string(),
            status: TaskStatus::Pending,
            payload: serde_json::json!({"n": 1}),
            result: None,
            max_attempts: None,
            retry_policy: "never".into(),
            retry_params: serde_json::json!({}),
            priority: 0,
            execution_policy_override: None,
        };

        tasks.save(task.clone()).await.unwrap();
        let list = tasks.list_by_dag_run(dag_id);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, id);
    }

    #[tokio::test]
    async fn task_run_repo_roundtrip() {
        let runs = MemTaskRunRepo::new();
        let task_id = TaskId::new();
        let tenant = TenantId::new();
        let run_id = TaskRunId::new();

        let tr = TaskRun {
            id: run_id,
            task_id,
            tenant_id: tenant,
            status: TaskRunStatus::Created,
            attempt_no: 0,
            error: None,
            worker_label: None,
            error_code: None,
            error_message: None,
            transient_error: None,
        };

        runs.save(tr.clone()).await.unwrap();
        let fetched = runs.get(run_id).await.unwrap().unwrap();
        assert_eq!(fetched.task_id, task_id);

        let by_task = runs.list_by_task(task_id);
        assert_eq!(by_task.len(), 1);
        assert_eq!(by_task[0].attempt_no, 0);
    }
}

//! Scheduler: decides what to run next and handles worker results.
//!
//! DbScheduler uses workflow repositories to:
//! - pick pending tasks that are unblocked,
//! - create TaskRun attempts and mark tasks running,
//! - process worker success/failure messages with retries,
//! - derive DagRun status as tasks complete.
//!
//! The logic is deliberately straightforward to keep at-least-once semantics
//! testable without real DB/NATS dependencies.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use contexide_core::prelude::{DagRunId, Result, TaskRunId};
use contexide_storage_pg::workflows::{Task, TaskRun};
use contexide_workflow_core::{DagRunStatus, TaskRunStatus, TaskStatus};
use tracing::debug;

use crate::domain::{ReadyTask, RetryPolicy, WorkerStatus, dag_status_from_tasks};
use crate::storage::{DagRunRepo, TaskRepo, TaskRunRepo};

#[async_trait]
pub trait Scheduler: Send + Sync {
    /// Find tasks that are ready to run, create TaskRun records, and return ReadyTask descriptions.
    async fn schedule_ready_tasks(&self) -> Result<Vec<ReadyTask>>;

    /// Handle a status message from a worker (success or failure).
    async fn handle_worker_status(&self, status: WorkerStatus) -> Result<()>;
}

pub struct DbScheduler<D: DagRunRepo, T: TaskRepo, R: TaskRunRepo> {
    dag_runs: Arc<D>,
    tasks: Arc<T>,
    task_runs: Arc<R>,
    retry_policy: RetryPolicy,
    /// Optional hard cap on tasks to schedule per pump; None means no cap.
    pub max_batch: Option<usize>,
}

impl<D: DagRunRepo, T: TaskRepo, R: TaskRunRepo> DbScheduler<D, T, R> {
    pub fn new(
        dag_runs: Arc<D>,
        tasks: Arc<T>,
        task_runs: Arc<R>,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            dag_runs,
            tasks,
            task_runs,
            retry_policy,
            max_batch: None,
        }
    }

    fn ordering_for_kind(kind: &str) -> usize {
        match kind {
            "extractor" => 0,
            "normalizer" => 1,
            "chunker" => 2,
            "embedder" => 3,
            "indexer" => 4,
            _ => usize::MAX,
        }
    }

    fn is_ready(task: &Task, siblings: &[Task]) -> bool {
        if !matches!(task.status, TaskStatus::Pending | TaskStatus::Ready) {
            return false;
        }

        let idx = Self::ordering_for_kind(&task.kind);
        if idx == 0 || idx == usize::MAX {
            return true;
        }

        for sib in siblings {
            let sib_idx = Self::ordering_for_kind(&sib.kind);
            if sib_idx < idx && !matches!(sib.status, TaskStatus::Success | TaskStatus::Skipped) {
                return false;
            }
        }
        true
    }

    async fn mark_dag_running(&self, dag_run_id: DagRunId) -> Result<()> {
        if let Some(mut run) = self.dag_runs.get(dag_run_id).await?
            && matches!(run.status, DagRunStatus::Created)
        {
            run.status = DagRunStatus::Running;
            self.dag_runs.save(run).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<D, T, R> Scheduler for DbScheduler<D, T, R>
where
    D: DagRunRepo + 'static,
    T: TaskRepo + 'static,
    R: TaskRunRepo + 'static,
{
    async fn schedule_ready_tasks(&self) -> Result<Vec<ReadyTask>> {
        let pending = self.tasks.list_pending(self.max_batch).await?;
        let mut tasks_by_run: HashMap<DagRunId, Vec<Task>> = HashMap::new();
        let mut ready = Vec::new();

        for task in pending {
            let siblings = tasks_by_run.entry(task.dag_run_id).or_default();
            if siblings.is_empty() {
                let mut list = self.tasks.list_by_dag_run(task.dag_run_id).await?;
                siblings.append(&mut list);
            }

            if !Self::is_ready(&task, siblings) {
                continue;
            }

            let attempts = self.task_runs.list_by_task(task.id).await?;
            let attempt_no = attempts.len() as i32;

            let task_run = TaskRun {
                id: TaskRunId::new(),
                task_id: task.id,
                tenant_id: task.tenant_id,
                status: TaskRunStatus::Running,
                attempt_no,
                error: None,
                worker_label: None,
                error_code: None,
                error_message: None,
                transient_error: None,
            };

            self.task_runs.save(task_run.clone()).await?;

            let mut updated_task = task.clone();
            updated_task.status = TaskStatus::Running;
            self.tasks.save(updated_task.clone()).await?;

            self.mark_dag_running(task.dag_run_id).await?;

            ready.push(ReadyTask {
                tenant_id: task.tenant_id,
                dag_run_id: task.dag_run_id,
                task_id: task.id,
                task_run_id: task_run.id,
                kind: task.kind.clone(),
                payload: task.payload.clone(),
            });
        }

        Ok(ready)
    }

    async fn handle_worker_status(&self, status: WorkerStatus) -> Result<()> {
        // TODO: wrap in transaction once a shared transaction abstraction exists.
        match status {
            WorkerStatus::Success {
                tenant_id: _,
                dag_run_id,
                task_id,
                task_run_id,
                output,
            } => {
                if let Some(mut task_run) = self.task_runs.get(task_run_id).await? {
                    if task_run.status.is_terminal() {
                        return Ok(());
                    }
                    task_run.status = TaskRunStatus::Success;
                    task_run.error = None;
                    self.task_runs.save(task_run).await?;

                    let mut task = match self.tasks.get(task_id).await? {
                        Some(t) => t,
                        None => return Ok(()),
                    };
                    task.status = TaskStatus::Success;
                    task.result = output.clone();
                    self.tasks.save(task.clone()).await?;

                    // Fan-out: if chunker produced chunk_set_ids, create embed tasks.
                    if let Some(out) = output
                        && let Some(new_tasks) =
                            Self::extract_embed_tasks(out, dag_run_id, task.tenant_id)?
                    {
                        for embed_task in new_tasks {
                            self.tasks.save(embed_task).await?;
                        }
                    }

                    if let Some(mut dag_run) = self.dag_runs.get(dag_run_id).await? {
                        let statuses: Vec<TaskStatus> = self
                            .tasks
                            .list_by_dag_run(dag_run_id)
                            .await?
                            .into_iter()
                            .map(|t| t.status)
                            .collect();
                        let final_status = dag_status_from_tasks(&statuses);
                        if final_status != dag_run.status {
                            dag_run.status = final_status;
                            self.dag_runs.save(dag_run).await?;
                        }
                    }
                }
            }
            WorkerStatus::Failed {
                tenant_id: _,
                dag_run_id,
                task_id,
                task_run_id,
                error,
                error_kind,
            } => {
                if let Some(mut task_run) = self.task_runs.get(task_run_id).await? {
                    if task_run.status.is_terminal() {
                        return Ok(());
                    }
                    task_run.status = TaskRunStatus::Failed;
                    task_run.error = Some(error.clone());
                    self.task_runs.save(task_run).await?;

                    if let Some(mut task) = self.tasks.get(task_id).await? {
                        let attempts = self.task_runs.list_by_task(task_id).await?;
                        let attempt_count = attempts.len() as u32;

                        if self.retry_policy.should_retry(attempt_count) {
                            task.status = TaskStatus::Pending;
                            debug!(
                                task = ?task.id,
                                attempt = attempt_count,
                                "retrying task after failure"
                            );
                        } else {
                            task.status = TaskStatus::Failed;
                            task.result = Some(serde_json::json!({
                                "error": error,
                                "error_kind": error_kind,
                            }));
                        }

                        self.tasks.save(task).await?;
                    }

                    if let Some(mut dag_run) = self.dag_runs.get(dag_run_id).await? {
                        let statuses: Vec<TaskStatus> = self
                            .tasks
                            .list_by_dag_run(dag_run_id)
                            .await?
                            .into_iter()
                            .map(|t| t.status)
                            .collect();
                        let final_status = dag_status_from_tasks(&statuses);
                        if final_status != dag_run.status {
                            dag_run.status = final_status;
                            self.dag_runs.save(dag_run).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl<D, T, R> DbScheduler<D, T, R>
where
    D: DagRunRepo + 'static,
    T: TaskRepo + 'static,
    R: TaskRunRepo + 'static,
{
    fn extract_embed_tasks(
        output: serde_json::Value,
        dag_run_id: DagRunId,
        tenant_id: contexide_core::prelude::TenantId,
    ) -> Result<Option<Vec<Task>>> {
        let chunk_sets = output
            .get("chunk_set_ids")
            .and_then(|v| v.as_array())
            .cloned();

        if let Some(list) = chunk_sets {
            let mut tasks = Vec::new();
            for cs in list {
                if let Some(cs_str) = cs.as_str()
                    && let Ok(uuid) = uuid::Uuid::parse_str(cs_str)
                {
                    tasks.push(Task {
                        id: contexide_core::prelude::TaskId::new(),
                        dag_run_id,
                        tenant_id,
                        kind: "embedder".to_string(),
                        status: TaskStatus::Pending,
                        payload: serde_json::json!({ "chunk_set_id": uuid }),
                        result: None,
                        max_attempts: None,
                        retry_policy: "never".into(),
                        retry_params: serde_json::json!({}),
                        priority: 0,
                        execution_policy_override: None,
                    });
                }
            }
            return Ok(Some(tasks));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TaskRunRepo;
    use contexide_core::prelude::{TaskId, TenantId};
    use contexide_storage_pg::traits::Repository;
    use contexide_storage_pg::workflows::DagRun;
    use contexide_storage_pg::workflows::mem::{MemDagRunRepo, MemTaskRepo, MemTaskRunRepo};

    fn seed_simple_run(
        dag_runs: &MemDagRunRepo,
        tasks: &MemTaskRepo,
        tenant: TenantId,
    ) -> (DagRunId, TaskId, TaskId) {
        let dag_run = DagRun {
            id: DagRunId::new(),
            tenant_id: tenant,
            workflow_key: "ingest_default".into(),
            status: DagRunStatus::Created,
            params: serde_json::json!({}),
            error: None,
            execution_policy: None,
            execution_policy_version: 1,
        };
        let dag_run_id = dag_run.id;
        futures::executor::block_on(dag_runs.save(dag_run)).unwrap();

        let extractor = Task {
            id: TaskId::new(),
            dag_run_id,
            tenant_id: tenant,
            kind: "extractor".into(),
            status: TaskStatus::Pending,
            payload: serde_json::json!({"doc": "in"}),
            result: None,
            max_attempts: None,
            retry_policy: "never".into(),
            retry_params: serde_json::json!({}),
            priority: 0,
            execution_policy_override: None,
        };
        let normalizer = Task {
            id: TaskId::new(),
            dag_run_id,
            tenant_id: tenant,
            kind: "normalizer".into(),
            status: TaskStatus::Pending,
            payload: serde_json::Value::Null,
            result: None,
            max_attempts: None,
            retry_policy: "never".into(),
            retry_params: serde_json::json!({}),
            priority: 0,
            execution_policy_override: None,
        };

        futures::executor::block_on(tasks.save(extractor.clone())).unwrap();
        futures::executor::block_on(tasks.save(normalizer.clone())).unwrap();

        (dag_run_id, extractor.id, normalizer.id)
    }

    #[tokio::test]
    async fn schedules_first_pending_task() {
        let dag_runs = Arc::new(MemDagRunRepo::new());
        let tasks = Arc::new(MemTaskRepo::new());
        let task_runs = Arc::new(MemTaskRunRepo::new());
        let tenant = TenantId::new();
        let (_dag_run_id, extractor_id, _) = seed_simple_run(&dag_runs, &tasks, tenant);

        let scheduler = DbScheduler::new(
            Arc::clone(&dag_runs),
            Arc::clone(&tasks),
            Arc::clone(&task_runs),
            RetryPolicy { max_attempts: 2 },
        );

        let ready = scheduler.schedule_ready_tasks().await.unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, extractor_id);

        let task = tasks.get(extractor_id).await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Running);

        let runs = TaskRunRepo::list_by_task(task_runs.as_ref(), extractor_id)
            .await
            .unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, TaskRunStatus::Running);
    }

    #[tokio::test]
    async fn handles_success_and_completes_dag() {
        let dag_runs = Arc::new(MemDagRunRepo::new());
        let tasks = Arc::new(MemTaskRepo::new());
        let task_runs = Arc::new(MemTaskRunRepo::new());
        let tenant = TenantId::new();
        let (dag_run_id, extractor_id, _) = seed_simple_run(&dag_runs, &tasks, tenant);

        let scheduler = DbScheduler::new(
            Arc::clone(&dag_runs),
            Arc::clone(&tasks),
            Arc::clone(&task_runs),
            RetryPolicy { max_attempts: 2 },
        );

        let ready = scheduler.schedule_ready_tasks().await.unwrap();
        let task_run_id = ready[0].task_run_id;

        scheduler
            .handle_worker_status(WorkerStatus::Success {
                tenant_id: tenant,
                dag_run_id,
                task_id: extractor_id,
                task_run_id,
                output: Some(serde_json::json!({"ok": true})),
            })
            .await
            .unwrap();

        let task = tasks.get(extractor_id).await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Success);
        let dag = dag_runs.get(dag_run_id).await.unwrap().unwrap();
        assert_eq!(dag.status, DagRunStatus::Running); // other tasks still pending
    }

    #[tokio::test]
    async fn retries_then_marks_failed() {
        let dag_runs = Arc::new(MemDagRunRepo::new());
        let tasks = Arc::new(MemTaskRepo::new());
        let task_runs = Arc::new(MemTaskRunRepo::new());
        let tenant = TenantId::new();
        let (dag_run_id, extractor_id, _) = seed_simple_run(&dag_runs, &tasks, tenant);

        let scheduler = DbScheduler::new(
            Arc::clone(&dag_runs),
            Arc::clone(&tasks),
            Arc::clone(&task_runs),
            RetryPolicy { max_attempts: 1 },
        );

        let ready = scheduler.schedule_ready_tasks().await.unwrap();
        let task_run_id = ready[0].task_run_id;

        scheduler
            .handle_worker_status(WorkerStatus::Failed {
                tenant_id: tenant,
                dag_run_id,
                task_id: extractor_id,
                task_run_id,
                error: "network".into(),
                error_kind: None,
            })
            .await
            .unwrap();

        let task = tasks.get(extractor_id).await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Failed);

        let dag = dag_runs.get(dag_run_id).await.unwrap().unwrap();
        assert_eq!(dag.status, DagRunStatus::Failed);
    }

    #[tokio::test]
    async fn fanout_creates_embed_tasks_from_chunk_output() {
        let dag_runs = Arc::new(MemDagRunRepo::new());
        let tasks = Arc::new(MemTaskRepo::new());
        let task_runs = Arc::new(MemTaskRunRepo::new());
        let tenant = TenantId::new();
        let (dag_run_id, chunk_task_id) = {
            let dag_run = DagRun {
                id: DagRunId::new(),
                tenant_id: tenant,
                workflow_key: "fanout".into(),
                status: DagRunStatus::Running,
                params: serde_json::json!({}),
                error: None,
                execution_policy: None,
                execution_policy_version: 1,
            };
            let dag_run_id = dag_run.id;
            dag_runs.save(dag_run).await.unwrap();
            let chunk_task = Task {
                id: TaskId::new(),
                dag_run_id,
                tenant_id: tenant,
                kind: "chunker".into(),
                status: TaskStatus::Pending,
                payload: serde_json::json!({}),
                result: None,
                max_attempts: None,
                retry_policy: "never".into(),
                retry_params: serde_json::json!({}),
                priority: 0,
                execution_policy_override: None,
            };
            tasks.save(chunk_task.clone()).await.unwrap();
            (dag_run_id, chunk_task.id)
        };

        let scheduler = DbScheduler::new(
            Arc::clone(&dag_runs),
            Arc::clone(&tasks),
            Arc::clone(&task_runs),
            RetryPolicy { max_attempts: 1 },
        );

        // Pretend scheduler scheduled the chunk task.
        tasks
            .save(Task {
                id: chunk_task_id,
                dag_run_id,
                tenant_id: tenant,
                kind: "chunker".into(),
                status: TaskStatus::Running,
                payload: serde_json::json!({}),
                result: None,
                max_attempts: None,
                retry_policy: "never".into(),
                retry_params: serde_json::json!({}),
                priority: 0,
                execution_policy_override: None,
            })
            .await
            .unwrap();
        let tr = TaskRun {
            id: TaskRunId::new(),
            task_id: chunk_task_id,
            tenant_id: tenant,
            status: TaskRunStatus::Running,
            attempt_no: 0,
            error: None,
            worker_label: None,
            error_code: None,
            error_message: None,
            transient_error: None,
        };
        task_runs.save(tr.clone()).await.unwrap();

        let chunk_set_id = uuid::Uuid::now_v7();
        let first_run = task_runs.list_by_task(chunk_task_id)[0].id;

        scheduler
            .handle_worker_status(WorkerStatus::Success {
                tenant_id: tenant,
                dag_run_id,
                task_id: chunk_task_id,
                task_run_id: first_run,
                output: Some(serde_json::json!({
                    "chunk_set_ids": [chunk_set_id.to_string()]
                })),
            })
            .await
            .unwrap();

        // Embed task should be created in Pending state.
        let all_tasks = tasks.list_all();
        assert!(all_tasks.iter().any(|t| t.kind == "embedder"));
        let embed = all_tasks.iter().find(|t| t.kind == "embedder").unwrap();
        assert_eq!(embed.status, TaskStatus::Pending);
        assert_eq!(
            embed
                .payload
                .get("chunk_set_id")
                .and_then(|v| v.as_str())
                .unwrap(),
            chunk_set_id.to_string()
        );
    }
}

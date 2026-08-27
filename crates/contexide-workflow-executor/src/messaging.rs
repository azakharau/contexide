//! Messaging glue: bridges scheduler decisions with the transport layer.
//!
//! This module is transport-agnostic. It defines the message shapes sent to
//! workers, the subjects used for routing, and a small client trait that can
//! be backed by NATS JetStream or any other pub/sub. The `WorkflowMessaging`
//! struct is the only place where scheduling and messaging touch.

use std::sync::Arc;

use async_trait::async_trait;
use contexide_core::errors::Result;
use futures::{StreamExt, stream::BoxStream};

use crate::domain::WorkerStatus;
use crate::scheduler::Scheduler;
use contexide_messaging_nats::{
    WorkerRequest, WorkerStatus as MsgWorkerStatus, worker_done_subject, worker_request_subject,
};

/// Minimal transport-agnostic messaging client.
#[async_trait]
pub trait MessagingClient: Send + Sync {
    async fn publish(&self, subject: &str, payload: &[u8]) -> Result<()>;
    async fn subscribe(&self, subject: &str)
    -> Result<BoxStream<'static, Result<IncomingMessage>>>;
}

/// Raw incoming message from the messaging backend.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub subject: String,
    pub data: Vec<u8>,
}

/// Glue between Scheduler and the messaging transport.
pub struct WorkflowMessaging<S: Scheduler> {
    pub scheduler: Arc<S>,
    pub client: Arc<dyn MessagingClient>,
}

impl<S: Scheduler> WorkflowMessaging<S> {
    pub fn new(scheduler: Arc<S>, client: Arc<dyn MessagingClient>) -> Self {
        Self { scheduler, client }
    }

    /// Pull ready tasks from the scheduler and publish request messages for workers.
    pub async fn pump_requests(&self) -> Result<()> {
        let ready = self.scheduler.schedule_ready_tasks().await?;
        for task in ready {
            let msg = WorkerRequest {
                tenant_id: task.tenant_id,
                dag_run_id: task.dag_run_id.into(),
                task_id: task.task_id.into(),
                task_run_id: task.task_run_id.into(),
                kind: task.kind.clone(),
                payload: task.payload.clone(),
            };

            let subject = worker_request_subject("contexide.workflow", &task.kind);
            let data = serde_json::to_vec(&msg)?;
            self.client.publish(&subject, &data).await?;
        }
        Ok(())
    }

    /// Listen for worker completion messages and forward them to the scheduler.
    pub async fn listen_worker_statuses(&self) -> Result<()> {
        let mut stream = self
            .client
            .subscribe(&worker_done_subject("contexide.workflow", "*"))
            .await?;
        while let Some(msg) = stream.next().await {
            let msg = msg?;
            let done: MsgWorkerStatus = serde_json::from_slice(&msg.data)?;
            let status = if done.success {
                WorkerStatus::Success {
                    tenant_id: done.tenant_id,
                    dag_run_id: done.dag_run_id.into(),
                    task_id: done.task_id.into(),
                    task_run_id: done.task_run_id.into(),
                    output: done.output,
                }
            } else {
                WorkerStatus::Failed {
                    tenant_id: done.tenant_id,
                    dag_run_id: done.dag_run_id.into(),
                    task_id: done.task_id.into(),
                    task_run_id: done.task_run_id.into(),
                    error: done.error.unwrap_or_else(|| "unknown error".to_string()),
                    error_kind: done.error_kind,
                }
            };
            self.scheduler.handle_worker_status(status).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::RetryPolicy;
    use crate::scheduler::DbScheduler;
    use contexide_core::prelude::{DagRunId, TaskId, TaskRunId, TenantId};
    use contexide_messaging_nats::{WorkerStatus as MsgWorkerStatus, worker_done_subject};
    use contexide_storage_pg::traits::Repository;
    use contexide_storage_pg::workflows::mem::{MemDagRunRepo, MemTaskRepo, MemTaskRunRepo};
    use contexide_workflow_core::{DagRunStatus, TaskRunStatus, TaskStatus};
    use futures::stream;
    use std::sync::Mutex;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    struct MockMessaging {
        published: Mutex<Vec<(String, Vec<u8>)>>,
        incoming: Mutex<Option<mpsc::Receiver<IncomingMessage>>>,
    }

    impl MockMessaging {
        fn new() -> Self {
            Self {
                published: Mutex::new(Vec::new()),
                incoming: Mutex::new(None),
            }
        }

        fn with_incoming(msgs: Vec<IncomingMessage>) -> Self {
            let (tx, rx) = mpsc::channel(16);
            for m in msgs {
                let _ = tx.try_send(m);
            }
            Self {
                published: Mutex::new(Vec::new()),
                incoming: Mutex::new(Some(rx)),
            }
        }
    }

    #[async_trait]
    impl MessagingClient for MockMessaging {
        async fn publish(&self, subject: &str, payload: &[u8]) -> Result<()> {
            self.published
                .lock()
                .unwrap()
                .push((subject.to_string(), payload.to_vec()));
            Ok(())
        }

        async fn subscribe(
            &self,
            _subject: &str,
        ) -> Result<BoxStream<'static, Result<IncomingMessage>>> {
            if let Some(rx) = self.incoming.lock().unwrap().take() {
                let stream = ReceiverStream::new(rx).map(Ok).boxed();
                Ok(stream)
            } else {
                Ok(stream::empty().boxed())
            }
        }
    }

    #[tokio::test]
    async fn publishes_ready_tasks() {
        let dag_runs = Arc::new(MemDagRunRepo::new());
        let tasks = Arc::new(MemTaskRepo::new());
        let task_runs = Arc::new(MemTaskRunRepo::new());
        let tenant = TenantId::new();

        let dag = contexide_storage_pg::workflows::DagRun {
            id: DagRunId::new(),
            tenant_id: tenant,
            workflow_key: "ingest_default".into(),
            status: DagRunStatus::Created,
            params: serde_json::json!({}),
            error: None,
            execution_policy: None,
            execution_policy_version: 1,
        };
        dag_runs.save(dag.clone()).await.unwrap();

        let task = contexide_storage_pg::workflows::Task {
            id: TaskId::new(),
            dag_run_id: dag.id,
            tenant_id: tenant,
            kind: "extractor".into(),
            status: TaskStatus::Pending,
            payload: serde_json::json!({"k": "v"}),
            result: None,
            max_attempts: None,
            retry_policy: "never".into(),
            retry_params: serde_json::json!({}),
            priority: 0,
            execution_policy_override: None,
        };
        tasks.save(task.clone()).await.unwrap();

        let scheduler = Arc::new(DbScheduler::new(
            Arc::clone(&dag_runs),
            Arc::clone(&tasks),
            Arc::clone(&task_runs),
            RetryPolicy { max_attempts: 2 },
        ));
        let client = Arc::new(MockMessaging::new());
        let messaging = WorkflowMessaging::new(
            Arc::clone(&scheduler),
            client.clone() as Arc<dyn MessagingClient>,
        );

        messaging.pump_requests().await.unwrap();

        let records = client.published.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "contexide.workflow.extractor.request");
    }

    #[tokio::test]
    async fn listens_and_forwards_done_messages() {
        let dag_runs = Arc::new(MemDagRunRepo::new());
        let tasks = Arc::new(MemTaskRepo::new());
        let task_runs = Arc::new(MemTaskRunRepo::new());
        let tenant = TenantId::new();
        let dag_run_id = DagRunId::new();
        let task_id = TaskId::new();
        let task_run_id = TaskRunId::new();

        let dag = contexide_storage_pg::workflows::DagRun {
            id: dag_run_id,
            tenant_id: tenant,
            workflow_key: "ingest_default".into(),
            status: DagRunStatus::Running,
            params: serde_json::json!({}),
            error: None,
            execution_policy: None,
            execution_policy_version: 1,
        };
        dag_runs.save(dag).await.unwrap();

        let task = contexide_storage_pg::workflows::Task {
            id: task_id,
            dag_run_id,
            tenant_id: tenant,
            kind: "extractor".into(),
            status: TaskStatus::Running,
            payload: serde_json::json!({}),
            result: None,
            max_attempts: None,
            retry_policy: "never".into(),
            retry_params: serde_json::json!({}),
            priority: 0,
            execution_policy_override: None,
        };
        tasks.save(task).await.unwrap();

        let run = contexide_storage_pg::workflows::TaskRun {
            id: task_run_id,
            task_id,
            tenant_id: tenant,
            status: TaskRunStatus::Running,
            attempt_no: 0,
            error: None,
            worker_label: None,
            error_code: None,
            error_message: None,
            transient_error: None,
        };
        task_runs.save(run).await.unwrap();

        let done = MsgWorkerStatus {
            tenant_id: tenant,
            dag_run_id: dag_run_id.into(),
            task_id: task_id.into(),
            task_run_id: task_run_id.into(),
            kind: "extractor".into(),
            success: true,
            output: Some(serde_json::json!({"ok": true})),
            error: None,
            error_kind: None,
            result_meta: None,
        };
        let payload = serde_json::to_vec(&done).unwrap();
        let incoming = vec![IncomingMessage {
            subject: worker_done_subject("contexide.workflow", "extractor"),
            data: payload,
        }];

        let client = Arc::new(MockMessaging::with_incoming(incoming));
        let messaging = WorkflowMessaging::new(
            Arc::new(DbScheduler::new(
                Arc::clone(&dag_runs),
                Arc::clone(&tasks),
                Arc::clone(&task_runs),
                RetryPolicy { max_attempts: 2 },
            )),
            client as Arc<dyn MessagingClient>,
        );

        messaging.listen_worker_statuses().await.unwrap();

        let task = tasks.get(task_id).await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Success);
    }
}

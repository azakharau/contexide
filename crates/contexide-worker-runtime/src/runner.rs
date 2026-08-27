use std::sync::Arc;

use contexide_core::errors::Result;
use contexide_messaging_nats::{JetStreamClient, JetStreamMessage, WorkerRequest, WorkerStatus};
use futures::StreamExt;
use serde_json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{Instrument, error, info, warn};

use crate::config::WorkerRuntimeConfig;
use crate::context::WorkerContext;
use crate::signals::wait_for_shutdown_signal;
use crate::traits::DynWorkerHandler;

/// Builder for `WorkerRunner`.
pub struct WorkerRunnerBuilder {
    cfg: WorkerRuntimeConfig,
    handler: DynWorkerHandler,
    jetstream: Option<JetStreamClient>,
}

impl WorkerRunnerBuilder {
    pub fn new(cfg: WorkerRuntimeConfig, handler: DynWorkerHandler) -> Self {
        Self {
            cfg,
            handler,
            jetstream: None,
        }
    }

    /// Inject an already-constructed JetStream client.
    pub fn with_jetstream(mut self, js: JetStreamClient) -> Self {
        self.jetstream = Some(js);
        self
    }

    /// Build the runner, creating JetStream client from messaging config
    /// if it was not supplied explicitly.
    pub async fn build(self) -> Result<WorkerRunner> {
        let js = match self.jetstream {
            Some(js) => js,
            None => {
                let msg_cfg = contexide_config::load_messaging()?;
                contexide_messaging_nats::connect_jetstream(&msg_cfg).await?
            }
        };

        let ctx = WorkerContext::new(self.cfg.clone(), js.clone());

        Ok(WorkerRunner {
            cfg: self.cfg,
            ctx,
            handler: self.handler,
        })
    }
}

/// Long-running worker process that consumes `WorkerRequest` messages
/// and dispatches them to the handler with bounded concurrency.
pub struct WorkerRunner {
    cfg: WorkerRuntimeConfig,
    ctx: WorkerContext,
    handler: DynWorkerHandler,
}

impl WorkerRunner {
    /// Run the worker loop until a shutdown signal is received or
    /// a fatal error occurs.
    pub async fn run(self) -> Result<()> {
        let mut subscription = self
            .ctx
            .jetstream
            .subscribe(&self.cfg.subject_request)
            .await?;

        let semaphore = Arc::new(Semaphore::new(self.cfg.max_concurrency));
        let mut tasks = JoinSet::new();

        let shutdown = wait_for_shutdown_signal();
        tokio::pin!(shutdown);

        info!(
            worker = %self.cfg.worker_kind,
            subject = %self.cfg.subject_request,
            "worker runtime started"
        );

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!(worker = %self.cfg.worker_kind, "shutdown signal received");
                    break;
                }
                maybe_msg = subscription.next() => {
                    match maybe_msg {
                        Some(Ok(msg)) => {
                            let permit = match semaphore.clone().acquire_owned().await {
                                Ok(p) => p,
                                Err(e) => {
                                    error!(?e, "failed to acquire semaphore permit");
                                    continue;
                                }
                            };
                            let ctx = self.ctx.clone();
                            let handler = self.handler.clone();
                            let subject_done = self.cfg.subject_done.clone();
                            tasks.spawn(async move {
                                let _permit = permit;
                                if let Err(err) = handle_one(handler, ctx, msg, &subject_done).await {
                                    error!(?err, "failed to process worker message");
                                }
                            });
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "subscription yielded error; continuing");
                        }
                        None => {
                            warn!("subscription closed; breaking loop");
                            break;
                        }
                    }
                }
            }
        }

        // Stop pulling; wait for in-flight tasks up to grace period.
        match tokio::time::timeout(self.cfg.shutdown_grace, async {
            while let Some(res) = tasks.join_next().await {
                if let Err(e) = res {
                    error!(?e, "join error from task");
                }
            }
        })
        .await
        {
            Ok(_) => info!("all in-flight tasks finished"),
            Err(_) => warn!("grace period elapsed; exiting with tasks still in flight"),
        }

        Ok(())
    }
}

async fn handle_one(
    handler: DynWorkerHandler,
    ctx: WorkerContext,
    msg: JetStreamMessage,
    subject_done: &str,
) -> Result<()> {
    let raw = msg.data();
    let parsed: WorkerRequest = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "failed to decode WorkerRequest; NACK");
            msg.nack().await?;
            return Ok(());
        }
    };

    let span = tracing::info_span!(
        "worker.handle",
        task_id = %parsed.task_id,
        task_run_id = %parsed.task_run_id,
        dag_run_id = %parsed.dag_run_id,
        worker = %ctx.worker_kind()
    );

    let result = async {
        let status: WorkerStatus = handler.handle(&ctx, parsed).await?;
        let payload = serde_json::to_vec(&status)?;
        ctx.jetstream.publish(subject_done, &payload).await?;
        msg.ack().await
    };

    match result.instrument(span).await {
        Ok(_) => Ok(()),
        Err(e) => {
            warn!(error = %e, "handler or publish failed; NACK message");
            msg.nack().await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::WorkerHandler;
    use contexide_core::ids::{DagRunId, TaskId, TaskRunId, TenantId};
    use contexide_messaging_nats::{worker_done_subject, worker_request_subject};
    use std::time::Duration;
    use tokio::sync::Mutex;

    struct DummyHandler {
        calls: Arc<Mutex<Vec<WorkerRequest>>>,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl WorkerHandler for DummyHandler {
        async fn handle(&self, _ctx: &WorkerContext, req: WorkerRequest) -> Result<WorkerStatus> {
            tokio::time::sleep(self.delay).await;
            self.calls.lock().await.push(req.clone());
            Ok(WorkerStatus {
                tenant_id: req.tenant_id,
                dag_run_id: req.dag_run_id,
                task_id: req.task_id,
                task_run_id: req.task_run_id,
                kind: req.kind.clone(),
                success: true,
                output: Some(serde_json::json!({"ok": true})),
                error: None,
                error_kind: None,
                result_meta: None,
            })
        }
    }

    #[tokio::test]
    async fn dispatches_requests_and_honors_concurrency() {
        let prefix = "contexide.workflow";
        let worker_kind = "dummy";
        let req_subject = worker_request_subject(prefix, worker_kind);
        let done_subject = worker_done_subject(prefix, worker_kind);

        // Prepare fake JetStream with two messages.
        let (js, published, shutdown_tx, shutdown_rx) =
            contexide_messaging_nats::mock_jetstream_with_shutdown(
                vec![
                    WorkerRequest {
                        tenant_id: TenantId::new(),
                        dag_run_id: DagRunId::new().into(),
                        task_id: TaskId::new().into(),
                        task_run_id: TaskRunId::new().into(),
                        kind: worker_kind.to_string(),
                        payload: serde_json::json!({"i": 1}),
                    },
                    WorkerRequest {
                        tenant_id: TenantId::new(),
                        dag_run_id: DagRunId::new().into(),
                        task_id: TaskId::new().into(),
                        task_run_id: TaskRunId::new().into(),
                        kind: worker_kind.to_string(),
                        payload: serde_json::json!({"i": 2}),
                    },
                ],
                &req_subject,
            );

        let calls = Arc::new(Mutex::new(Vec::new()));
        let handler = Arc::new(DummyHandler {
            calls: calls.clone(),
            delay: Duration::from_millis(50),
        });

        let cfg = WorkerRuntimeConfig {
            worker_kind: worker_kind.to_string(),
            stream: "contexide.workflow".into(),
            subject_request: req_subject.clone(),
            subject_done: done_subject.clone(),
            max_concurrency: 1,
            shutdown_grace: Duration::from_secs(1),
        };

        let runner = WorkerRunner {
            cfg,
            ctx: WorkerContext::new(
                WorkerRuntimeConfig {
                    worker_kind: worker_kind.to_string(),
                    stream: "contexide.workflow".into(),
                    subject_request: req_subject.clone(),
                    subject_done: done_subject.clone(),
                    max_concurrency: 1,
                    shutdown_grace: Duration::from_secs(1),
                },
                js.clone(),
            ),
            handler,
        };

        crate::signals::install_mock_shutdown(shutdown_rx);

        let task = tokio::spawn(runner.run());
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Trigger shutdown via installed channel
        let _ = shutdown_tx.send(()).await;
        task.await.unwrap().unwrap();

        let handled = calls.lock().await.clone();
        assert_eq!(handled.len(), 2);

        let published = published.lock().await.clone();
        assert_eq!(published.len(), 2);
        assert!(published.iter().all(|(s, _)| s == &done_subject));
    }
}

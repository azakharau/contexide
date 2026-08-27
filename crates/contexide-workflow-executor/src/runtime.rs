//! Runtime wrapper that runs scheduling and status loops concurrently.
//!
//! Binaries should wire repositories, messaging client and config, then call
//! `WorkflowExecutor::run()`. Graceful shutdown hooks can be added later.

use anyhow::anyhow;
use contexide_core::errors::{Error, Result};
use std::sync::Arc;

use crate::{config::ExecutorConfig, messaging::WorkflowMessaging, scheduler::Scheduler};

/// High-level executor that runs the scheduling loop and listens for worker results.
pub struct WorkflowExecutor<S: Scheduler> {
    #[allow(dead_code)]
    scheduler: Arc<S>,
    messaging: Arc<WorkflowMessaging<S>>,
    config: ExecutorConfig,
}

impl<S: Scheduler + 'static> WorkflowExecutor<S> {
    pub fn new(
        scheduler: Arc<S>,
        messaging: Arc<WorkflowMessaging<S>>,
        config: ExecutorConfig,
    ) -> Self {
        Self {
            scheduler,
            messaging,
            config,
        }
    }

    pub async fn run(self) -> Result<()> {
        let poll = self.config.poll_interval;
        let messaging_requests = Arc::clone(&self.messaging);
        let messaging_status = Arc::clone(&self.messaging);

        let scheduling_loop = tokio::spawn(async move {
            loop {
                messaging_requests.pump_requests().await?;
                tokio::time::sleep(poll).await;
            }
            #[allow(unreachable_code)]
            Ok::<(), contexide_core::errors::Error>(())
        });

        let status_loop =
            tokio::spawn(async move { messaging_status.listen_worker_statuses().await });

        tokio::select! {
            res = scheduling_loop => res.map_err(|e| Error::Other(anyhow!(e)))??,
            res = status_loop => res.map_err(|e| Error::Other(anyhow!(e)))??,
        };

        Ok(())
    }
}

use std::sync::Arc;

use contexide_core::errors::Result;
use contexide_messaging_nats::{WorkerRequest, WorkerStatus};

use crate::context::WorkerContext;

/// Domain-specific handler that processes one `WorkerRequest`.
#[async_trait::async_trait]
pub trait WorkerHandler: Send + Sync + 'static {
    /// Process a single worker request and return a `WorkerStatus`
    /// to be published back to JetStream.
    ///
    /// Handlers should be idempotent with respect to `task_run_id`
    /// when feasible, because messages may be delivered more than once.
    async fn handle(&self, ctx: &WorkerContext, req: WorkerRequest) -> Result<WorkerStatus>;
}

/// Trait-object alias for wiring with runtime.
pub type DynWorkerHandler = Arc<dyn WorkerHandler>;

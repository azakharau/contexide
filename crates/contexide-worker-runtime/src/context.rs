use contexide_messaging_nats::JetStreamClient;
use std::sync::Arc;

use crate::config::WorkerRuntimeConfig;

/// Shared runtime context available to worker handlers.
///
/// This context intentionally exposes only cross-cutting concerns (messaging,
/// runtime config). Domain-specific deps (storage, blob, vector, embeddings)
/// should be injected by the worker binary into the handler itself.
#[derive(Clone)]
pub struct WorkerContext {
    /// Worker runtime configuration (subjects, concurrency hints).
    pub config: Arc<WorkerRuntimeConfig>,
    /// JetStream client for publishing statuses or auxiliary messages.
    pub jetstream: JetStreamClient,
}

impl WorkerContext {
    /// Construct a new context.
    pub fn new(config: WorkerRuntimeConfig, jetstream: JetStreamClient) -> Self {
        Self {
            config: Arc::new(config),
            jetstream,
        }
    }

    /// Convenience accessor for worker kind string.
    pub fn worker_kind(&self) -> &str {
        &self.config.worker_kind
    }
}

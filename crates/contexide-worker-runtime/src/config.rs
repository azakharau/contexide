use std::time::Duration;

use contexide_config::MessagingConfig;

/// Worker runtime configuration (messaging + concurrency).
#[derive(Debug, Clone)]
pub struct WorkerRuntimeConfig {
    /// Logical worker kind, e.g. "extractor", "normalizer".
    ///
    /// Used for logging and deriving default subject names.
    pub worker_kind: String,
    /// JetStream stream name used for workflow worker messages.
    pub stream: String,
    /// Subject for incoming worker requests (e.g. `contexide.workflow.extractor.request`).
    pub subject_request: String,
    /// Subject for outgoing worker statuses (e.g. `contexide.workflow.extractor.done`).
    pub subject_done: String,
    /// Maximum number of in-flight tasks processed concurrently.
    pub max_concurrency: usize,
    /// Max duration to wait for graceful shutdown after SIGINT/SIGTERM.
    pub shutdown_grace: Duration,
}

impl WorkerRuntimeConfig {
    /// Build config from messaging settings and a worker kind.
    ///
    /// This is the recommended way to derive subject names consistently across workers.
    pub fn from_messaging(msg: &MessagingConfig, worker_kind: &str) -> Self {
        let prefix = msg.workflow_prefix.trim_end_matches('.');
        Self {
            worker_kind: worker_kind.to_string(),
            stream: msg.workflow_stream.clone(),
            subject_request: format!("{prefix}.{worker_kind}.request"),
            subject_done: format!("{prefix}.{worker_kind}.done"),
            max_concurrency: msg.worker_default_concurrency.unwrap_or(4),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

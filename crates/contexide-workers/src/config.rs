//! Worker-facing configuration bundle.
//!
//! The intent is to keep binaries small: they load this bundle once,
//! derive `WorkerRuntimeConfig`, and pass the pieces into domain handlers.

use anyhow::Result;
use contexide_config::{
    BlobStorageConfig, EmbeddingsConfig, MessagingConfig, StorageConfig, VectorConfig,
    WorkersTuning, load_blob_storage, load_embeddings, load_messaging, load_storage, load_vector,
    load_workers,
};
use contexide_worker_runtime::WorkerRuntimeConfig;

/// Configuration required by a worker process.
#[derive(Debug, Clone)]
pub struct WorkerAppConfig {
    pub messaging: MessagingConfig,
    pub workers: WorkersTuning,
    pub storage: StorageConfig,
    pub blob_storage: Option<BlobStorageConfig>,
    pub vector: Option<VectorConfig>,
    pub embeddings: Option<EmbeddingsConfig>,
}

impl WorkerAppConfig {
    /// Load all worker-relevant configs from ENV.
    pub fn load() -> Result<Self> {
        Ok(Self {
            messaging: load_messaging()?,
            workers: load_workers()?,
            storage: load_storage()?,
            blob_storage: load_blob_storage().ok(),
            vector: load_vector().ok(),
            embeddings: load_embeddings().ok(),
        })
    }

    /// Pick a per-kind concurrency value from tuning, falling back to
    /// messaging default or 4.
    pub fn concurrency_for(&self, worker_kind: &str) -> usize {
        match worker_kind {
            "extractor" => self.workers.extract,
            "normalizer" => self.workers.normalize,
            "chunker" => self.workers.chunk,
            "embedder" => self.workers.embed,
            "indexer" => self.workers.index,
            _ => self.messaging.worker_default_concurrency.unwrap_or(4),
        }
    }

    /// Build runtime config for a given worker kind using messaging subjects
    /// and tuned concurrency.
    pub fn runtime_config(&self, worker_kind: &str) -> WorkerRuntimeConfig {
        let mut cfg = WorkerRuntimeConfig::from_messaging(&self.messaging, worker_kind);
        cfg.max_concurrency = self.concurrency_for(worker_kind);
        cfg
    }
}

#![allow(unused_imports)]
//! Minimal typed configuration from ENV only (plus optional `.env` for dev).
//!
//! Rules:
//! - Each crate has its own scoped prefix: `CONTEXIDE_<CRATE>_<KEY>`.
//! - Global fallback: `CONTEXIDE_<KEY>`.
//! - Legacy fallbacks for well-known keys (e.g., `DATABASE_URL`, `MINIO_*`).
//! - Then hard-coded defaults.
//!
//! Crate scopes supported here (keys are documented below):
//! - STORAGE
//! - BLOB_STORAGE
//! - VECTOR
//! - EMBEDDINGS
//! - API
//! - WORKERS
//!
//! Usage pattern per crate (static LazyLock recommended in the crate itself):
//! ```ignore
//! use std::sync::LazyLock;
//! use contexide_config::{load_storage, StorageConfig};
//!
//! pub static CONFIG: LazyLock<StorageConfig> = LazyLock::new(||
//!     load_storage().expect("STORAGE config is required")
//! );
//! ```
//!
//! Or load the full application config (for binaries that wire everything):
//! ```ignore
//! let app = contexide_config::load_app()?;
//! ```

use anyhow::Context;
use std::time::Duration;

// ---------- Common types ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEnv {
    Dev,
    Prod,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub env: RunEnv,
    pub storage: StorageConfig,
    pub blob_storage: BlobStorageConfig,
    pub vector: VectorConfig,
    pub embeddings: EmbeddingsConfig,
    pub api: ApiConfig,
    pub workers: WorkersTuning,
    pub messaging: MessagingConfig,
    pub workflow: WorkflowConfig,
}

// ---------- Group-specific typed configs ----------

#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Postgres URL for the storage repository.
    /// ENV (priority high → low):
    /// - CONTEXIDE_STORAGE_DATABASE_URL
    /// - CONTEXIDE_DATABASE_URL
    /// - DATABASE_URL
    pub database_url: String,
    /// Max pool size.
    /// ENV: CONTEXIDE_STORAGE_DB_MAX_CONNECTIONS → CONTEXIDE_DB_MAX_CONNECTIONS
    pub db_max_connections: u32,
    /// Optional override for reversible migrations directory.
    /// ENV: CONTEXIDE_STORAGE_MIGRATIONS_DIR → CONTEXIDE_MIGRATIONS_DIR
    pub migrations_dir_override: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BlobStorageConfig {
    /// MinIO/S3 endpoint, e.g., http://127.0.0.1:9000
    /// ENV: CONTEXIDE_BLOB_STORAGE_S3_ENDPOINT → MINIO_ENDPOINT
    pub endpoint: String,
    /// Access key (optional for AWS profiles/role-based).
    /// ENV: CONTEXIDE_BLOB_STORAGE_S3_ACCESS_KEY → MINIO_ACCESS_KEY
    pub access_key: String,
    /// Secret key (optional for AWS profiles/role-based).
    /// ENV: CONTEXIDE_BLOB_STORAGE_S3_SECRET_KEY → MINIO_SECRET_KEY
    pub secret_key: String,
    /// Bucket name (required).
    /// ENV: CONTEXIDE_BLOB_STORAGE_S3_BUCKET → MINIO_BUCKET
    pub bucket: String,
    /// Path-style addressing (true for MinIO).
    /// ENV: CONTEXIDE_BLOB_STORAGE_S3_PATH_STYLE → MINIO_PATH_STYLE
    pub path_style: bool,
    /// Optional region (can be empty for MinIO).
    /// ENV: CONTEXIDE_BLOB_STORAGE_S3_REGION → MINIO_REGION
    pub region: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VectorConfig {
    /// Vector DB endpoint, e.g., http://127.0.0.1:6333 (Qdrant)
    /// ENV: CONTEXIDE_VECTOR_VECTOR_ENDPOINT → VECTOR_ENDPOINT
    pub endpoint: String,
    /// Collection name prefix
    /// ENV: CONTEXIDE_VECTOR_VECTOR_COLLECTION_PREFIX → VECTOR_COLLECTION_PREFIX
    pub collection_prefix: String,
    /// "cosine" | "dot" | "l2"
    /// ENV: CONTEXIDE_VECTOR_VECTOR_METRIC → VECTOR_METRIC
    pub metric: String,
    /// Must be 1024 in our MVP.
    /// ENV: CONTEXIDE_VECTOR_VECTOR_DIM → VECTOR_DIM
    pub dim: usize,
}

#[derive(Debug, Clone)]
pub struct EmbeddingsConfig {
    /// "http" | "onnx" (start with one)
    /// ENV: CONTEXIDE_EMBEDDINGS_EMBEDDINGS_PROVIDER → EMBEDDINGS_PROVIDER
    pub provider: String,
    /// Model name (HTTP) or path (ONNX)
    /// ENV: CONTEXIDE_EMBEDDINGS_EMBEDDINGS_MODEL → EMBEDDINGS_MODEL
    pub model: String,
    /// Batch size for embedding calls
    /// ENV: CONTEXIDE_EMBEDDINGS_EMBEDDINGS_BATCH → EMBEDDINGS_BATCH
    pub batch: usize,
}

#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// Bind address for HTTP API, e.g. "0.0.0.0:8080"
    /// ENV: CONTEXIDE_API_HTTP_BIND → CONTEXIDE_HTTP_BIND
    pub bind: String,
}

#[derive(Debug, Clone)]
pub struct WorkersTuning {
    /// Tokio task concurrency per stage
    /// ENV: CONTEXIDE_WORKERS_WORKERS_* → WORKERS_*
    pub ingest: usize,
    pub extract: usize,
    pub normalize: usize,
    pub chunk: usize,
    pub embed: usize,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct MessagingConfig {
    /// NATS connection URL, e.g. nats://127.0.0.1:4222
    pub nats_url: String,
    /// Workflow message subject prefix, e.g. "contexide.workflow".
    pub workflow_prefix: String,
    /// JetStream stream name for workflow messages.
    pub workflow_stream: String,
    /// Default per-process concurrency hint for workers (optional).
    pub worker_default_concurrency: Option<usize>,
}

/// Workflow-related configuration loaded from environment.
#[derive(Debug, Clone)]
pub struct WorkflowConfig {
    pub max_running_tasks_total: u32,
    pub default_tenant_max_running_dag_runs: u32,
    pub default_tenant_max_running_tasks: u32,
    pub default_tenant_max_running_tasks_per_domain: u32,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// How often the scheduling loop should wake up to look for ready tasks.
    pub poll_interval: Duration,
    /// Soft limit on how many tasks we may run in parallel per domain.
    pub max_concurrent_per_domain: usize,
    /// Maximum number of retry attempts per task (MVP: global).
    pub max_retries: u32,
}

// ---------- Defaults ----------

fn defaults_env() -> RunEnv {
    RunEnv::Dev
}

fn defaults_storage() -> StorageConfig {
    StorageConfig {
        database_url: "postgres://postgres:postgres@localhost:5432/contexide".into(),
        db_max_connections: 10,
        migrations_dir_override: None,
    }
}

fn defaults_blob() -> BlobStorageConfig {
    BlobStorageConfig {
        endpoint: "http://127.0.0.1:9000".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        bucket: "contexide".into(),
        path_style: true,
        region: None,
    }
}

fn defaults_vector() -> VectorConfig {
    VectorConfig {
        endpoint: "http://127.0.0.1:6333".into(),
        collection_prefix: "contexide".into(),
        metric: "cosine".into(),
        dim: 1024,
    }
}

fn defaults_embeddings() -> EmbeddingsConfig {
    EmbeddingsConfig {
        provider: "http".into(),
        model: "bge-m3".into(),
        batch: 64,
    }
}

fn defaults_api() -> ApiConfig {
    ApiConfig {
        bind: "0.0.0.0:8080".into(),
    }
}

fn defaults_workers() -> WorkersTuning {
    WorkersTuning {
        ingest: 2,
        extract: 2,
        normalize: 2,
        chunk: 2,
        embed: 2,
        index: 2,
    }
}

fn defaults_messaging() -> MessagingConfig {
    MessagingConfig {
        nats_url: "nats://127.0.0.1:4222".into(),
        workflow_prefix: "contexide.workflow".into(),
        workflow_stream: "contexide.workflow".into(),
        worker_default_concurrency: Some(4),
    }
}

fn defaults_workflow() -> WorkflowConfig {
    WorkflowConfig {
        max_running_tasks_total: 64,
        default_tenant_max_running_dag_runs: 5,
        default_tenant_max_running_tasks: 32,
        default_tenant_max_running_tasks_per_domain: 16,
    }
}

fn defaults_executor() -> ExecutorConfig {
    ExecutorConfig {
        poll_interval: Duration::from_millis(500),
        max_concurrent_per_domain: 8,
        max_retries: 3,
    }
}

// ---------- ENV helpers (scoped resolution) ----------

fn dotenv() {
    let _ = dotenvy::dotenv();
}

fn pick_env(names: &[&str]) -> Option<String> {
    for k in names {
        if let Ok(v) = std::env::var(k)
            && !v.is_empty()
        {
            return Some(v);
        }
    }
    None
}

/// Resolve `CONTEXIDE_<SCOPE>_<KEY>` → `CONTEXIDE_<KEY>` → plain fallbacks.
fn env_scoped_s(scope: &str, key: &str, plain_fallbacks: &[&str]) -> Option<String> {
    let scoped = format!("CONTEXIDE_{}_{}", scope, key);
    if let Some(v) = pick_env(&[&scoped]) {
        return Some(v);
    }
    let global = format!("CONTEXIDE_{}", key);
    if let Some(v) = pick_env(&[&global]) {
        return Some(v);
    }
    pick_env(plain_fallbacks)
}

fn env_scoped_u32(scope: &str, key: &str, plain_fallbacks: &[&str]) -> Option<u32> {
    env_scoped_s(scope, key, plain_fallbacks).and_then(|s| s.parse().ok())
}

fn env_scoped_usize(scope: &str, key: &str, plain_fallbacks: &[&str]) -> Option<usize> {
    env_scoped_s(scope, key, plain_fallbacks).and_then(|s| s.parse().ok())
}

fn env_scoped_bool(scope: &str, key: &str, plain_fallbacks: &[&str]) -> Option<bool> {
    env_scoped_s(scope, key, plain_fallbacks).and_then(|v| match v.as_str() {
        "1" | "true" | "TRUE" => Some(true),
        "0" | "false" | "FALSE" => Some(false),
        _ => None,
    })
}

// ---------- Per-crate loaders (each validates on its own) ----------

/// Load only STORAGE config (valid on its own).
pub fn load_storage() -> anyhow::Result<StorageConfig> {
    dotenv();
    let scope = "STORAGE";
    let mut cfg = defaults_storage();

    // DB URL: scoped → global → DATABASE_URL/CONTEXIDE_DB_URL
    if let Some(url) = env_scoped_s(scope, "DATABASE_URL", &["DATABASE_URL", "CONTEXIDE_DB_URL"]) {
        cfg.database_url = url;
    }
    if let Some(n) = env_scoped_u32(
        scope,
        "DB_MAX_CONNECTIONS",
        &["CONTEXIDE_DB_MAX_CONNECTIONS"],
    ) {
        cfg.db_max_connections = n;
    }
    if let Some(d) = env_scoped_s(scope, "MIGRATIONS_DIR", &["CONTEXIDE_MIGRATIONS_DIR"]) {
        cfg.migrations_dir_override = Some(d);
    }

    // Validate
    if cfg.database_url.is_empty() {
        anyhow::bail!("STORAGE: DATABASE_URL must not be empty");
    }
    Ok(cfg)
}

/// Load only BLOB_STORAGE config (valid on its own).
pub fn load_blob_storage() -> anyhow::Result<BlobStorageConfig> {
    dotenv();
    let scope = "BLOB_STORAGE";
    let mut cfg = defaults_blob();

    if let Some(v) = env_scoped_s(scope, "S3_ENDPOINT", &["MINIO_ENDPOINT"]) {
        cfg.endpoint = v;
    }
    if let Some(v) = env_scoped_s(scope, "S3_ACCESS_KEY", &["MINIO_ACCESS_KEY"]) {
        cfg.access_key = v;
    }
    if let Some(v) = env_scoped_s(scope, "S3_SECRET_KEY", &["MINIO_SECRET_KEY"]) {
        cfg.secret_key = v;
    }
    if let Some(v) = env_scoped_s(scope, "S3_BUCKET", &["MINIO_BUCKET"]) {
        cfg.bucket = v;
    }
    if let Some(v) = env_scoped_bool(scope, "S3_PATH_STYLE", &["MINIO_PATH_STYLE"]) {
        cfg.path_style = v;
    }
    if let Some(v) = env_scoped_s(scope, "S3_REGION", &["MINIO_REGION"]) {
        cfg.region = Some(v);
    }

    // Validate
    if !(cfg.endpoint.starts_with("http://") || cfg.endpoint.starts_with("https://")) {
        anyhow::bail!(
            "BLOB_STORAGE: S3_ENDPOINT or MINIO_ENDPOINT must start with http:// or https://"
        );
    }
    if cfg.bucket.is_empty() {
        anyhow::bail!("BLOB_STORAGE: S3_ENDPOINT or MINIO_BUCKET must not be empty");
    }
    Ok(cfg)
}

/// Load only VECTOR config (valid on its own).
pub fn load_vector() -> anyhow::Result<VectorConfig> {
    dotenv();
    let scope = "VECTOR";
    let mut cfg = defaults_vector();

    if let Some(v) = env_scoped_s(scope, "VECTOR_ENDPOINT", &["VECTOR_ENDPOINT"]) {
        cfg.endpoint = v;
    }
    if let Some(v) = env_scoped_s(
        scope,
        "VECTOR_COLLECTION_PREFIX",
        &["VECTOR_COLLECTION_PREFIX"],
    ) {
        cfg.collection_prefix = v;
    }
    if let Some(v) = env_scoped_s(scope, "VECTOR_METRIC", &["VECTOR_METRIC"]) {
        cfg.metric = v;
    }
    if let Some(v) = env_scoped_usize(scope, "VECTOR_DIM", &["VECTOR_DIM"]) {
        cfg.dim = v;
    }

    // Validate
    if !(cfg.endpoint.starts_with("http://") || cfg.endpoint.starts_with("https://")) {
        anyhow::bail!("VECTOR: VECTOR_ENDPOINT must start with http:// or https://");
    }
    if cfg.dim != 1024 {
        anyhow::bail!("VECTOR: VECTOR_DIM must be 1024 (got {})", cfg.dim);
    }
    Ok(cfg)
}

/// Load only EMBEDDINGS config (valid on its own).
pub fn load_embeddings() -> anyhow::Result<EmbeddingsConfig> {
    dotenv();
    let scope = "EMBEDDINGS";
    let mut cfg = defaults_embeddings();

    if let Some(v) = env_scoped_s(scope, "EMBEDDINGS_PROVIDER", &["EMBEDDINGS_PROVIDER"]) {
        cfg.provider = v;
    }
    if let Some(v) = env_scoped_s(scope, "EMBEDDINGS_MODEL", &["EMBEDDINGS_MODEL"]) {
        cfg.model = v;
    }
    if let Some(v) = env_scoped_usize(scope, "EMBEDDINGS_BATCH", &["EMBEDDINGS_BATCH"]) {
        cfg.batch = v;
    }

    // Minimal validation
    if cfg.model.is_empty() {
        anyhow::bail!("EMBEDDINGS: EMBEDDINGS_MODEL must not be empty");
    }
    Ok(cfg)
}

/// Load only API config (valid on its own).
pub fn load_api() -> anyhow::Result<ApiConfig> {
    dotenv();
    let scope = "API";
    let mut cfg = defaults_api();

    if let Some(v) = env_scoped_s(scope, "HTTP_BIND", &["CONTEXIDE_HTTP_BIND"]) {
        cfg.bind = v;
    }
    // No strict validation (localhost is fine)
    Ok(cfg)
}

/// Load only WORKERS tuning (valid on its own).
pub fn load_workers() -> anyhow::Result<WorkersTuning> {
    dotenv();
    let scope = "WORKERS";
    let mut cfg = defaults_workers();

    if let Some(v) = env_scoped_usize(scope, "WORKERS_INGEST", &["WORKERS_INGEST"]) {
        cfg.ingest = v;
    }
    if let Some(v) = env_scoped_usize(scope, "WORKERS_EXTRACT", &["WORKERS_EXTRACT"]) {
        cfg.extract = v;
    }
    if let Some(v) = env_scoped_usize(scope, "WORKERS_NORMALIZE", &["WORKERS_NORMALIZE"]) {
        cfg.normalize = v;
    }
    if let Some(v) = env_scoped_usize(scope, "WORKERS_CHUNK", &["WORKERS_CHUNK"]) {
        cfg.chunk = v;
    }
    if let Some(v) = env_scoped_usize(scope, "WORKERS_EMBED", &["WORKERS_EMBED"]) {
        cfg.embed = v;
    }
    if let Some(v) = env_scoped_usize(scope, "WORKERS_INDEX", &["WORKERS_INDEX"]) {
        cfg.index = v;
    }

    Ok(cfg)
}

/// Load WORKFLOW EXECUTOR config (valid on its own).
///
/// ENV keys (scoped → global fallbacks):
/// - CONTEXIDE_EXECUTOR_POLL_INTERVAL_MS
/// - CONTEXIDE_EXECUTOR_MAX_CONCURRENT_PER_DOMAIN
/// - CONTEXIDE_EXECUTOR_MAX_RETRIES
pub fn load_executor() -> anyhow::Result<ExecutorConfig> {
    dotenv();
    let scope = "EXECUTOR";
    let mut cfg = defaults_executor();

    if let Some(ms) = env_scoped_s(scope, "POLL_INTERVAL_MS", &[]) {
        let v: u64 = ms.parse().context("parse POLL_INTERVAL_MS")?;
        cfg.poll_interval = Duration::from_millis(v);
    }
    if let Some(v) = env_scoped_usize(scope, "MAX_CONCURRENT_PER_DOMAIN", &[]) {
        cfg.max_concurrent_per_domain = v;
    }
    if let Some(v) = env_scoped_u32(scope, "MAX_RETRIES", &[]) {
        cfg.max_retries = v;
    }

    Ok(cfg)
}

/// Load messaging config (valid on its own).
pub fn load_messaging() -> anyhow::Result<MessagingConfig> {
    dotenv();
    let scope = "MESSAGING";
    let mut cfg = defaults_messaging();

    if let Some(v) = env_scoped_s(scope, "NATS_URL", &["NATS_URL"]) {
        cfg.nats_url = v;
    }
    if let Some(v) = env_scoped_s(scope, "WORKFLOW_PREFIX", &["WORKFLOW_PREFIX"]) {
        cfg.workflow_prefix = v;
    }
    if let Some(v) = env_scoped_s(scope, "WORKFLOW_STREAM", &["WORKFLOW_STREAM"]) {
        cfg.workflow_stream = v;
    }
    if let Some(v) = env_scoped_usize(scope, "WORKER_DEFAULT_CONCURRENCY", &[]) {
        cfg.worker_default_concurrency = Some(v);
    }

    Ok(cfg)
}

/// Load workflow config (valid on its own).
pub fn load_workflow() -> anyhow::Result<WorkflowConfig> {
    dotenv();
    let scope = "WORKFLOW";
    let mut cfg = defaults_workflow();

    if let Some(v) = env_scoped_u32(
        scope,
        "MAX_RUNNING_TASKS_TOTAL",
        &["MAX_RUNNING_TASKS_TOTAL"],
    ) {
        cfg.max_running_tasks_total = v;
    }
    if let Some(v) = env_scoped_u32(
        scope,
        "DEFAULT_TENANT_MAX_RUNNING_DAG_RUNS",
        &["DEFAULT_TENANT_MAX_RUNNING_DAG_RUNS"],
    ) {
        cfg.default_tenant_max_running_dag_runs = v;
    }
    if let Some(v) = env_scoped_u32(
        scope,
        "DEFAULT_TENANT_MAX_RUNNING_TASKS",
        &["DEFAULT_TENANT_MAX_RUNNING_TASKS"],
    ) {
        cfg.default_tenant_max_running_tasks = v;
    }
    if let Some(v) = env_scoped_u32(
        scope,
        "DEFAULT_TENANT_MAX_RUNNING_TASKS_PER_DOMAIN",
        &["DEFAULT_TENANT_MAX_RUNNING_TASKS_PER_DOMAIN"],
    ) {
        cfg.default_tenant_max_running_tasks_per_domain = v;
    }

    Ok(cfg)
}

// ---------- Global loader (whole app) ----------

/// Load the whole application config by composing all sub-configs.
pub fn load_app() -> anyhow::Result<AppConfig> {
    dotenv();

    // CONTEXIDE_ENV or default
    let env = match env_scoped_s("GLOBAL", "ENV", &["CONTEXIDE_ENV"]) {
        Some(v) if v.eq_ignore_ascii_case("prod") => RunEnv::Prod,
        _ => defaults_env(),
    };

    let storage = load_storage().context("loading STORAGE config")?;
    let blob_storage = load_blob_storage().context("loading BLOB_STORAGE config")?;
    let vector = load_vector().context("loading VECTOR config")?;
    let embeddings = load_embeddings().context("loading EMBEDDINGS config")?;
    let api = load_api().context("loading API config")?;
    let workers = load_workers().context("loading WORKERS config")?;
    let messaging = load_messaging().context("loading MESSAGING config")?;
    let workflow = load_workflow().context("loading WORKFLOW config")?;

    Ok(AppConfig {
        env,
        storage,
        blob_storage,
        vector,
        embeddings,
        api,
        workers,
        messaging,
        workflow,
    })
}

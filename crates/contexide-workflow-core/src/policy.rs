//! Execution policy snapshot: retry and quota configuration.
//!
//! These types are serialized into storage/messaging as JSON.

use serde::{Deserialize, Serialize};

/// Strategy for retrying a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryStrategyKind {
    None,
    Fixed,
    Exponential,
}

/// Retry strategy parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of attempts including the first one.
    pub max_attempts: u16,
    /// Strategy kind.
    pub kind: RetryStrategyKind,
    /// Base delay in milliseconds.
    pub base_delay_ms: u32,
    /// Optional jitter in milliseconds to randomize delays.
    pub jitter_ms: u32,
    /// Optional max delay cap in milliseconds.
    pub max_delay_ms: Option<u32>,
}

/// Identifier for a quota bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaBucketRef {
    /// Tenant-level namespace, e.g. `tenant-{uuid}`.
    pub scope: String,
    /// Logical bucket key, e.g. "embeddings", "ingest".
    pub key: String,
}

/// Quota configuration snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Maximum concurrent tasks in this bucket.
    pub max_concurrent: u32,
    /// Maximum tasks per minute in this bucket.
    pub rate_per_minute: u32,
}

/// Effective retry/quotas for a DagRun or Task (immutable snapshot).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    pub retry: RetryPolicy,
    /// Optional primary quota bucket (if `None`, global defaults apply).
    pub quota_bucket: Option<QuotaBucketRef>,
    /// Optional resolved quota config for this bucket at planning time.
    pub quota_config: Option<QuotaConfig>,
}

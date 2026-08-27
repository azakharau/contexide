//! Provider plugins registry.
//!
//! Each concrete provider implements `EmbeddingsProvider` with its native wire format.
//! We avoid a one-size-fits-all HTTP parser on purpose.
//!
//! Add new providers by creating `providers/<name>.rs` and exposing a constructor here.

use std::sync::Arc;

use contexide_core::embeddings::EmbeddingsProvider;
use contexide_core::errors::{Error, Result};

use crate::ModelInfo;

pub mod openai;
pub mod tei;

/// Thin options bag for constructing providers at runtime.
#[derive(Debug, Clone)]
pub struct ProviderOpts {
    /// Base endpoint, e.g. "https://api.openai.com/v1" or "http://127.0.0.1:8080".
    pub endpoint: String,
    /// Model metadata (id/dims/batch).
    pub info: ModelInfo,
    /// API key if the provider requires it (Bearer).
    pub api_key: Option<String>,
    /// Optional route override (provider-specific default exists).
    pub route: Option<String>,
    /// Optional organization/project/tenant header (OpenAI-org, etc.).
    pub org: Option<String>,
}

impl ProviderOpts {
    pub fn new(endpoint: impl Into<String>, info: ModelInfo) -> Self {
        Self {
            endpoint: endpoint.into(),
            info,
            api_key: None,
            route: None,
            org: None,
        }
    }
}

/// Factory that returns a provider by `kind`.
///
/// Supported kinds:
/// - "openai"  -> OpenAI-compatible embeddings (`/embeddings`)
/// - "tei"     -> HuggingFace Text-Embeddings-Inference (`/embeddings`)
pub fn build(kind: &str, opts: ProviderOpts) -> Result<Arc<dyn EmbeddingsProvider>> {
    match kind {
        "openai" => {
            let p = openai::OpenAiEmbeddings::new(opts)?;
            Ok(Arc::new(p))
        }
        "tei" => {
            let p = tei::TeiEmbeddings::new(opts)?;
            Ok(Arc::new(p))
        }
        other => Err(Error::Other(anyhow::anyhow!(
            "unknown embeddings provider kind: {other}"
        ))),
    }
}

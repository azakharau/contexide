// crates/contexide-embeddings/src/providers/tei.rs
//! Text Embeddings Inference (TEI) provider plugin.
//!
//! - Request is a borrowed slice of &str -> we only need `Serialize`.
//! - Response supports both shapes: `{"embeddings":[...]}`
/*   and plain `[[...]]` via `#[serde(untagged)]`. */
//! - No lifetimes in the provider itself; only the request payload borrows.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use contexide_core::errors::{Error, Result};

use crate::{Embedding, ModelInfo};
use contexide_core::embeddings::{EmbeddingsProvider, validate_batch_shape};

/// TEI request payload: we only serialize it; `Deserialize` is not needed.
#[derive(Debug, Serialize)]
struct TeiRequest<'a> {
    /// Batch of inputs. Borrowed slice serializes fine; no ownership required.
    inputs: &'a [&'a str],
    /// Optional controls; TEI accepts these (ignored by some backends).
    #[serde(skip_serializing_if = "Option::is_none")]
    normalize: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    truncate: Option<&'a str>,
}

/// TEI may respond with either a wrapped or plain embeddings array.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TeiResponse {
    Wrapped { embeddings: Vec<Vec<f32>> },
    Plain(Vec<Vec<f32>>),
}

impl TeiResponse {
    fn into_embeddings(self) -> Vec<Vec<f32>> {
        match self {
            TeiResponse::Wrapped { embeddings } => embeddings,
            TeiResponse::Plain(v) => v,
        }
    }
}

/// Concrete TEI provider.
pub struct TeiEmbeddings {
    client: Client,
    endpoint: String, // e.g. "http://127.0.0.1:8080/embed"
    info: ModelInfo,
    default_normalize: Option<bool>,
    default_truncate: Option<String>,
}

impl TeiEmbeddings {
    /// Construct from options used by our providers registry/factory.
    pub fn new(opts: crate::providers::ProviderOpts) -> Result<Self> {
        let client = Client::builder()
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| Error::Other(anyhow!(e)))?;

        Ok(Self {
            client,
            endpoint: opts.endpoint,
            info: opts.info,
            default_normalize: None,
            default_truncate: None,
        })
    }

    /// Low-level call that posts batch to TEI and parses embeddings.
    async fn post_embed<'a>(&self, inputs: &'a [&'a str]) -> Result<Vec<Vec<f32>>> {
        let req = TeiRequest {
            inputs,
            normalize: self.default_normalize,
            truncate: self.default_truncate.as_deref(),
        };

        let res = self
            .client
            .post(&self.endpoint)
            .json(&req)
            .send()
            .await
            .map_err(|e| Error::Other(anyhow!("tei request failed: {e}")))?
            .error_for_status()
            .map_err(|e| Error::Other(anyhow!("tei non-success status: {e}")))?;

        let parsed: TeiResponse = res
            .json()
            .await
            .map_err(|e| Error::Other(anyhow!("tei parse failed: {e}")))?;

        Ok(parsed.into_embeddings())
    }
}

#[async_trait::async_trait]
impl EmbeddingsProvider for TeiEmbeddings {
    fn info(&self) -> ModelInfo {
        self.info
    }

    async fn embed(&self, inputs: &[&str]) -> Result<Vec<Embedding>> {
        // TEI typically supports reasonable batch sizes; callers should micro-batch if needed.
        let resp = self
            .post_embed(inputs)
            .await
            .with_context(|| format!("tei embed failed for batch size {}", inputs.len()))
            .map_err(Error::Other)?;

        let mut out = Vec::with_capacity(resp.len());
        for v in resp {
            out.push(Embedding {
                vector: v,
                token_count: None,
            });
        }
        validate_batch_shape(&self.info, inputs.len(), &out)?;

        Ok(out)
    }
}

/// Factory glue so the enum-based registry can build us.
pub fn build(opts: crate::providers::ProviderOpts) -> Result<Arc<dyn EmbeddingsProvider>> {
    Ok(Arc::new(TeiEmbeddings::new(opts)?))
}

#[cfg(test)]
mod tests {

    #[test]
    fn request_is_serialize_only() {
        // Compile-time check that we didn't accidentally add Deserialize
        fn assert_serialize<T: serde::Serialize>() {}
        assert_serialize::<super::TeiRequest<'_>>();
    }
}

//! OpenAI-compatible embeddings provider.
//!
//! Wire format:
//! POST {endpoint}/embeddings
//!   { "model": "...", "input": ["...", ...] }
//! Response:
//!   { "data": [ { "embedding": [f32,...], "index": 0 }, ... ], ... }
//!
//! Notes:
//! - Uses `Authorization: Bearer <api_key>` if provided.
//! - Optional `OpenAI-Organization` header via `ProviderOpts.org`.

use std::time::Duration;

use anyhow::anyhow;
use contexide_core::errors::{Error, Result};
use reqwest::{Client, header};
use serde::Deserialize;
use serde_json::json;

use crate::{Embedding, ModelInfo};
use contexide_core::embeddings::{EmbeddingsProvider, validate_batch_shape};

use super::ProviderOpts;

#[derive(Debug)]
pub struct OpenAiEmbeddings {
    client: Client,
    url: String,
    info: ModelInfo,
    api_key: Option<String>,
    org: Option<String>,
}

impl OpenAiEmbeddings {
    pub fn new(opts: ProviderOpts) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| Error::Other(anyhow!(e)))?;
        let base = opts.endpoint.trim_end_matches('/');
        let route = opts.route.unwrap_or_else(|| "/embeddings".to_string());
        let url = format!("{base}{}", route);

        Ok(Self {
            client,
            url,
            info: opts.info,
            api_key: opts.api_key,
            org: opts.org,
        })
    }

    fn headers(&self) -> Result<header::HeaderMap> {
        let mut h = header::HeaderMap::new();
        h.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        if let Some(k) = &self.api_key {
            let v = format!("Bearer {k}");
            h.insert(
                header::AUTHORIZATION,
                header::HeaderValue::from_str(&v).map_err(|e| {
                    Error::Other(anyhow!("invalid OpenAI API key header value: {}", e))
                })?,
            );
        }
        if let Some(org) = &self.org {
            // This header is optional; many providers ignore it safely.
            h.insert(
                "OpenAI-Organization",
                header::HeaderValue::from_str(org).map_err(|e| {
                    Error::Other(anyhow!("invalid OpenAI-Organization header value: {}", e))
                })?,
            );
        }
        Ok(h)
    }
}

#[derive(Debug, Deserialize)]
struct OaiVec {
    embedding: Vec<f32>,
    #[allow(dead_code)]
    index: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct OaiResp {
    data: Vec<OaiVec>,
    #[allow(dead_code)]
    usage: Option<serde_json::Value>,
}

#[async_trait::async_trait]
impl EmbeddingsProvider for OpenAiEmbeddings {
    fn info(&self) -> ModelInfo {
        self.info
    }

    async fn embed(&self, inputs: &[&str]) -> Result<Vec<Embedding>> {
        let body = json!({
            "model": self.info.id,
            "input": inputs
        });

        let resp = self
            .client
            .post(&self.url)
            .headers(self.headers()?)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Other(anyhow!(e)))?;

        if !resp.status().is_success() {
            return Err(Error::Other(anyhow!(
                "openai embeddings HTTP {}",
                resp.status()
            )));
        }

        let parsed: OaiResp = resp.json().await.map_err(|e| Error::Other(anyhow!(e)))?;
        let mut out = Vec::with_capacity(parsed.data.len());
        for v in parsed.data {
            out.push(Embedding {
                vector: v.embedding,
                token_count: None,
            });
        }
        validate_batch_shape(&self.info, inputs.len(), &out)?;
        Ok(out)
    }
}

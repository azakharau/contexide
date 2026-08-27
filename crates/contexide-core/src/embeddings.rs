//! Embeddings provider contracts and DTOs (domain only).
//!
//! Concrete providers (TEI/OpenAI/ONNX/etc.) live in adapter crates.

use crate::errors::{Error, Result};

/// Static model metadata used by providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelInfo {
    pub id: &'static str,
    pub dims: usize,
    pub max_batch: usize,
}

/// Single embedding result.
#[derive(Debug, Clone)]
pub struct Embedding {
    pub vector: Vec<f32>,
    pub token_count: Option<usize>,
}

impl Embedding {
    pub fn validate_shape(&self, info: &ModelInfo) -> Result<()> {
        if self.vector.len() != info.dims {
            return Err(Error::Other(anyhow::anyhow!(
                "embedding dims mismatch: got {}, expected {}",
                self.vector.len(),
                info.dims
            )));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait EmbeddingsProvider: Send + Sync {
    fn info(&self) -> ModelInfo;
    async fn embed(&self, inputs: &[&str]) -> Result<Vec<Embedding>>;
}

pub fn batch_inputs<'a>(inputs: &'a [&'a str], max_batch: usize) -> Vec<&'a [&'a str]> {
    assert!(max_batch > 0, "max_batch must be > 0");
    inputs.chunks(max_batch).collect()
}

pub fn validate_batch_shape(
    info: &ModelInfo,
    inputs_len: usize,
    results: &[Embedding],
) -> Result<()> {
    if results.len() != inputs_len {
        return Err(Error::Other(anyhow::anyhow!(
            "result length mismatch: got {}, expected {}",
            results.len(),
            inputs_len
        )));
    }
    for (idx, e) in results.iter().enumerate() {
        e.validate_shape(info).map_err(|err| {
            Error::Other(anyhow::anyhow!(
                "invalid embedding at index {}: {}",
                idx,
                err
            ))
        })?;
    }
    Ok(())
}

//! Chunking and tokenization contracts (domain only).
//!
//! Implementations (sliding window, HF tokenizers, etc.) live in adapter crates.

use crate::prelude::BlockId;
use serde::{Deserialize, Serialize};

/// Chunking specification (token-based sliding window).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkSpec {
    pub window_tokens: usize,
    pub overlap_tokens: usize,
    pub min_chunk_tokens: usize,
}

impl ChunkSpec {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.window_tokens == 0 {
            return Err("window_tokens must be > 0");
        }
        if self.overlap_tokens >= self.window_tokens {
            return Err("overlap_tokens must be < window_tokens");
        }
        if self.min_chunk_tokens == 0 || self.min_chunk_tokens > self.window_tokens {
            return Err("min_chunk_tokens must be in 1..=window_tokens");
        }
        Ok(())
    }
}

/// Input unit for chunking (typically one normalized block).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInput {
    pub block_id: Option<BlockId>,
    pub text: String,
}

impl ChunkInput {
    pub fn new(block_id: Option<BlockId>, text: impl Into<String>) -> Self {
        Self {
            block_id,
            text: text.into(),
        }
    }
}

/// Output unit of chunking with token offsets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkPiece {
    pub block_id: Option<BlockId>,
    pub text: String,
    pub start_token: usize,
    pub end_token: usize,
    pub token_count: usize,
}

/// Minimal tokenizer interface.
pub trait Tokenizer: Send + Sync {
    fn id(&self) -> &'static str;
    /// Encode text to token ids (implementation-defined) and optional char offsets.
    fn encode(&self, s: &str) -> Vec<usize>;
    /// Decode token ids back to text (best-effort).
    fn decode(&self, tokens: &[usize]) -> String;
}

/// Chunker interface.
pub trait Chunker: Send + Sync {
    fn chunk(
        &self,
        tokenizer: &dyn Tokenizer,
        inputs: &[ChunkInput],
        spec: &ChunkSpec,
    ) -> Vec<ChunkPiece>;
}

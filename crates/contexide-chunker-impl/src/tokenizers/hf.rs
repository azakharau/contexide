//! Hugging Face `tokenizers` adapter implementing our `Tokenizer` trait.
//!
//! Goals:
//! - Zero-cost bridge over `tokenizers::Tokenizer`.
//! - Safe usize<->u32 conversion for ids.
//! - Constructors for file-based and in-memory usage.
//!
//! Notes:
//! - Use `from_file(".../tokenizer.json")` for real models (e.g., BAAI/bge-m3).
//! - In tests we construct a tiny `WordLevel` tokenizer programmatically.
//!
//! Add dependency in the workspace (root Cargo.toml):
//! [workspace.dependencies]
//! tokenizers = { version = "0.20", default-features = false, features = ["onig"] }
//! anyhow = "1"

use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokenizers::{Encoding, Tokenizer};

use crate::Tokenizer as TokenizerTrait;

/// HF-backed tokenizer implementing our `Tokenizer` trait.
pub struct HfTokenizer {
    inner: Arc<Tokenizer>,
    /// Stable identifier, e.g. "hf:bge-m3".
    id: &'static str,
    /// Whether to skip special tokens on decode.
    skip_special: bool,
    /// Whether to add special tokens on encode (for chunking usually `false`).
    include_special_on_encode: bool,
}

type Encoded = (Vec<usize>, Vec<(usize, usize)>);

impl HfTokenizer {
    /// Build from an already constructed `tokenizers::Tokenizer`.
    pub fn from_inner(
        inner: Tokenizer,
        id: &'static str,
        skip_special: bool,
        include_special_on_encode: bool,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            id,
            skip_special,
            include_special_on_encode,
        }
    }

    /// Load from a `tokenizer.json` file on disk.
    pub fn from_file(path: &str, id: &'static str) -> Result<Self> {
        let inner = Tokenizer::from_file(path)
            .map_err(|e| anyhow!("failed to load tokenizer from {}: {}", path, e))?;
        // Sensible defaults for chunking:
        // - do NOT add special tokens on encode
        // - skip special tokens on decode
        Ok(Self::from_inner(
            inner, id, /*skip_special=*/ true, /*include_special_on_encode=*/ false,
        ))
    }

    /// Encode helper returning `Encoding` (useful for offsets/word ids).
    #[inline]
    pub fn encode_full(&self, text: &str) -> Result<Encoding> {
        self.inner
            .encode(text, self.include_special_on_encode)
            .map_err(|e| anyhow!("encode failed: {e}"))
    }

    /// Convenience helper if caller needs both ids and byte offsets at once.
    #[inline]
    pub fn encode_ids_and_offsets(&self, text: &str) -> Result<Encoded> {
        let enc = self.encode_full(text)?;
        let ids: Vec<usize> = enc.get_ids().iter().map(|&u| u as usize).collect();
        let offsets: Vec<(usize, usize)> = enc.get_offsets().to_vec();
        Ok((ids, offsets))
    }

    #[inline]
    fn to_u32_slice(tokens: &[usize]) -> Result<Vec<u32>> {
        tokens
            .iter()
            .copied()
            .map(|t| u32::try_from(t).map_err(|_| anyhow!("token id {} doesn't fit u32", t)))
            .collect()
    }
}

impl TokenizerTrait for HfTokenizer {
    fn id(&self) -> &'static str {
        self.id
    }

    fn encode(&self, text: &str) -> Vec<usize> {
        // For chunking we prefer not to include special tokens by default.
        match self.encode_full(text) {
            Ok(enc) => enc.get_ids().iter().map(|&u| u as usize).collect(),
            Err(_) => Vec::new(),
        }
    }

    fn decode(&self, tokens: &[usize]) -> String {
        Self::to_u32_slice(tokens)
            .and_then(|ids| {
                self.inner
                    .decode(&ids, self.skip_special)
                    .map_err(|e| anyhow!(e))
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashMap;
    use tokenizers::models::wordlevel::WordLevel;
    use tokenizers::pre_tokenizers::whitespace::Whitespace;

    /// Build a tiny word-level tokenizer for tests:
    /// vocab: {"hello":1, "world":2, "[UNK]":0}
    fn mk_test_inner() -> Tokenizer {
        let mut vocab = AHashMap::new();
        vocab.insert("[UNK]".to_string(), 0_u32);
        vocab.insert("hello".to_string(), 1_u32);
        vocab.insert("world".to_string(), 2_u32);

        let model = WordLevel::builder()
            .vocab(vocab)
            .unk_token("[UNK]".to_string())
            .build()
            .unwrap();
        let mut tok = Tokenizer::new(model);
        tok.with_pre_tokenizer(Some(Whitespace));
        tok
    }

    #[test]
    fn encode_decode_roundtrip() {
        let inner = mk_test_inner();
        let hf = HfTokenizer::from_inner(
            inner, "hf:test", /*skip_special=*/ true,
            /*include_special_on_encode=*/ false,
        );

        let ids = hf.encode("hello world");
        assert_eq!(ids, vec![1, 2]);

        let text = hf.decode(&ids);
        assert_eq!(text, "hello world");
    }

    #[test]
    fn ids_and_offsets() {
        let inner = mk_test_inner();
        let hf = HfTokenizer::from_inner(inner, "hf:test", true, false);

        let (ids, offs) = hf.encode_ids_and_offsets("hello world").unwrap();
        assert_eq!(ids, vec![1, 2]);
        // With whitespace pretokenizer we'll have two spans in order.
        assert_eq!(offs.len(), 2);
        assert!(offs[0].0 <= offs[0].1 && offs[1].0 <= offs[1].1);
    }

    #[test]
    fn id_is_stable() {
        let inner = mk_test_inner();
        let hf = HfTokenizer::from_inner(inner, "hf:test", true, false);
        assert_eq!(hf.id(), "hf:test");
    }
}

// crates/contexide-chunker/src/sliding.rs
//! Sliding window chunker over a generic `Tokenizer`.
//!
//! Behavior:
//! - Splits token stream into fixed-size windows with overlap.
//! - Drops too-short chunks (< `min_chunk_tokens`), except when the entire
//!   block fits into a single chunk, in which case it is kept.
//! - Decodes each chunk's token span back to text via `Tokenizer::decode`.
//!
//! Notes:
//! - `ChunkSpec::validate()` is enforced via `expect` to surface misconfig
//!   early (mirrors earlier dummy chunker tests).
//! - Stateless; reuse a single instance anywhere.

use crate::{ChunkInput, ChunkPiece, ChunkSpec, Chunker, Tokenizer};

/// Sliding-window chunker (stateless).
#[derive(Debug, Default, Clone, Copy)]
pub struct SlidingWindowChunker;

impl Chunker for SlidingWindowChunker {
    fn chunk(
        &self,
        tokenizer: &dyn Tokenizer,
        inputs: &[ChunkInput],
        spec: &ChunkSpec,
    ) -> Vec<ChunkPiece> {
        // Fail fast on invalid configuration to avoid silent bad splits.
        spec.validate().expect("invalid ChunkSpec");

        let mut all = Vec::new();
        let win = spec.window_tokens;
        let ovl = spec.overlap_tokens;
        let step = win - ovl;
        let min_len = spec.min_chunk_tokens;

        for inp in inputs {
            // Encode once per block; chunk on token indices only.
            let ids = tokenizer.encode(&inp.text);
            let n = ids.len();
            if n == 0 {
                continue;
            }

            let mut start = 0usize;
            let mut emitted_for_block = 0usize;

            while start < n {
                let end = (start + win).min(n);
                let len = end - start;

                let keep = if emitted_for_block == 0 && end == n {
                    // Single-chunk whole block is allowed even if len < min_len.
                    true
                } else {
                    len >= min_len
                };

                if keep {
                    let text = tokenizer.decode(&ids[start..end]);
                    all.push(ChunkPiece {
                        block_id: inp.block_id,
                        text,
                        start_token: start,
                        end_token: end,
                        token_count: len,
                    });
                    emitted_for_block += 1;
                }

                if end == n {
                    break;
                }
                start += step;
            }
        }

        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChunkInput, Tokenizer};

    struct WsTok;
    impl Tokenizer for WsTok {
        fn id(&self) -> &'static str {
            "ws"
        }
        fn encode(&self, text: &str) -> Vec<usize> {
            text.split_whitespace()
                .enumerate()
                .map(|(i, _)| i)
                .collect()
        }
        fn decode(&self, tokens: &[usize]) -> String {
            if tokens.is_empty() {
                return String::new();
            }
            let mut s = String::new();
            for (i, _) in tokens.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                s.push_str("tok");
            }
            s
        }
    }

    #[test]
    fn slides_with_overlap() {
        let tok = WsTok;
        let ch = SlidingWindowChunker;
        let spec = ChunkSpec {
            window_tokens: 5,
            overlap_tokens: 2,
            min_chunk_tokens: 1,
        };
        let input = ChunkInput::new(None, "a b c d e f g h i j");
        let out = ch.chunk(&tok, &[input], &spec);

        // Windows: [0..5), [3..8), [6..10)
        assert_eq!(out.len(), 3);
        assert_eq!((out[0].start_token, out[0].end_token), (0, 5));
        assert_eq!((out[1].start_token, out[1].end_token), (3, 8));
        assert_eq!((out[2].start_token, out[2].end_token), (6, 10));

        // Text is synthetic "tok ..." by count
        assert_eq!(out[0].text.split_whitespace().count(), 5);
        assert_eq!(out[2].text.split_whitespace().count(), 4);
    }

    #[test]
    fn drops_tiny_chunks_except_singleton_block() {
        let tok = WsTok;
        let ch = SlidingWindowChunker;

        // Case 1: one short block (< min), should be kept (singleton block).
        let spec1 = ChunkSpec {
            window_tokens: 8,
            overlap_tokens: 2,
            min_chunk_tokens: 5,
        };
        let input1 = ChunkInput::new(None, "a b c d"); // 4 tokens only
        let out1 = ch.chunk(&tok, &[input1], &spec1);
        assert_eq!(out1.len(), 1);
        assert_eq!(out1[0].token_count, 4);

        // Case 2: multi-window where a short trailing chunk is produced — it should be dropped.
        let spec2 = ChunkSpec {
            window_tokens: 5,
            overlap_tokens: 2,
            min_chunk_tokens: 3,
        };
        // 9 tokens -> windows at [0..5), [3..8), trailing [6..9) has len=3 -> kept, if >= min
        // If we bump min to 4, trailing should be dropped.
        let spec2_drop = ChunkSpec {
            window_tokens: 5,
            overlap_tokens: 2,
            min_chunk_tokens: 4,
        };
        let input2 = ChunkInput::new(None, "a b c d e f g h i");
        let out2 = ch.chunk(&tok, std::slice::from_ref(&input2), &spec2);
        assert_eq!(out2.len(), 3); // trailing len=3 kept (>=3)

        let out2_drop = ch.chunk(&tok, &[input2], &spec2_drop);
        assert_eq!(out2_drop.len(), 2); // trailing len=3 dropped (<4)
    }

    #[test]
    #[should_panic(expected = "invalid ChunkSpec")]
    fn panics_on_invalid_spec() {
        let tok = WsTok;
        let ch = SlidingWindowChunker;
        let spec = ChunkSpec {
            window_tokens: 10,
            overlap_tokens: 10, // invalid
            min_chunk_tokens: 1,
        };
        let _ = ch.chunk(&tok, &[], &spec);
    }
}

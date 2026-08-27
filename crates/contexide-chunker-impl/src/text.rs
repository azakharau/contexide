// crates/contexide-chunker/src/text.rs
//! Token-based sliding-window chunker for normalized text.
//!
//! Key points:
//! - Uses a generic `Tokenizer` (static dispatch) to stay fast and extensible.
//! - Window/overlap are in *tokens* (not bytes/chars).
//! - Minimal whitespace handling: optional per-line rtrim + outer trim.
//! - Pure CPU, synchronous; I/O lives elsewhere.
//!
//! You typically feed this with already-normalized text blocks, one per `ChunkInput`.
//! Persisting to storage is out of scope for this crate.

use crate::{ChunkInput, ChunkPiece, ChunkSpec, Chunker, Tokenizer};

/// Sliding-window chunker with small formatting knobs.
#[derive(Debug, Clone, Copy)]
pub struct SlidingWindowChunker {
    /// If true, apply per-line trailing space trim plus outer trim on each decoded chunk.
    pub trim_whitespace: bool,
}

impl Default for SlidingWindowChunker {
    fn default() -> Self {
        Self {
            trim_whitespace: true,
        }
    }
}

impl SlidingWindowChunker {
    #[inline]
    fn maybe_trim(&self, s: String) -> String {
        if !self.trim_whitespace {
            return s;
        }
        trim_multiline(&s)
    }
}

impl Chunker for SlidingWindowChunker {
    fn chunk(
        &self,
        tokenizer: &dyn Tokenizer,
        inputs: &[ChunkInput],
        spec: &ChunkSpec,
    ) -> Vec<ChunkPiece> {
        // Validate once to fail-fast on misconfig.
        spec.validate().expect("invalid ChunkSpec");

        let mut out = Vec::new();
        let step = spec.window_tokens - spec.overlap_tokens; // safe: validated overlap < window

        for inp in inputs {
            let ids = tokenizer.encode(&inp.text);
            let n = ids.len();
            if n == 0 {
                continue;
            }

            let mut start = 0usize;

            while start < n {
                let end = (start + spec.window_tokens).min(n);
                let token_len = end - start;

                // Accept if large enough or it's the entire text (single window).
                if token_len >= spec.min_chunk_tokens || (start == 0 && end == n) {
                    let text = self.maybe_trim(tokenizer.decode(&ids[start..end]));

                    out.push(ChunkPiece {
                        block_id: inp.block_id,
                        text,
                        start_token: start,
                        end_token: end,
                        token_count: token_len,
                    });
                }

                if end == n {
                    break;
                }
                start += step; // step >= 1 by validation
            }
        }

        out
    }
}

/// Trim strategy used by `SlidingWindowChunker::maybe_trim`.
/// - rtrim each line (drop trailing spaces/tabs)
/// - keep line breaks intact
/// - outer full `trim()` to drop leading/trailing blank lines or spaces
fn trim_multiline(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for (i, line) in input.lines().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        // rtrim on each line
        let trimmed = line.trim_end_matches([' ', '\t']);
        out.push_str(trimmed);
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Tokenizer;

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
    fn sliding_three_windows() {
        let tok = WsTok;
        let spec = ChunkSpec {
            window_tokens: 5,
            overlap_tokens: 2,
            min_chunk_tokens: 1,
        };
        let ch = SlidingWindowChunker::default();

        let input = ChunkInput::new(None, "a b c d e f g h i j");
        let chunks = ch.chunk(&tok, &[input], &spec);

        assert_eq!(chunks.len(), 3);
        assert_eq!((chunks[0].start_token, chunks[0].end_token), (0, 5));
        assert_eq!((chunks[1].start_token, chunks[1].end_token), (3, 8));
        assert_eq!((chunks[2].start_token, chunks[2].end_token), (6, 10));
    }

    #[test]
    fn accepts_small_tail_as_single_window() {
        let tok = WsTok;
        // window larger than text; should still produce one chunk
        let spec = ChunkSpec {
            window_tokens: 100,
            overlap_tokens: 10,
            min_chunk_tokens: 32,
        };
        let ch = SlidingWindowChunker::default();

        let input = ChunkInput::new(None, "one two three");
        let chunks = ch.chunk(&tok, &[input], &spec);
        assert_eq!(chunks.len(), 1);
        assert_eq!((chunks[0].start_token, chunks[0].end_token), (0, 3));
    }

    #[test]
    fn trim_multiline_keeps_newlines_but_drops_trailing_spaces() {
        let s = "a  \n b\t \n\n";
        let t = super::trim_multiline(s);
        assert_eq!(t, "a\n b");
    }
}

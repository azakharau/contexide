//! Text normalization utilities (MVP).
//!
//! Goals (keep it boring and predictable):
//! - Unicode NFKC normalization (canonical + compatibility).
//! - Unify newlines to `\n`.
//! - Remove zero-width & BOM chars.
//! - Replace NBSP with regular space.
//! - Collapse runs of spaces/tabs to single space.
//! - Collapse 3+ consecutive newlines to a single blank line (i.e. `\n\n`).
//! - Trim trailing spaces at line ends and global leading/trailing whitespace.
//!
//! This keeps semantics intact while making chunking/tokenization stable.

use crate::NormalizedText;
use unicode_normalization::UnicodeNormalization;

/// Normalize a text string into a canonical, chunk-friendly form.
pub fn normalize(input: &str) -> NormalizedText {
    let bytes_in = input.len();

    // 1) Unicode NFKC
    let nfkc = UnicodeNormalization::nfkc(input).collect::<String>();

    // 2) Map control-like/format chars and unify line endings.
    //    - CRLF/CR -> LF
    //    - NBSP -> space
    //    - Drop zero-width & BOM-like characters
    let mut mapped = String::with_capacity(nfkc.len());
    for ch in nfkc.chars() {
        match ch {
            // CR -> LF; CRLF will become two LFs here, but we collapse later.
            '\r' => mapped.push('\n'),
            // Non-breaking space -> regular space
            '\u{00A0}' => mapped.push(' '),
            // Zero-width family & BOM/WORD JOINER etc. => drop
            '\u{200B}' | '\u{200C}' | '\u{200D}' | '\u{FEFF}' | '\u{2060}' => {}
            // Keep other chars as-is
            _ => mapped.push(ch),
        }
    }

    // 3) Collapse whitespace:
    //    - Runs of [space or tab] -> single space
    //    - 3+ newlines -> exactly two newlines (single blank line)
    //    We also trim trailing spaces at the end of each line.
    let collapsed_ws = collapse_whitespace(&mapped);

    // 4) Global trim
    let trimmed = collapsed_ws.trim().to_string();

    // Stats
    let bytes_out = trimmed.len();
    let lines = if trimmed.is_empty() {
        0
    } else {
        // Count '\n' + 1 line
        bytecount::count(trimmed.as_bytes(), b'\n') + 1
    };
    let words = trimmed.split_whitespace().count();

    NormalizedText {
        changed: bytes_in != bytes_out || input != trimmed,
        text: trimmed,
        bytes_in,
        bytes_out,
        lines,
        words,
    }
}

/// Collapse runs of horizontal whitespace and normalize blank lines.
///
/// Rules:
/// - `[ ' ' | '\t' ]+`       => single `' '` (but never at line start)
/// - `\n{3,}`                => `\n\n`
/// - trim trailing spaces at end-of-line
fn collapse_whitespace(s: &str) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(s.len());

    let mut prev_was_space = false;
    let mut newline_run = 0;

    // Buffer for current line; we trim trailing spaces before flushing.
    let mut line_buf = String::new();

    let flush_line = |out: &mut String, line: &mut String, newline_count: usize| {
        // Trim trailing horizontal spaces at EOL
        while line.ends_with(' ') {
            line.pop();
        }
        // Write the line
        if !line.is_empty() {
            write!(out, "{}", line).ok();
        }
        line.clear();
        // Write requested newlines
        for _ in 0..newline_count {
            out.push('\n');
        }
    };

    for ch in s.chars() {
        match ch {
            '\n' => {
                // End current line; newlines themselves are buffered via `newline_run`.
                flush_line(&mut out, &mut line_buf, 0);
                newline_run += 1;
                prev_was_space = false;
            }
            ' ' | '\t' => {
                // Only emit a single space if we're *inside* a line (not at its start),
                // and previous char wasn't already a space.
                if !prev_was_space && !line_buf.is_empty() {
                    line_buf.push(' ');
                    prev_was_space = true;
                }
                // else: skip leading or repeated spaces
            }
            _ => {
                // Emit pending newlines (capped to at most two: a single blank line)
                if newline_run > 0 {
                    let n = if newline_run >= 3 { 2 } else { newline_run };
                    for _ in 0..n {
                        out.push('\n');
                    }
                    newline_run = 0;
                }
                prev_was_space = false;
                line_buf.push(ch);
            }
        }
    }

    // Flush tail
    if newline_run > 0 {
        let n = if newline_run >= 3 { 2 } else { newline_run };
        flush_line(&mut out, &mut line_buf, n);
    } else {
        flush_line(&mut out, &mut line_buf, 0);
    }

    out
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nfkc_and_nbsp() {
        // "A\u{00A0}B" (NBSP) -> "A B"
        let out = normalize("Ａ\u{00A0}B"); // 'Ａ' (fullwidth A) -> 'A' under NFKC
        assert_eq!(out.text, "A B");
        assert!(out.changed);
    }

    #[test]
    fn crlf_and_runs() {
        let input = "a\r\n\r\n\r\nb\t\tc";
        let out = normalize(input);
        assert_eq!(out.text, "a\n\nb c");
    }

    #[test]
    fn zero_width_removed() {
        let input = "a\u{200B}\u{200C}\u{200D}\u{FEFF}\u{2060}b";
        let out = normalize(input);
        assert_eq!(out.text, "ab");
    }

    #[test]
    fn trim_trailing_spaces_per_line() {
        let input = "a   \n  b\t\t \n";
        let out = normalize(input);
        assert_eq!(out.text, "a\nb");
    }

    #[test]
    fn stats_are_reasonable() {
        let input = "a b\n\nc";
        let out = normalize(input);
        assert_eq!(out.lines, 3);
        assert_eq!(out.words, 3);
        assert!(!out.text.is_empty());
    }
}

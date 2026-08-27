//! HTML normalization (MVP).
//!
//! Pipeline:
//! 1) Convert HTML → plain text using `html2text` (battle-tested).
//! 2) Feed the result into `text::normalize` to apply our canonical rules
//!    (NFKC, newline unification, whitespace collapsing, trimming).
//!
//! Rationale:
//! - Keeps the module small and reliable without hand-rolling a DOM walker.
//! - Good enough for chunking/tokenization; we can swap implementation later
//!   (e.g., a DOM-based extractor for finer control) without changing the API.

use crate::NormalizedText;
use crate::text::normalize as normalize_text;

/// Options for HTML → text conversion (kept minimal in MVP).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HtmlOptions {
    /// Soft-wrap width for `html2text`. Bigger width = fewer artificial breaks.
    /// In MVP we default to 120 to avoid aggressive wrapping while still preventing
    /// extremely long unbroken lines.
    pub wrap_width: usize,
}

impl Default for HtmlOptions {
    fn default() -> Self {
        Self { wrap_width: 120 }
    }
}

/// Convert raw HTML into normalized plain text.
/// - Uses `html2text` for HTML → text
/// - Then applies `text::normalize`
///
/// # Note
/// `html2text::from_read` always wraps at a given width. We keep width modest (120)
/// and rely on our whitespace normalization to tidy things up.
pub fn normalize_html(input_html: &str) -> NormalizedText {
    normalize_html_with(input_html, HtmlOptions::default())
}

/// Same as `normalize_html`, but with custom options.
pub fn normalize_html_with(input_html: &str, opts: HtmlOptions) -> NormalizedText {
    // 1) HTML → text (soft-wrapped)
    let plain = html2text::from_read(input_html.as_bytes(), opts.wrap_width).unwrap_or_default();

    // 2) Canonical text normalization
    normalize_text(&plain)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headings_and_paragraphs() {
        let html = r#"
            <h1> Title&nbsp;Here </h1>
            <p>First <b>paragraph</b>.</p>
            <p>Second paragraph.</p>
        "#;

        let out = normalize_html(html);
        let s = out.text.as_str();

        // Should contain title and paragraphs, normalized spaces/newlines.
        assert!(s.contains("Title Here"));
        assert!(s.contains("First paragraph."));
        assert!(s.contains("Second paragraph."));
        // No HTML tags remain.
        assert!(!s.contains('<') && !s.contains('>'));
    }

    #[test]
    fn lists_collapse_reasonably() {
        let html = r#"
            <ul>
              <li>item&nbsp;one</li>
              <li>item two</li>
            </ul>
        "#;
        let out = normalize_html(html);
        let s = out.text.as_str();

        // `html2text` will render bullets; our text normalize will collapse spaces.
        assert!(s.contains("item one"));
        assert!(s.contains("item two"));
    }

    #[test]
    fn scripts_are_ignored() {
        let html = r#"
            <p>Safe text</p>
            <script>var x = 1;</script>
        "#;

        let out = normalize_html(html);
        let s = out.text.as_str();

        assert!(s.contains("Safe text"));
        assert!(!s.contains("var x = 1"));
    }

    #[test]
    fn br_and_nbsp() {
        let html = "A&nbsp;B<br/>C";
        let out = normalize_html(html);
        assert_eq!(out.text, "A B\nC");
    }
}

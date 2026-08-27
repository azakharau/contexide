// crates/contexide-normalizer/src/markdown.rs
//! Markdown normalization (MVP).
//!
//! Pipeline:
//! 1) Parse Markdown with `pulldown-cmark` to events.
//! 2) Render a *plain text* view (no markup): keep textual content,
//!    drop formatting markers (backticks, link URLs, image URLs, etc.).
//! 3) Run `text::normalize` to apply NFKC, newline unification,
//!    whitespace collapsing, trimming.
//!
//! Rules (MVP):
//! - Headings/paragraphs become text separated by blank lines (when needed).
//! - Lists: "- " for unordered; "N. " for ordered (numbers preserved).
//! - Links: keep link text only (ignore URL/title).
//! - Images: keep alt text only.
//! - Inline code: keep content, drop backticks.
//! - Code blocks: keep content (no fences), add blank line before/after.
//! - Tables: join cells with '\t', rows separated by newline.
//!
//! The goal is chunk-friendly output with minimal noise.

use crate::{NormalizedText, html::normalize_html, text::normalize as normalize_text};
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

/// Options for Markdown → plain-text rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MdOptions {
    /// Enable GFM-like extensions where helpful.
    pub enable_tables: bool,
    pub enable_footnotes: bool,
    pub enable_tasklists: bool,
    pub enable_strikethrough: bool,
    /// Soft-break handling: convert SoftBreak to space (true) or newline (false).
    pub soft_break_as_space: bool,
}

impl Default for MdOptions {
    fn default() -> Self {
        Self {
            enable_tables: true,
            enable_footnotes: false,
            enable_tasklists: true,
            enable_strikethrough: true,
            soft_break_as_space: true,
        }
    }
}

/// Public helper: minimal defaults.
pub fn normalize_markdown(md: &str) -> NormalizedText {
    normalize_markdown_with(md, MdOptions::default())
}

/// Markdown → normalized text with custom options.
pub fn normalize_markdown_with(md: &str, opts: MdOptions) -> NormalizedText {
    let plain = render_plain_markdown(md, opts);
    normalize_text(&plain)
}

/// Render Markdown into a plain-text string (no markup).
fn render_plain_markdown(md: &str, opts: MdOptions) -> String {
    // Enable useful extensions.
    let mut o = Options::empty();
    if opts.enable_tables {
        o.insert(Options::ENABLE_TABLES);
    }
    if opts.enable_footnotes {
        o.insert(Options::ENABLE_FOOTNOTES);
    }
    if opts.enable_tasklists {
        o.insert(Options::ENABLE_TASKLISTS);
    }
    if opts.enable_strikethrough {
        o.insert(Options::ENABLE_STRIKETHROUGH);
    }

    let parser = Parser::new_ext(md, o);

    // Simple state for lists and tables.
    struct State {
        // Ordered list counters per nesting level (Vec top = deepest).
        // Unordered lists use sentinel 0.
        ordered: Vec<usize>,
        // Table rendering.
        in_table: bool,
        row_cell_idx: usize,
        row_idx: usize,
        // Whether we are at the start of a new block (to decide blank lines).
        at_block_start: bool,
    }
    let mut st = State {
        ordered: Vec::new(),
        in_table: false,
        row_cell_idx: 0,
        row_idx: 0,
        at_block_start: true,
    };

    use std::fmt::Write;
    let mut out = String::with_capacity(md.len());

    // Small helpers
    let ensure_newline = |buf: &mut String| {
        if !buf.ends_with('\n') && !buf.is_empty() {
            buf.push('\n');
        }
    };
    let ensure_blank_line = |buf: &mut String| {
        // ensure exactly one blank line (two newlines)
        if !buf.ends_with("\n\n") {
            ensure_newline(buf);
            buf.push('\n');
        }
    };

    for ev in parser {
        match ev {
            Event::Start(tag) => match tag {
                Tag::Paragraph => {
                    if !st.at_block_start {
                        ensure_newline(&mut out);
                    }
                    st.at_block_start = false;
                }
                Tag::Heading { .. } => {
                    if !st.at_block_start {
                        ensure_blank_line(&mut out);
                    }
                    st.at_block_start = false;
                }
                Tag::BlockQuote(_) => {
                    if !st.at_block_start {
                        ensure_newline(&mut out);
                    }
                    st.at_block_start = false;
                }
                Tag::CodeBlock(_) => {
                    ensure_blank_line(&mut out);
                    st.at_block_start = false;
                }
                Tag::List(start) => {
                    // Ordered list => Some(start), Unordered => None.
                    let start_ix: usize = start.unwrap_or(0).try_into().unwrap_or(0);
                    st.ordered.push(start_ix);
                    if !st.at_block_start {
                        ensure_newline(&mut out);
                    }
                    st.at_block_start = false;
                }
                Tag::Item => {
                    // Prefix with bullet or number.
                    if let Some(top) = st.ordered.last_mut() {
                        if *top == 0 {
                            // unordered
                            out.push_str("- ");
                        } else {
                            // ordered
                            write!(&mut out, "{}. ", *top).ok();
                            *top += 1;
                        }
                    } else {
                        // item without surrounding list — treat as dash
                        out.push_str("- ");
                    }
                }
                Tag::Table(_) => {
                    if !st.at_block_start {
                        ensure_newline(&mut out);
                    }
                    st.in_table = true;
                    st.row_idx = 0;
                    st.at_block_start = false;
                }
                Tag::TableHead | Tag::TableRow => {
                    if st.row_idx > 0 {
                        ensure_newline(&mut out);
                    }
                    st.row_cell_idx = 0;
                    st.row_idx += 1;
                }
                Tag::TableCell => {
                    if st.row_cell_idx > 0 {
                        out.push('\t'); // tab between cells
                    }
                }
                Tag::Link { .. } | Tag::Image { .. } => {
                    // keep only inner text; ignore URL/title entirely
                }
                _ => {}
            },
            Event::End(tag_end) => match tag_end {
                TagEnd::Paragraph => {
                    ensure_newline(&mut out);
                }
                TagEnd::Heading { .. } => {
                    ensure_blank_line(&mut out);
                }
                TagEnd::BlockQuote(_) => {
                    ensure_newline(&mut out);
                }
                TagEnd::CodeBlock => {
                    ensure_blank_line(&mut out);
                }
                TagEnd::List(_) => {
                    let _ = st.ordered.pop();
                    ensure_newline(&mut out);
                }
                TagEnd::Item => {
                    ensure_newline(&mut out);
                }
                TagEnd::Table => {
                    st.in_table = false;
                    ensure_newline(&mut out);
                }
                TagEnd::TableHead | TagEnd::TableRow | TagEnd::TableCell => {
                    // no-op; row/cell separation handled on Start
                }
                TagEnd::Link | TagEnd::Image => {
                    // nothing (we already captured their text)
                }
                _ => {}
            },
            Event::Text(cow) => {
                out.push_str(&cow);
                if st.in_table {
                    st.row_cell_idx += 1;
                }
            }
            Event::Code(cow) => {
                // inline code: keep content only (no backticks)
                out.push_str(&cow);
            }
            Event::Html(_cow) => {
                // raw HTML snippet inside MD; drop in MD normalizer (HTML handled elsewhere)
            }
            Event::SoftBreak => {
                if opts.soft_break_as_space {
                    out.push(' ');
                } else {
                    out.push('\n');
                }
            }
            Event::HardBreak => out.push('\n'),
            Event::Rule => {
                // horizontal rule -> blank line
                ensure_blank_line(&mut out);
            }
            Event::FootnoteReference(_label) => {
                // drop marker; footnote text will appear elsewhere if enabled
            }
            Event::TaskListMarker(_checked) => {
                // drop checkbox; the item text is enough for RAG
            }
            Event::InlineMath(cow) | Event::DisplayMath(cow) => {
                // keep math content only (no $ or $$)
                out.push_str(&cow);
            }
            Event::InlineHtml(cow) => {
                // raw inline HTML; drop in MD normalizer
                out.push_str(&normalize_html(&cow).text);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headings_paragraphs_lists() {
        let md = r#"
# Title

Some *text* here.

- item one
- item two

1. first
2. second
"#;
        let out = normalize_markdown(md);
        let s = out.text.as_str();
        assert!(s.contains("Title"));
        assert!(s.contains("Some text here"));
        assert!(s.contains("item one"));
        assert!(s.contains("1. first"));
        assert!(s.contains("2. second"));
    }

    #[test]
    fn inline_and_block_code() {
        let md = r#"
Inline `a + b`.

```rust
fn main() {}
"#;
        let out = normalize_markdown(md);
        let s = out.text.as_str();
        assert!(s.contains("a + b"));
        assert!(s.contains("fn main() {}"));
        assert!(!s.contains("```"));
    }
}

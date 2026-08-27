//! Task priority model.
//!
//! Higher numeric value means higher priority. Zero is default.

/// Simple priority wrapper. Higher value => higher priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Priority(pub i16);

impl Priority {
    /// Default priority (0).
    pub const DEFAULT: Priority = Priority(0);

    /// Convenience constructor.
    pub const fn new(raw: i16) -> Self {
        Priority(raw)
    }
}

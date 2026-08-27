//! Storage domain types and repository contracts (DB-agnostic).
//!
//! Data structs live here; concrete persistence (SQLx, migrations) belongs
//! in adapter crates.

pub mod entities;
pub mod traits;

pub use entities::*;
pub use traits::*;

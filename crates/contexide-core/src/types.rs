//! `types` — shared domain types (enums + small value objects).
//!
//! These types are used across API, events, and workers. They are independent of
//! any database mapping to keep `core` lightweight. If you want to map them to
//! Postgres enums later, you can enable the `sqlx` derive feature in the
//! workspace and add `#[cfg_attr(feature = "db", derive(sqlx::Type))]` +
//! `#[cfg_attr(feature = "db", sqlx(...))]` attributes back in a follow-up change.

pub mod asset_source;
pub mod block_modality;
pub mod content_address;
pub mod document_status;
pub mod stage;

pub use asset_source::AssetSource;
pub use block_modality::BlockModality;
pub use content_address::ContentAddress;
pub use document_status::DocumentStatus;
pub use stage::Stage;

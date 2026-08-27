//! Tenants domain module.
//!
//! Defines the `Tenant` DTO colocated with potential DB-specific mappings.
//! Minimal MVP fields: stable `id`, human-readable unique `name`, and unique `email`.

pub use contexide_core::storage::Tenant;

pub mod mem;
pub mod pg;

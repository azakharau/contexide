//! Executor configuration entry-point.
//!
//! The actual typed config (`ExecutorConfig`) lives in `contexide-config` so
//! it can be shared by binaries. This module just re-exports the type and
//! provides a small helper for loading it with unified error handling.

use contexide_core::errors::{Error, Result};

pub use contexide_config::ExecutorConfig;

/// Load executor config from `contexide-config`.
pub fn load_executor_config() -> Result<ExecutorConfig> {
    contexide_config::load_executor().map_err(Error::Other)
}

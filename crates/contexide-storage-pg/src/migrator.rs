// crates/contexide-storage-pg/src/migrator.rs
//! Runtime migration runner for `contexide-storage-pg` (sqlx 0.8).
//!
//! - Loads migrations from the crate-local `migrations/` directory at **runtime**.
//! - If the directory is missing, `run_all()` is a no-op (useful in early dev).
//! - Compatible with reversible migrations (<ts>_<name>.up.sql / .down.sql).
//!
//! Usage:
//! ```rust,no_run
//! use contexide_storage_pg::migrator;
//! use contexide_storage_pg::pool::new_pool;
//!
//! # async fn boot() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = new_pool("postgres://...").await?;
//! migrator::run_all(&pool).await?;
//! # Ok(())
//! # }
//! ```

use sqlx::migrate::{MigrateError, Migrator};
use sqlx::{Pool, Postgres};
use std::path::{Path, PathBuf};

fn default_migrations_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

/// Resolve directory with optional override via CONTexIDE_MIGRATIONS_DIR.
/// If both are absent, treat as no-op (MVP-friendly).
fn resolve_dir() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CONTEXIDE_MIGRATIONS_DIR") {
        let path = PathBuf::from(p);
        return Some(path);
    }
    let local = default_migrations_dir();
    if local.exists() { Some(local) } else { None }
}

/// Apply all pending migrations. Safe to call multiple times (idempotent).
///
/// If the `migrations/` directory does not exist, this function returns `Ok(())`.
pub async fn run_all(pool: &Pool<Postgres>) -> Result<(), MigrateError> {
    if let Some(dir) = resolve_dir() {
        Migrator::new(dir).await?.run(pool).await
    } else {
        Ok(())
    }
}

/// Return the list of embedded (loaded) migration filenames for logging.
/// If directory is missing, returns an empty list.
pub async fn migration_names() -> Result<Vec<String>, MigrateError> {
    if let Some(dir) = resolve_dir() {
        let m = Migrator::new(dir).await?;
        Ok(m.migrations
            .iter()
            .map(|x| format!("{}_{:?}", x.version, x.description))
            .collect())
    } else {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn names_dont_panic_without_dir() {
        // Should not error even if `migrations/` is absent.
        let _ = migration_names().await.unwrap();
    }
}

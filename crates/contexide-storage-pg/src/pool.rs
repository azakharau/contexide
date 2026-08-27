//! Postgres pool bootstrap for `sqlx`.
//!
//! Keeps the API minimal and explicit. Adjust timeouts/connections as needed.

use sqlx::{Pool, Postgres, postgres::PgPoolOptions};

/// Create a new Postgres connection pool.
///
/// # Notes
/// - Uses a modest default of 10 connections.
/// - Short acquire timeout to fail fast on misconfiguration.
/// - The `database_url` is a standard Postgres URL, e.g.:
///   `postgres://user:pass@localhost:5432/contexide`.
pub async fn new_pool(database_url: &str) -> Result<Pool<Postgres>, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(database_url)
        .await
}

/// Health-check the pool by performing a trivial `SELECT 1`.
pub async fn ping(pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query_scalar::<_, i32>("select 1")
        .fetch_one(pool)
        .await
        .map(|_| ())
}

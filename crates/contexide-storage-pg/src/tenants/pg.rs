// crates/contexide-storage/src/tenants/pg.rs
//! Postgres implementation of Tenants repository.
//!
//! Explicit SQL with `sqlx`. Creation is idempotent on (name, email) with
//! readable conflict errors for mismatched pairs.
//!
//! ## Expected schema
//! ```sql
//! create table if not exists tenants (
//!   id         uuid primary key,
//!   name       text not null unique,
//!   email      text not null unique,
//!   created_at timestamptz not null default now()
//! );
//! create unique index if not exists uq_tenants_name  on tenants(name);
//! create unique index if not exists uq_tenants_email on tenants(email);
//! ```

use contexide_core::errors::{Error, Result};
use contexide_core::prelude::TenantId;
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::tenants::Tenant;
use crate::traits::{Repository, TenantsRepo};

/// Postgres-backed tenants repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgTenantsRepo {
    pool: Pool<Postgres>,
}

impl PgTenantsRepo {
    /// Build from a pool (cheap clone; `Pool` is `Arc` under the hood).
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    /// Access the underlying pool (e.g., for transactions).
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

/* =============================================================================
FromRow colocated with DTO:
-----------------------------------------------------------------------------
Per our rule, `impl FromRow for Tenant` lives in `tenants/mod.rs`.
We still import PgRow/Row here so query_as::<_, Tenant> compiles cleanly.
============================================================================= */

#[allow(unused_imports)]
use sqlx::postgres::PgRow as _;

/* =============================================================================
Base Repository impl (get / save / delete)
============================================================================= */

#[async_trait::async_trait]
impl Repository for PgTenantsRepo {
    type Key = TenantId;
    type Entity = Tenant;

    /// Fetch by id using `FromRow` impl on `Tenant`.
    async fn get(&self, id: TenantId) -> Result<Option<Tenant>> {
        let row = sqlx::query_as::<_, Tenant>(
            r#"
            select id, name, email
            from tenants
            where id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Upsert by `id`. Returns stored row.
    ///
    /// Notes:
    /// - This is *not* the idempotent create path. It assumes the `id` is known.
    /// - Keeps both `name` and `email` in sync. Violations will bubble as `sqlx::Error`.
    async fn save(&self, entity: Tenant) -> Result<Tenant> {
        let row = sqlx::query_as::<_, Tenant>(
            r#"
            insert into tenants (id, name, email)
            values ($1, $2, $3)
            on conflict (id) do update set
                name  = excluded.name,
                email = excluded.email
            returning id, name, email
            "#,
        )
        .bind::<Uuid>(entity.id.0)
        .bind(&entity.name)
        .bind(&entity.email)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: TenantId) -> Result<bool> {
        let affected = sqlx::query(r#"delete from tenants where id = $1"#)
            .bind::<Uuid>(id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }
}

/* =============================================================================
Domain TenantsRepo impl
============================================================================= */

#[async_trait::async_trait]
impl TenantsRepo for PgTenantsRepo {
    /// Idempotent create by `(name, email)`.
    ///
    /// Semantics (matches in-memory impl):
    /// - If `name` exists:
    ///     - if email matches the same row → return existing id
    ///     - else → conflict error
    /// - Else if `email` exists:
    ///     - if name matches the same row → return existing id
    ///     - else → conflict error
    /// - Else: insert a new row with v7 id and return it.
    async fn create(&self, name: &str, email: &str) -> Result<TenantId> {
        let by_name =
            sqlx::query_as::<_, Tenant>(r#"select id, name, email from tenants where name = $1"#)
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;

        let by_email =
            sqlx::query_as::<_, Tenant>(r#"select id, name, email from tenants where email = $1"#)
                .bind(email)
                .fetch_optional(&self.pool)
                .await?;

        match (by_name, by_email) {
            (Some(n), Some(e)) if n.id == e.id => {
                // Same tenant referenced by both unique keys.
                return Ok(n.id);
            }
            (Some(_n), Some(_)) => {
                return Err(Error::Other(anyhow::anyhow!(
                    "tenant conflict: name '{}' and email '{}' map to different tenants",
                    name,
                    email
                )));
            }
            (Some(n), None) => {
                if n.email == email {
                    return Ok(n.id);
                }
                return Err(Error::Other(anyhow::anyhow!(
                    "tenant conflict: name '{}' already exists with different email",
                    name
                )));
            }
            (None, Some(e)) => {
                if e.name == name {
                    return Ok(e.id);
                }
                return Err(Error::Other(anyhow::anyhow!(
                    "tenant conflict: email '{}' already exists with different name",
                    email
                )));
            }
            (None, None) => {
                let new_id = Uuid::now_v7();
                let id = sqlx::query_scalar::<_, Uuid>(
                    r#"
                    insert into tenants (id, name, email)
                    values ($1, $2, $3)
                    returning id
                    "#,
                )
                .bind(new_id)
                .bind(name)
                .bind(email)
                .fetch_one(&self.pool)
                .await?;
                Ok(TenantId(id))
            }
        }
    }

    /// Lookup by unique name.
    async fn get_by_name(&self, name: &str) -> Result<Option<Tenant>> {
        let row =
            sqlx::query_as::<_, Tenant>(r#"select id, name, email from tenants where name = $1"#)
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    /// Lookup by unique email.
    async fn get_by_email(&self, email: &str) -> Result<Option<Tenant>> {
        let row =
            sqlx::query_as::<_, Tenant>(r#"select id, name, email from tenants where email = $1"#)
                .bind(email)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    /// List tenants (deterministic order by id). Optional limit.
    async fn list(&self, limit: Option<usize>) -> Result<Vec<Tenant>> {
        if let Some(n) = limit {
            let rows = sqlx::query_as::<_, Tenant>(
                r#"
                select id, name, email
                from tenants
                order by id asc
                limit $1
                "#,
            )
            .bind(n as i64)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        } else {
            let rows = sqlx::query_as::<_, Tenant>(
                r#"
                select id, name, email
                from tenants
                order by id asc
                "#,
            )
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }
    }
}

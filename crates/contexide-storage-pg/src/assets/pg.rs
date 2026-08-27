// crates/contexide-repo/src/assets/pg.rs
//! Postgres implementation of `AssetsRepo` + base `Repository`.
//!
//! Uses explicit SQL with `sqlx` and a custom `FromRow` for mapping enums.
//!
//! ## Expected schema (example)
//! ```sql
//! create table if not exists assets (
//!   id            uuid primary key,
//!   tenant_id     uuid not null,
//!   document_id   uuid not null,
//!   source        text not null check (source in ('upload','fetch','generate')),
//!   original_uri  text,
//!   content_type  text not null,
//!   size_bytes    bigint not null,
//!   content_hash  text not null,
//!   storage_key   text,
//!   created_at    timestamptz not null default now()
//! );
//! create index if not exists idx_assets_doc on assets(document_id);
//! ```
//!
//! If your enum variants differ, adjust the CHECK or switch to a native Postgres enum.

use contexide_core::errors::Result;
use contexide_core::prelude::{AssetId, DocumentId};

use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::assets::Asset;
use crate::traits::{AssetsRepo, Repository};

/// Postgres-backed assets repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgAssetsRepo {
    pool: Pool<Postgres>,
}

impl PgAssetsRepo {
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
Base Repository impl
============================================================================= */

#[async_trait::async_trait]
impl Repository for PgAssetsRepo {
    type Key = AssetId;
    type Entity = Asset;

    /// Fetch by id using custom `FromRow`.
    async fn get(&self, id: AssetId) -> Result<Option<Asset>> {
        let row = sqlx::query_as::<_, Asset>(
            r#"
            select id, tenant_id, document_id, source, original_uri,
                   content_type, size_bytes, content_hash, storage_key
            from assets
            where id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Upsert by id: insert or update all mutable fields, then return the stored row.
    ///
    /// Semantics:
    /// - If the id does not exist, insert a new row.
    /// - If it exists, update all fields (MVP behavior; refine later if needed).
    async fn save(&self, entity: Asset) -> Result<Asset> {
        let row = sqlx::query_as::<_, Asset>(
            r#"
            insert into assets (
                id, tenant_id, document_id, source, original_uri,
                content_type, size_bytes, content_hash, storage_key
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            on conflict (id) do update set
                tenant_id    = excluded.tenant_id,
                document_id  = excluded.document_id,
                source       = excluded.source,
                original_uri = excluded.original_uri,
                content_type = excluded.content_type,
                size_bytes   = excluded.size_bytes,
                content_hash = excluded.content_hash,
                storage_key  = excluded.storage_key
            returning id, tenant_id, document_id, source, original_uri,
                      content_type, size_bytes, content_hash, storage_key
            "#,
        )
        .bind::<Uuid>(entity.id.0)
        .bind::<Uuid>(entity.tenant_id.0)
        .bind::<Uuid>(entity.document_id.0)
        .bind::<&'static str>(entity.source.into())
        .bind(entity.original_uri.as_deref())
        .bind(&entity.content_type)
        .bind(entity.size_bytes as i64)
        .bind(&entity.content_hash)
        .bind(entity.storage_key.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: AssetId) -> Result<bool> {
        let affected = sqlx::query(
            r#"
            delete from assets where id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(affected == 1)
    }
}

/* =============================================================================
Domain AssetsRepo impl
============================================================================= */

#[async_trait::async_trait]
impl AssetsRepo for PgAssetsRepo {
    /// List all assets for a document (order by id for deterministic behavior).
    async fn list_by_document(&self, document_id: DocumentId) -> Result<Vec<Asset>> {
        let rows = sqlx::query_as::<_, Asset>(
            r#"
            select id, tenant_id, document_id, source, original_uri,
                   content_type, size_bytes, content_hash, storage_key
            from assets
            where document_id = $1
            order by id
            "#,
        )
        .bind::<Uuid>(document_id.0)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Update `storage_key` for an asset.
    async fn set_storage_key(&self, id: AssetId, storage_key: &str) -> Result<bool> {
        let affected = sqlx::query(
            r#"
            update assets
            set storage_key = $2
            where id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .bind(storage_key)
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(affected == 1)
    }
}

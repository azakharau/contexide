// crates/contexide-storage/src/blocks/pg.rs
//! Postgres implementation of `BlocksRepo` + base `Repository`.
//!
//! Uses explicit SQL with `sqlx` and a custom `FromRow` for mapping enums.
//!
//! ## Expected schema (example)
//! ```sql
//! create table if not exists blocks (
//!   id          uuid primary key,
//!   tenant_id   uuid not null,
//!   asset_id    uuid not null,
//!   modality    text not null check (modality in ('text','image','audio','video')),
//!   order_no    int  not null,
//!   text        text,
//!   meta_json   text,
//!   created_at  timestamptz not null default now()
//! );
//! create index if not exists idx_blocks_asset on blocks(asset_id);
//! create index if not exists idx_blocks_asset_order on blocks(asset_id, order_no);
//! ```
//!
//! If your enum variants differ, adjust the CHECK or switch to a native Postgres enum.

use contexide_core::errors::Result;
use contexide_core::prelude::{AssetId, BlockId};

use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::blocks::Block;
use crate::traits::{BlocksRepo, Repository};

/// Postgres-backed blocks repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgBlocksRepo {
    pool: Pool<Postgres>,
}

impl PgBlocksRepo {
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
impl Repository for PgBlocksRepo {
    type Key = BlockId;
    type Entity = Block;

    /// Fetch by id using custom `FromRow`.
    async fn get(&self, id: BlockId) -> Result<Option<Block>> {
        let row = sqlx::query_as::<_, Block>(
            r#"
            select id, tenant_id, asset_id, modality, order_no, text, meta_json
            from blocks
            where id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Upsert by id: insert or update mutable fields, return stored row.
    ///
    /// Semantics:
    /// - If the id does not exist, insert a new row.
    /// - If it exists, update all fields (MVP behavior; refine later if needed).
    async fn save(&self, entity: Block) -> Result<Block> {
        let row = sqlx::query_as::<_, Block>(
            r#"
            insert into blocks (
                id, tenant_id, asset_id, modality, order_no, text, meta_json
            ) values ($1, $2, $3, $4, $5, $6, $7)
            on conflict (id) do update set
                tenant_id = excluded.tenant_id,
                asset_id  = excluded.asset_id,
                modality  = excluded.modality,
                order_no  = excluded.order_no,
                text      = excluded.text,
                meta_json = excluded.meta_json
            returning id, tenant_id, asset_id, modality, order_no, text, meta_json
            "#,
        )
        .bind::<Uuid>(entity.id.0)
        .bind::<Uuid>(entity.tenant_id.0)
        .bind::<Uuid>(entity.asset_id.0)
        .bind::<&'static str>(entity.modality.into()) // enum -> lowercase str
        .bind(entity.order_no)
        .bind(entity.text.as_deref())
        .bind(entity.meta_json.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: BlockId) -> Result<bool> {
        let affected = sqlx::query(r#"delete from blocks where id = $1"#)
            .bind::<Uuid>(id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }
}

/* =============================================================================
Domain BlocksRepo impl
============================================================================= */

#[async_trait::async_trait]
impl BlocksRepo for PgBlocksRepo {
    /// List blocks for an asset ordered by (`order_no`, then `id`).
    async fn list_by_asset(&self, asset_id: AssetId) -> Result<Vec<Block>> {
        let rows = sqlx::query_as::<_, Block>(
            r#"
            select id, tenant_id, asset_id, modality, order_no, text, meta_json
            from blocks
            where asset_id = $1
            order by order_no asc, id asc
            "#,
        )
        .bind::<Uuid>(asset_id.0)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Delete all blocks for a given asset. Returns count of removed blocks.
    async fn delete_by_asset(&self, asset_id: AssetId) -> Result<u64> {
        let affected = sqlx::query(r#"delete from blocks where asset_id = $1"#)
            .bind::<Uuid>(asset_id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected as u64)
    }
}

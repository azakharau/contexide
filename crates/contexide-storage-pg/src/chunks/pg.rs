// crates/contexide-storage/src/chunks/pg.rs
//! Postgres implementation of `ChunksRepo` + base `Repository`.
//!
//! Uses explicit SQL with `sqlx` and a custom `FromRow` for the DTO.
//!
//! ## Expected schema (example)
//! ```sql
//! create table if not exists chunks (
//!   id            uuid primary key,
//!   tenant_id     uuid not null,
//!   chunk_set_id  uuid not null,
//!   order_no      int  not null,
//!   byte_start    int  not null,
//!   byte_end      int  not null,
//!   text          text not null,
//!   meta_json     text,
//!   created_at    timestamptz not null default now()
//! );
//! create index if not exists idx_chunks_set        on chunks(chunk_set_id);
//! create index if not exists idx_chunks_set_order  on chunks(chunk_set_id, order_no);
//! ```

use contexide_core::errors::Result;
use contexide_core::prelude::{ChunkId, ChunkSetId};
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::chunks::Chunk;
use crate::traits::{ChunksRepo, Repository};

/// Postgres-backed chunks repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgChunksRepo {
    pool: Pool<Postgres>,
}

impl PgChunksRepo {
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
Base Repository impl (get / save / delete)
============================================================================= */

#[async_trait::async_trait]
impl Repository for PgChunksRepo {
    type Key = ChunkId;
    type Entity = Chunk;

    /// Fetch by id using custom `FromRow`.
    async fn get(&self, id: ChunkId) -> Result<Option<Chunk>> {
        let row = sqlx::query_as::<_, Chunk>(
            r#"
            select id, tenant_id, chunk_set_id, order_no, byte_start, byte_end, text, meta_json
            from chunks
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
    async fn save(&self, entity: Chunk) -> Result<Chunk> {
        let row = sqlx::query_as::<_, Chunk>(
            r#"
            insert into chunks (
                id, tenant_id, chunk_set_id, order_no, byte_start, byte_end, text, meta_json
            ) values ($1, $2, $3, $4, $5, $6, $7, $8)
            on conflict (id) do update set
                chunk_set_id = excluded.chunk_set_id,
                order_no     = excluded.order_no,
                byte_start   = excluded.byte_start,
                byte_end     = excluded.byte_end,
                text         = excluded.text,
                meta_json    = excluded.meta_json
            returning id, tenant_id, chunk_set_id, order_no, byte_start, byte_end, text, meta_json
            "#,
        )
        .bind::<Uuid>(entity.id.0)
        .bind::<Uuid>(entity.tenant_id.0)
        .bind::<Uuid>(entity.chunk_set_id.0)
        .bind(entity.order_no)
        .bind(entity.byte_start)
        .bind(entity.byte_end)
        .bind(&entity.text)
        .bind(entity.meta_json.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: ChunkId) -> Result<bool> {
        let affected = sqlx::query(r#"delete from chunks where id = $1"#)
            .bind::<Uuid>(id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }
}

/* =============================================================================
Domain ChunksRepo impl
============================================================================= */

#[async_trait::async_trait]
impl ChunksRepo for PgChunksRepo {
    /// List chunks for a set ordered by (`order_no`, then `id`).
    async fn list_by_set(&self, chunk_set_id: ChunkSetId) -> Result<Vec<Chunk>> {
        let rows = sqlx::query_as::<_, Chunk>(
            r#"
            select id, tenant_id, chunk_set_id, order_no, byte_start, byte_end, text, meta_json
            from chunks
            where chunk_set_id = $1
            order by order_no asc, id asc
            "#,
        )
        .bind::<Uuid>(chunk_set_id.0)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Delete all chunks of a given set. Returns count of removed chunks.
    async fn delete_by_set(&self, chunk_set_id: ChunkSetId) -> Result<u64> {
        let affected = sqlx::query(r#"delete from chunks where chunk_set_id = $1"#)
            .bind::<Uuid>(chunk_set_id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected as u64)
    }
}

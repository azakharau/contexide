// crates/contexide-storage/src/embeddings_ref/pg.rs
//! Postgres implementation of `EmbeddingsRepo` + base `Repository`.
//!
//! Stores the mapping for a (chunk_id, embedding_set_id) pair to an opaque
//! `vector_id` in the external vector DB. Enables idempotency, progress tracking,
//! and cleanups without coupling to a specific vDB.
//!
//! ## Expected schema (example)
//! ```sql
//! create table embedding_refs (
//!   chunk_id         uuid not null,
//!   embedding_set_id uuid not null,
//!   tenant_id        uuid not null,
//!   vector_id        text not null,
//!   created_at       timestamptz not null default now(),
//!   primary key (chunk_id, embedding_set_id)
//! );
//!
//! create index idx_embedding_refs_set    on embedding_refs(embedding_set_id);
//! create index idx_embedding_refs_tenant on embedding_refs(tenant_id);
//! ```

use contexide_core::{
    errors::Result,
    prelude::{ChunkId, EmbeddingSetId},
};
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::{
    embedding_refs::EmbeddingRef,
    traits::{EmbeddingsRepo, Repository},
};

/// Postgres-backed embedding_refs repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgEmbeddingsRefRepo {
    pool: Pool<Postgres>,
}

impl PgEmbeddingsRefRepo {
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
impl Repository for PgEmbeddingsRefRepo {
    type Key = (ChunkId, EmbeddingSetId);
    type Entity = EmbeddingRef;

    /// Fetch by composite key using custom `FromRow`.
    async fn get(&self, id: Self::Key) -> Result<Option<EmbeddingRef>> {
        let (chunk_id, set_id) = id;
        let row = sqlx::query_as::<_, EmbeddingRef>(
            r#"
            select chunk_id, embedding_set_id, tenant_id, vector_id
            from embedding_refs
            where chunk_id = $1 and embedding_set_id = $2
            "#,
        )
        .bind::<Uuid>(chunk_id.0)
        .bind::<Uuid>(set_id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Upsert by composite key and return the stored row.
    ///
    /// - If (chunk_id, embedding_set_id) does not exist, insert a new row.
    /// - If exists, update `tenant_id` and `vector_id` (MVP behavior).
    async fn save(&self, entity: EmbeddingRef) -> Result<EmbeddingRef> {
        let row = sqlx::query_as::<_, EmbeddingRef>(
            r#"
            insert into embedding_refs (chunk_id, embedding_set_id, tenant_id, vector_id)
            values ($1, $2, $3, $4)
            on conflict (chunk_id, embedding_set_id) do update set
                tenant_id = excluded.tenant_id,
                vector_id = excluded.vector_id
            returning chunk_id, embedding_set_id, tenant_id, vector_id
            "#,
        )
        .bind::<Uuid>(entity.chunk_id.0)
        .bind::<Uuid>(entity.embedding_set_id.0)
        .bind::<Uuid>(entity.tenant_id.0)
        .bind(&entity.vector_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// Delete by composite key; returns whether a row was removed.
    async fn delete(&self, id: Self::Key) -> Result<bool> {
        let (chunk_id, set_id) = id;
        let affected = sqlx::query(
            r#"
            delete from embedding_refs
            where chunk_id = $1 and embedding_set_id = $2
            "#,
        )
        .bind::<Uuid>(chunk_id.0)
        .bind::<Uuid>(set_id.0)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected == 1)
    }
}

/* =============================================================================
Domain EmbeddingsRepo impl
============================================================================= */

#[async_trait::async_trait]
impl EmbeddingsRepo for PgEmbeddingsRefRepo {
    /// List mappings for a set (deterministic order by `chunk_id`, then `embedding_set_id`).
    async fn list_by_set(&self, embedding_set_id: EmbeddingSetId) -> Result<Vec<EmbeddingRef>> {
        let rows = sqlx::query_as::<_, EmbeddingRef>(
            r#"
            select chunk_id, embedding_set_id, tenant_id, vector_id
            from embedding_refs
            where embedding_set_id = $1
            order by chunk_id asc, embedding_set_id asc
            "#,
        )
        .bind::<Uuid>(embedding_set_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Delete all mappings for a set. Returns count of removed rows.
    async fn delete_by_set(&self, embedding_set_id: EmbeddingSetId) -> Result<u64> {
        let affected = sqlx::query(
            r#"
            delete from embedding_refs
            where embedding_set_id = $1
            "#,
        )
        .bind::<Uuid>(embedding_set_id.0)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected as u64)
    }
}

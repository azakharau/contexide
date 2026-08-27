// crates/contexide-storage/src/embedding_sets/pg.rs
//! Postgres implementation of `EmbeddingSetsRepo` + base `Repository`.
//!
//! Uses explicit SQL with `sqlx` and a custom `FromRow` for the DTO.
//!
//! ## Expected schema (example)
//! ```sql
//! create table if not exists embedding_sets (
//!   id            uuid primary key,
//!   tenant_id     uuid not null,
//!   chunk_set_id  uuid not null,
//!   model_kind    text not null,
//!   model_version text not null,
//!   dim           int  not null,
//!   metric        text not null,          -- e.g. 'cosine' | 'dot' | 'l2'
//!   ready         boolean not null default false,
//!   created_at    timestamptz not null default now()
//! );
//! create index if not exists idx_embedding_sets_chunk_set on embedding_sets(chunk_set_id);
//! create index if not exists idx_embedding_sets_ready on embedding_sets(ready);
//! ```

use contexide_core::errors::Result;
use contexide_core::prelude::{ChunkSetId, EmbeddingSetId, TenantId};
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::{
    embedding_sets::EmbeddingSet,
    traits::{EmbeddingSetsRepo, Repository},
};

/// Postgres-backed embedding_sets repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgEmbeddingSetsRepo {
    pool: Pool<Postgres>,
}

impl PgEmbeddingSetsRepo {
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
impl Repository for PgEmbeddingSetsRepo {
    type Key = EmbeddingSetId;
    type Entity = EmbeddingSet;

    /// Fetch by id using custom `FromRow`.
    async fn get(&self, id: EmbeddingSetId) -> Result<Option<EmbeddingSet>> {
        let row = sqlx::query_as::<_, EmbeddingSet>(
            r#"
            select id, tenant_id, chunk_set_id, model_kind, model_version,
                   dim, metric, ready
            from embedding_sets
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
    async fn save(&self, entity: EmbeddingSet) -> Result<EmbeddingSet> {
        let row = sqlx::query_as::<_, EmbeddingSet>(
            r#"
            insert into embedding_sets (
                id, tenant_id, chunk_set_id, model_kind, model_version,
                dim, metric, ready
            ) values ($1, $2, $3, $4, $5, $6, $7, $8)
            on conflict (id) do update set
                tenant_id    = excluded.tenant_id,
                chunk_set_id = excluded.chunk_set_id,
                model_kind   = excluded.model_kind,
                model_version= excluded.model_version,
                dim          = excluded.dim,
                metric       = excluded.metric,
                ready        = excluded.ready
            returning id, tenant_id, chunk_set_id, model_kind, model_version,
                      dim, metric, ready
            "#,
        )
        .bind::<Uuid>(entity.id.0)
        .bind::<Uuid>(entity.tenant_id.0)
        .bind::<Uuid>(entity.chunk_set_id.0)
        .bind(&entity.model_kind)
        .bind(&entity.model_version)
        .bind(entity.dim)
        .bind(&entity.metric)
        .bind(entity.ready)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: EmbeddingSetId) -> Result<bool> {
        let affected = sqlx::query(r#"delete from embedding_sets where id = $1"#)
            .bind::<Uuid>(id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }
}

/* =============================================================================
Domain EmbeddingSetsRepo impl
============================================================================= */

#[async_trait::async_trait]
impl EmbeddingSetsRepo for PgEmbeddingSetsRepo {
    /// Create a new embedding set with generated id (UUIDv7) and `ready=false`.
    async fn create(
        &self,
        tenant_id: TenantId,
        chunk_set_id: ChunkSetId,
        model_kind: &str,
        model_version: &str,
        dim: i32,
        metric: &str,
    ) -> Result<EmbeddingSetId> {
        let new_id = Uuid::now_v7();
        let id = sqlx::query_scalar::<_, Uuid>(
            r#"
            insert into embedding_sets (
                id, tenant_id, chunk_set_id, model_kind, model_version,
                dim, metric, ready
            ) values ($1, $2, $3, $4, $5, $6, $7, false)
            returning id
            "#,
        )
        .bind(new_id)
        .bind::<Uuid>(tenant_id.0)
        .bind::<Uuid>(chunk_set_id.0)
        .bind(model_kind)
        .bind(model_version)
        .bind(dim)
        .bind(metric)
        .fetch_one(&self.pool)
        .await?;

        Ok(EmbeddingSetId(id))
    }

    /// Mark as ready. Returns `Ok(false)` if not found.
    async fn mark_ready(&self, id: EmbeddingSetId) -> Result<bool> {
        let affected = sqlx::query(r#"update embedding_sets set ready = true where id = $1"#)
            .bind::<Uuid>(id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }

    /// List all embedding sets for a chunk set (ordered deterministically by id).
    async fn list_by_chunk_set(&self, chunk_set_id: ChunkSetId) -> Result<Vec<EmbeddingSet>> {
        let rows = sqlx::query_as::<_, EmbeddingSet>(
            r#"
            select id, tenant_id, chunk_set_id, model_kind, model_version,
                   dim, metric, ready
            from embedding_sets
            where chunk_set_id = $1
            order by id asc
            "#,
        )
        .bind::<Uuid>(chunk_set_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

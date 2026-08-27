// crates/contexide-storage/src/chunk_sets/pg.rs
//! Postgres implementation of `ChunkSetsRepo` + base `Repository`.
//!
//! Uses explicit SQL with `sqlx` and a custom `FromRow` for the DTO.
//!
//! ## Expected schema (example)
//! ```sql
//! create table if not exists chunk_sets (
//!   id            uuid primary key,
//!   tenant_id     uuid not null,
//!   document_id   uuid not null,
//!   profile_hash  text not null,
//!   finalized     boolean not null default false,
//!   created_at    timestamptz not null default now()
//! );
//! create index if not exists idx_chunk_sets_doc on chunk_sets(document_id);
//! create index if not exists idx_chunk_sets_doc_final on chunk_sets(document_id, finalized);
//! ```

use contexide_core::errors::Result;
use contexide_core::prelude::{ChunkSetId, DocumentId, TenantId};
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::chunk_sets::ChunkSet;
use crate::traits::{ChunkSetsRepo, Repository};

/// Postgres-backed chunk_sets repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgChunkSetsRepo {
    pool: Pool<Postgres>,
}

impl PgChunkSetsRepo {
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
impl Repository for PgChunkSetsRepo {
    type Key = ChunkSetId;
    type Entity = ChunkSet;

    /// Fetch by id using custom `FromRow`.
    async fn get(&self, id: ChunkSetId) -> Result<Option<ChunkSet>> {
        let row = sqlx::query_as::<_, ChunkSet>(
            r#"
            select id, tenant_id, document_id, profile_hash, finalized
            from chunk_sets
            where id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Upsert by id: insert or update fields, return stored row.
    ///
    /// Semantics:
    /// - If the id does not exist, insert a new row.
    /// - If it exists, update all mutable fields (MVP behavior).
    async fn save(&self, entity: ChunkSet) -> Result<ChunkSet> {
        let row = sqlx::query_as::<_, ChunkSet>(
            r#"
            insert into chunk_sets (
                id, tenant_id, document_id, profile_hash, finalized
            ) values ($1, $2, $3, $4, $5)
            on conflict (id) do update set
                tenant_id    = excluded.tenant_id,
                document_id  = excluded.document_id,
                profile_hash = excluded.profile_hash,
                finalized    = excluded.finalized
            returning id, tenant_id, document_id, profile_hash, finalized
            "#,
        )
        .bind::<Uuid>(entity.id.0)
        .bind::<Uuid>(entity.tenant_id.0)
        .bind::<Uuid>(entity.document_id.0)
        .bind(&entity.profile_hash)
        .bind(entity.finalized)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: ChunkSetId) -> Result<bool> {
        let affected = sqlx::query(r#"delete from chunk_sets where id = $1"#)
            .bind::<Uuid>(id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }
}

/* =============================================================================
Domain ChunkSetsRepo impl
============================================================================= */

#[async_trait::async_trait]
impl ChunkSetsRepo for PgChunkSetsRepo {
    /// Create a new chunk set with generated id (UUIDv7) and `finalized=false`.
    async fn create(
        &self,
        tenant_id: TenantId,
        document_id: DocumentId,
        profile_hash: &str,
    ) -> Result<ChunkSetId> {
        let new_id = Uuid::now_v7();
        let id = sqlx::query_scalar::<_, Uuid>(
            r#"
            insert into chunk_sets (id, tenant_id, document_id, profile_hash, finalized)
            values ($1, $2, $3, $4, false)
            returning id
            "#,
        )
        .bind(new_id)
        .bind::<Uuid>(tenant_id.0)
        .bind::<Uuid>(document_id.0)
        .bind(profile_hash)
        .fetch_one(&self.pool)
        .await?;

        Ok(ChunkSetId(id))
    }

    /// Mark as finalized. Returns `Ok(false)` if not found.
    async fn mark_finalized(&self, id: ChunkSetId) -> Result<bool> {
        let affected = sqlx::query(r#"update chunk_sets set finalized = true where id = $1"#)
            .bind::<Uuid>(id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }

    /// List all sets for a document (ordered deterministically by id).
    async fn list_by_document(&self, document_id: DocumentId) -> Result<Vec<ChunkSet>> {
        let rows = sqlx::query_as::<_, ChunkSet>(
            r#"
            select id, tenant_id, document_id, profile_hash, finalized
            from chunk_sets
            where document_id = $1
            order by id asc
            "#,
        )
        .bind::<Uuid>(document_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

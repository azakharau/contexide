//! Minimal repository for the `documents` table.
//!
//! This module exposes a small repository trait plus a Postgres-backed
//! implementation. Free functions are provided for quick ergonomics.
//!
//! Keep things explicit and boring: no derive macros, no hidden magic.
//! If you introduce a Postgres enum later, you can swap the TEXT `status`
//! column and move DB type mappings into `contexide_core`.

use contexide_core::prelude::{DocumentId, DocumentStatus, Result};
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use crate::traits::{DocumentsRepo, Repository};

use super::Document;

/* =============================================================================
Postgres implementation
============================================================================= */

/// Postgres-backed repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgDocumentsRepo {
    pool: Pool<Postgres>,
}

impl PgDocumentsRepo {
    /// Build from a pool (cheap clone; `Pool` is `Arc` under the hood).
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    /// Access the underlying pool (useful for transactions in advanced flows).
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

#[async_trait::async_trait]
impl DocumentsRepo for PgDocumentsRepo {
    async fn set_status(&self, id: DocumentId, status: DocumentStatus) -> Result<bool> {
        let status_str: &'static str = status.into();

        let affected = sqlx::query(
            r#"
            UPDATE documents
            SET status = $2
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .bind(status_str)
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(affected == 1)
    }
}

#[async_trait::async_trait]
impl Repository for PgDocumentsRepo {
    type Key = DocumentId;
    type Entity = Document;

    async fn get(&self, id: DocumentId) -> Result<Option<Document>> {
        let rec = sqlx::query_as::<_, Document>(
            r#"
            SELECT id, tenant_id, title, status
            FROM documents
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .fetch_optional(&self.pool)
        .await?;

        Ok(rec)
    }

    async fn save(&self, mut entity: Document) -> Result<Document> {
        if entity.id.0.is_nil() {
            entity.id = DocumentId(Uuid::now_v7());
            sqlx::query(
                r#"
                INSERT INTO documents (id, tenant_id, title, status)
                VALUES ($1, $2, $3, $4)
                "#,
            )
            .bind::<Uuid>(entity.id.0)
            .bind::<Uuid>(entity.tenant_id.0)
            .bind(&entity.title)
            .bind::<&str>(entity.status.into())
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                r#"
                UPDATE documents
                SET tenant_id = $2, title = $3, status = $4
                WHERE id = $1
                "#,
            )
            .bind::<Uuid>(entity.id.0)
            .bind::<Uuid>(entity.tenant_id.0)
            .bind(&entity.title)
            .bind::<&str>(entity.status.into())
            .execute(&self.pool)
            .await?;
        }
        Ok(entity)
    }

    async fn delete(&self, id: DocumentId) -> Result<bool> {
        let affected = sqlx::query(
            r#"
            DELETE FROM documents
            WHERE id = $1
            "#,
        )
        .bind::<Uuid>(id.0)
        .execute(&self.pool)
        .await?
        .rows_affected();

        Ok(affected == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_conversions_compile() {
        // compile-time sanity for conversions
        let s: &'static str = DocumentStatus::Ready.into();
        assert_eq!(s, "ready");

        let parsed = DocumentStatus::try_from("failed").unwrap();
        assert!(matches!(parsed, DocumentStatus::Failed));
    }
}

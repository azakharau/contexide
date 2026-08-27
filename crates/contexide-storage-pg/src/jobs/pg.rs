// crates/contexide-storage/src/jobs/pg.rs
//! Postgres implementation of `JobsRepo` + base `Repository`.
//!
//! Uses explicit SQL with `sqlx` and a custom `FromRow` for enum mapping.
//!
//! ## Expected schema (example)
//! ```sql
//! create table if not exists jobs (
//!   id           uuid primary key,
//!   tenant_id    uuid not null,
//!   kind         text not null check (kind in ('ingest','extract','normalize','chunk','embed','index')),
//!   status       text not null check (status in ('pending','running','done','failed')),
//!   payload_json text,
//!   created_at   timestamptz not null default now()
//! );
//! create index if not exists idx_jobs_kind on jobs(kind);
//! create index if not exists idx_jobs_kind_status on jobs(kind, status);
//! ```

use contexide_core::errors::Result;
use contexide_core::prelude::{JobId, TenantId};
use sqlx::{Pool, Postgres};
use uuid::Uuid;

use super::{Job, JobKind, JobStatus};
use crate::traits::{JobsRepo, Repository};

/// Postgres-backed jobs repository (wraps a `sqlx::Pool<Postgres>`).
pub struct PgJobsRepo {
    pool: Pool<Postgres>,
}

impl PgJobsRepo {
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
impl Repository for PgJobsRepo {
    type Key = JobId;
    type Entity = Job;

    /// Fetch by id using custom `FromRow`.
    async fn get(&self, id: JobId) -> Result<Option<Job>> {
        let row = sqlx::query_as::<_, Job>(
            r#"
            select id, tenant_id, kind, status, payload_json
            from jobs
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
    /// - If it exists, update fields (MVP behavior).
    async fn save(&self, entity: Job) -> Result<Job> {
        let kind_str: &'static str = entity.kind.into();
        let status_str: &'static str = entity.status.into();

        let row = sqlx::query_as::<_, Job>(
            r#"
            insert into jobs (id, tenant_id, kind, status, payload_json)
            values ($1, $2, $3, $4, $5)
            on conflict (id) do update set
                tenant_id    = excluded.tenant_id,
                kind         = excluded.kind,
                status       = excluded.status,
                payload_json = excluded.payload_json
            returning id, tenant_id, kind, status, payload_json
            "#,
        )
        .bind::<Uuid>(entity.id.0)
        .bind::<Uuid>(entity.tenant_id.0)
        .bind(kind_str)
        .bind(status_str)
        .bind(entity.payload_json.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: JobId) -> Result<bool> {
        let affected = sqlx::query(r#"delete from jobs where id = $1"#)
            .bind::<Uuid>(id.0)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }
}

/* =============================================================================
Domain JobsRepo impl
============================================================================= */

#[async_trait::async_trait]
impl JobsRepo for PgJobsRepo {
    /// Create a new job with generated id (UUIDv7).
    async fn create(
        &self,
        tenant_id: TenantId,
        kind: JobKind,
        status: JobStatus,
        payload_json: Option<String>,
    ) -> Result<JobId> {
        let new_id = Uuid::now_v7();
        let kind_str: &'static str = kind.into();
        let status_str: &'static str = status.into();

        let id = sqlx::query_scalar::<_, Uuid>(
            r#"
            insert into jobs (id, tenant_id, kind, status, payload_json)
            values ($1, $2, $3, $4, $5)
            returning id
            "#,
        )
        .bind(new_id)
        .bind::<Uuid>(tenant_id.0)
        .bind(kind_str)
        .bind(status_str)
        .bind(payload_json.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JobId(id))
    }

    /// Update job status. Returns `Ok(false)` if not found.
    async fn set_status(&self, id: JobId, status: JobStatus) -> Result<bool> {
        let status_str: &'static str = status.into();
        let affected = sqlx::query(r#"update jobs set status = $2 where id = $1"#)
            .bind::<Uuid>(id.0)
            .bind(status_str)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(affected == 1)
    }

    /// List jobs by kind & status (optionally limited).
    async fn list_by_kind_status(
        &self,
        kind: JobKind,
        status: JobStatus,
        limit: Option<usize>,
    ) -> Result<Vec<Job>> {
        let kind_str: &'static str = kind.into();
        let status_str: &'static str = status.into();

        if let Some(n) = limit {
            let rows = sqlx::query_as::<_, Job>(
                r#"
                select id, tenant_id, kind, status, payload_json
                from jobs
                where kind = $1 and status = $2
                order by id asc
                limit $3
                "#,
            )
            .bind(kind_str)
            .bind(status_str)
            .bind(n as i64)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        } else {
            let rows = sqlx::query_as::<_, Job>(
                r#"
                select id, tenant_id, kind, status, payload_json
                from jobs
                where kind = $1 and status = $2
                order by id asc
                "#,
            )
            .bind(kind_str)
            .bind(status_str)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows)
        }
    }
}

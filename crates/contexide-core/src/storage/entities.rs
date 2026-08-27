use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::prelude::{
    AssetId, BlockId, ChunkId, ChunkSetId, DocumentId, EmbeddingSetId, JobId, TenantId,
};
use crate::types::{AssetSource, BlockModality, DocumentStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: TenantId,
    pub name: String,
    pub email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub tenant_id: TenantId,
    pub title: String,
    pub status: DocumentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: AssetId,
    pub tenant_id: TenantId,
    pub document_id: DocumentId,
    pub source: AssetSource,
    pub original_uri: Option<String>,
    pub content_type: String,
    pub size_bytes: u64,
    pub content_hash: String,
    pub storage_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub id: BlockId,
    pub tenant_id: TenantId,
    pub asset_id: AssetId,
    pub modality: BlockModality,
    pub order_no: i32,
    pub text: Option<String>,
    pub meta_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkSet {
    pub id: ChunkSetId,
    pub tenant_id: TenantId,
    pub document_id: DocumentId,
    pub profile_hash: String,
    pub finalized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub id: ChunkId,
    pub chunk_set_id: ChunkSetId,
    pub tenant_id: TenantId,
    pub order_no: i32,
    pub byte_start: i32,
    pub byte_end: i32,
    pub text: String,
    pub meta_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingSet {
    pub id: EmbeddingSetId,
    pub tenant_id: TenantId,
    pub chunk_set_id: ChunkSetId,
    pub model_kind: String,
    pub model_version: String,
    pub dim: i32,
    pub metric: String,
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingRef {
    pub chunk_id: ChunkId,
    pub embedding_set_id: EmbeddingSetId,
    pub tenant_id: TenantId,
    pub vector_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Ingest,
    Extract,
    Normalize,
    Chunk,
    Embed,
    Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub tenant_id: TenantId,
    pub kind: JobKind,
    pub status: JobStatus,
    pub payload_json: Option<String>,
}

impl TryFrom<&str> for JobKind {
    type Error = ();
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(match s {
            "ingest" => JobKind::Ingest,
            "extract" => JobKind::Extract,
            "normalize" => JobKind::Normalize,
            "chunk" => JobKind::Chunk,
            "embed" => JobKind::Embed,
            "index" => JobKind::Index,
            _ => return Err(()),
        })
    }
}

impl From<JobKind> for &'static str {
    fn from(k: JobKind) -> Self {
        match k {
            JobKind::Ingest => "ingest",
            JobKind::Extract => "extract",
            JobKind::Normalize => "normalize",
            JobKind::Chunk => "chunk",
            JobKind::Embed => "embed",
            JobKind::Index => "index",
        }
    }
}

impl TryFrom<&str> for JobStatus {
    type Error = ();
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(match s {
            "pending" => JobStatus::Pending,
            "running" => JobStatus::Running,
            "done" => JobStatus::Done,
            "failed" => JobStatus::Failed,
            _ => return Err(()),
        })
    }
}

impl From<JobStatus> for &'static str {
    fn from(s: JobStatus) -> Self {
        match s {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Done => "done",
            JobStatus::Failed => "failed",
        }
    }
}

#[cfg(feature = "db")]
mod db_impls {
    use super::*;
    use sqlx::{FromRow, Row};

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for Tenant {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            Ok(Tenant {
                id: TenantId(row.try_get("id")?),
                name: row.try_get("name")?,
                email: row.try_get("email")?,
            })
        }
    }

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for Document {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            let status_raw: String = row.try_get("status")?;
            let status =
                DocumentStatus::try_from(status_raw.as_str()).unwrap_or(DocumentStatus::Draft);
            Ok(Document {
                id: DocumentId(row.try_get("id")?),
                tenant_id: TenantId(row.try_get("tenant_id")?),
                title: row.try_get("title")?,
                status,
            })
        }
    }

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for Asset {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            let source_raw: String = row.try_get("source")?;
            let source =
                AssetSource::from_str(&source_raw).map_err(|_| sqlx::Error::ColumnDecode {
                    index: "source".into(),
                    source: Box::new(std::fmt::Error),
                })?;
            Ok(Asset {
                id: AssetId(row.try_get("id")?),
                tenant_id: TenantId(row.try_get("tenant_id")?),
                document_id: DocumentId(row.try_get("document_id")?),
                source,
                original_uri: row.try_get("original_uri")?,
                content_type: row.try_get("content_type")?,
                size_bytes: {
                    let v: i64 = row.try_get("size_bytes")?;
                    v as u64
                },
                content_hash: row.try_get("content_hash")?,
                storage_key: row.try_get("storage_key")?,
            })
        }
    }

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for Block {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            let modality_raw: String = row.try_get("modality")?;
            let modality = BlockModality::from_str(&modality_raw).unwrap_or(BlockModality::Text);
            Ok(Block {
                id: BlockId(row.try_get("id")?),
                tenant_id: TenantId(row.try_get("tenant_id")?),
                asset_id: AssetId(row.try_get("asset_id")?),
                modality,
                order_no: row.try_get("order_no")?,
                text: row.try_get("text")?,
                meta_json: row.try_get("meta_json")?,
            })
        }
    }

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for ChunkSet {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            Ok(ChunkSet {
                id: ChunkSetId(row.try_get("id")?),
                tenant_id: TenantId(row.try_get("tenant_id")?),
                document_id: DocumentId(row.try_get("document_id")?),
                profile_hash: row.try_get("profile_hash")?,
                finalized: row.try_get("finalized")?,
            })
        }
    }

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for Chunk {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            Ok(Chunk {
                id: ChunkId(row.try_get("id")?),
                chunk_set_id: ChunkSetId(row.try_get("chunk_set_id")?),
                tenant_id: TenantId(row.try_get("tenant_id")?),
                order_no: row.try_get("order_no")?,
                byte_start: row.try_get("byte_start")?,
                byte_end: row.try_get("byte_end")?,
                text: row.try_get("text")?,
                meta_json: row.try_get("meta_json")?,
            })
        }
    }

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for EmbeddingSet {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            Ok(EmbeddingSet {
                id: EmbeddingSetId(row.try_get("id")?),
                tenant_id: TenantId(row.try_get("tenant_id")?),
                chunk_set_id: ChunkSetId(row.try_get("chunk_set_id")?),
                model_kind: row.try_get("model_kind")?,
                model_version: row.try_get("model_version")?,
                dim: row.try_get("dim")?,
                metric: row.try_get("metric")?,
                ready: row.try_get("ready")?,
            })
        }
    }

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for EmbeddingRef {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            Ok(EmbeddingRef {
                chunk_id: ChunkId(row.try_get("chunk_id")?),
                embedding_set_id: EmbeddingSetId(row.try_get("embedding_set_id")?),
                tenant_id: TenantId(row.try_get("tenant_id")?),
                vector_id: row.try_get("vector_id")?,
            })
        }
    }

    impl<'r> FromRow<'r, sqlx::postgres::PgRow> for Job {
        fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
            let kind_raw: String = row.try_get("kind")?;
            let kind = JobKind::try_from(kind_raw.as_str()).unwrap_or(JobKind::Ingest);
            let status_raw: String = row.try_get("status")?;
            let status = JobStatus::try_from(status_raw.as_str()).unwrap_or(JobStatus::Pending);
            Ok(Job {
                id: JobId(row.try_get("id")?),
                tenant_id: TenantId(row.try_get("tenant_id")?),
                kind,
                status,
                payload_json: row.try_get("payload_json")?,
            })
        }
    }
}

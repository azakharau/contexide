use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::prelude::{
    AssetId, BlockId, ChunkId, ChunkSetId, DocumentId, EmbeddingSetId, JobId, TenantId,
};
use crate::storage::entities::*;
use crate::types::DocumentStatus;

/// Minimal generic repository contract using associated types.
#[async_trait]
pub trait Repository: Send + Sync {
    type Key: Send + Sync + 'static;
    type Entity: Send + Sync + 'static;

    async fn get(&self, id: Self::Key) -> Result<Option<Self::Entity>>;
    async fn save(&self, entity: Self::Entity) -> Result<Self::Entity>;
    async fn delete(&self, id: Self::Key) -> Result<bool>;
}

#[async_trait]
pub trait DocumentsRepo: Repository<Key = DocumentId, Entity = Document> + Send + Sync {
    async fn set_status(&self, id: DocumentId, status: DocumentStatus) -> Result<bool>;
}

#[async_trait]
pub trait AssetsRepo: Repository<Key = AssetId, Entity = Asset> + Send + Sync {
    async fn list_by_document(&self, document_id: DocumentId) -> Result<Vec<Asset>>;
    async fn set_storage_key(&self, id: AssetId, storage_key: &str) -> Result<bool>;
}

#[async_trait]
pub trait BlocksRepo: Repository<Key = BlockId, Entity = Block> + Send + Sync {
    async fn list_by_asset(&self, asset_id: AssetId) -> Result<Vec<Block>>;
    async fn delete_by_asset(&self, asset_id: AssetId) -> Result<u64>;
}

#[async_trait]
pub trait ChunkSetsRepo: Repository<Key = ChunkSetId, Entity = ChunkSet> + Send + Sync {
    async fn create(
        &self,
        tenant_id: TenantId,
        document_id: DocumentId,
        profile_hash: &str,
    ) -> Result<ChunkSetId>;

    async fn mark_finalized(&self, id: ChunkSetId) -> Result<bool>;
    async fn list_by_document(&self, document_id: DocumentId) -> Result<Vec<ChunkSet>>;
}

#[async_trait]
pub trait ChunksRepo: Repository<Key = ChunkId, Entity = Chunk> + Send + Sync {
    async fn list_by_set(&self, chunk_set_id: ChunkSetId) -> Result<Vec<Chunk>>;
    async fn delete_by_set(&self, chunk_set_id: ChunkSetId) -> Result<u64>;
}

#[async_trait]
pub trait EmbeddingSetsRepo:
    Repository<Key = EmbeddingSetId, Entity = EmbeddingSet> + Send + Sync
{
    async fn create(
        &self,
        tenant_id: TenantId,
        chunk_set_id: ChunkSetId,
        model_kind: &str,
        model_version: &str,
        dim: i32,
        metric: &str,
    ) -> Result<EmbeddingSetId>;

    async fn mark_ready(&self, id: EmbeddingSetId) -> Result<bool>;
    async fn list_by_chunk_set(&self, chunk_set_id: ChunkSetId) -> Result<Vec<EmbeddingSet>>;
}

#[async_trait]
pub trait EmbeddingsRepo:
    Repository<Key = (ChunkId, EmbeddingSetId), Entity = EmbeddingRef> + Send + Sync
{
    async fn list_by_set(&self, embedding_set_id: EmbeddingSetId) -> Result<Vec<EmbeddingRef>>;
    async fn delete_by_set(&self, embedding_set_id: EmbeddingSetId) -> Result<u64>;
}

#[async_trait]
pub trait JobsRepo: Repository<Key = JobId, Entity = Job> + Send + Sync {
    async fn create(
        &self,
        tenant_id: TenantId,
        kind: JobKind,
        status: JobStatus,
        payload_json: Option<String>,
    ) -> Result<JobId>;

    async fn set_status(&self, id: JobId, status: JobStatus) -> Result<bool>;

    async fn list_by_kind_status(
        &self,
        kind: JobKind,
        status: JobStatus,
        limit: Option<usize>,
    ) -> Result<Vec<Job>>;
}

#[async_trait]
pub trait TenantsRepo: Repository<Key = TenantId, Entity = Tenant> + Send + Sync {
    async fn create(&self, name: &str, email: &str) -> Result<TenantId>;
    async fn get_by_name(&self, name: &str) -> Result<Option<Tenant>>;
    async fn get_by_email(&self, email: &str) -> Result<Option<Tenant>>;
    async fn list(&self, limit: Option<usize>) -> Result<Vec<Tenant>>;
}

#[async_trait]
pub trait DagRunsRepo:
    Repository<Key = crate::prelude::DagRunId, Entity = crate::workflow::DagRun> + Send + Sync
{
    async fn list_by_tenant(&self, tenant_id: TenantId) -> Result<Vec<crate::workflow::DagRun>>;
}

#[async_trait]
pub trait TasksRepo:
    Repository<Key = crate::prelude::TaskId, Entity = crate::workflow::Task> + Send + Sync
{
    async fn list_by_dag_run(
        &self,
        dag_run_id: crate::prelude::DagRunId,
    ) -> Result<Vec<crate::workflow::Task>>;
}

#[async_trait]
pub trait TaskRunsRepo:
    Repository<Key = crate::prelude::TaskRunId, Entity = crate::workflow::TaskRun> + Send + Sync
{
    async fn list_by_task(
        &self,
        task_id: crate::prelude::TaskId,
    ) -> Result<Vec<crate::workflow::TaskRun>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionResult {
    pub admitted: bool,
    pub reason: Option<String>,
}

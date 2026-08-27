//! Repository interfaces (DB-agnostic), dyn-friendly + associated types.
//!
//! Design goals:
//! - Use trait objects (`dyn`) for IO/pluggable layers (vtable).
//! - Keep associated types to preserve strong typing and make generic reuse easy.
//! - Keep contracts small; add convenience via extension traits later.
use contexide_core::{
    errors::Result,
    prelude::{AssetId, BlockId, ChunkId, ChunkSetId, DocumentId, EmbeddingSetId, JobId, TenantId},
    storage::{
        Asset, Block, Chunk, ChunkSet, Document, EmbeddingRef, EmbeddingSet, Job, JobKind,
        JobStatus, Tenant,
    },
    types::DocumentStatus,
};

/// Minimal generic repository contract using associated types.
///
/// Object-safe with `async_trait`, so consumers may hold `Arc<dyn Repository<...>>`
/// if they wish. We keep it tiny on purpose; domain repos extend it.
#[async_trait::async_trait]
pub trait Repository: Send + Sync {
    /// Logical primary key type.
    type Key: Send + Sync + 'static;
    /// Entity type returned by the repository.
    type Entity: Send + Sync + 'static;

    /// Fetch a single entity by key. Returns `Ok(None)` if not found.
    async fn get(&self, id: Self::Key) -> Result<Option<Self::Entity>>;

    // Create a new repository
    async fn save(&self, entity: Self::Entity) -> Result<Self::Entity>;

    // Delete entity by key
    async fn delete(&self, id: Self::Key) -> Result<bool>;
}

/// Domain repository for documents built on top of `Repository`.
#[async_trait::async_trait]
pub trait DocumentsRepo: Repository<Key = DocumentId, Entity = Document> + Send + Sync {
    /// Update the status of a document. Returns `Ok(false)` if the row was not found.
    async fn set_status(&self, id: DocumentId, status: DocumentStatus) -> Result<bool>;
}

/// Domain repo for assets. Extends the base contract and adds domain methods.
#[async_trait::async_trait]
pub trait AssetsRepo: Repository<Key = AssetId, Entity = Asset> + Send + Sync {
    /// List all assets for a document (ordered by insert time or id).
    async fn list_by_document(&self, document_id: DocumentId) -> Result<Vec<Asset>>;

    /// Update storage key (e.g., after uploading to blob storage).
    async fn set_storage_key(&self, id: AssetId, storage_key: &str) -> Result<bool>;
}

/// Domain repo for blocks. Extends the base contract and adds domain methods.
#[async_trait::async_trait]
pub trait BlocksRepo: Repository<Key = BlockId, Entity = Block> + Send + Sync {
    /// List all blocks for an asset (ordered by `order_no`, then `id` as tie-breaker).
    async fn list_by_asset(&self, asset_id: AssetId) -> Result<Vec<Block>>;

    /// Delete all blocks for an asset. Returns number of rows removed.
    async fn delete_by_asset(&self, asset_id: AssetId) -> Result<u64>;
}

/// Domain repo for chunk sets. Extends the base contract and adds domain methods.
#[async_trait::async_trait]
pub trait ChunkSetsRepo: Repository<Key = ChunkSetId, Entity = ChunkSet> + Send + Sync {
    /// Create a new chunk set and return its generated id.
    async fn create(
        &self,
        tenant_id: TenantId,
        document_id: DocumentId,
        profile_hash: &str,
    ) -> Result<ChunkSetId>;

    /// Mark the set as finalized. Returns `Ok(false)` if not found.
    async fn mark_finalized(&self, id: ChunkSetId) -> Result<bool>;

    /// List all chunk sets for a document.
    async fn list_by_document(&self, document_id: DocumentId) -> Result<Vec<ChunkSet>>;
}

/// Domain repo for chunks. Extends the base contract and adds domain methods.
#[async_trait::async_trait]
pub trait ChunksRepo: Repository<Key = ChunkId, Entity = Chunk> + Send + Sync {
    /// List all chunks for a set, ordered by (`order_no`, then `id`).
    async fn list_by_set(&self, chunk_set_id: ChunkSetId) -> Result<Vec<Chunk>>;

    /// Delete all chunks for a set. Returns number of rows removed.
    async fn delete_by_set(&self, chunk_set_id: ChunkSetId) -> Result<u64>;
}

/// Domain repo for embedding sets. Extends the base contract and adds domain methods.
#[async_trait::async_trait]
pub trait EmbeddingSetsRepo:
    Repository<Key = EmbeddingSetId, Entity = EmbeddingSet> + Send + Sync
{
    /// Create a new embedding set and return its generated id.
    async fn create(
        &self,
        tenant_id: TenantId,
        chunk_set_id: ChunkSetId,
        model_kind: &str,
        model_version: &str,
        dim: i32,
        metric: &str,
    ) -> Result<EmbeddingSetId>;

    /// Mark the set as ready (all vectors computed and indexed). Returns `Ok(false)` if not found.
    async fn mark_ready(&self, id: EmbeddingSetId) -> Result<bool>;

    /// List all embedding sets for a given chunk set.
    async fn list_by_chunk_set(&self, chunk_set_id: ChunkSetId) -> Result<Vec<EmbeddingSet>>;
}

/// Domain repo for embeddings mapping (chunk → vector_id in a set).
///
/// Base `Repository` key is a composite `(ChunkId, EmbeddingSetId)`.
#[async_trait::async_trait]
pub trait EmbeddingsRepo:
    Repository<Key = (ChunkId, EmbeddingSetId), Entity = EmbeddingRef> + Send + Sync
{
    /// List all mappings for a given embedding set.
    async fn list_by_set(&self, embedding_set_id: EmbeddingSetId) -> Result<Vec<EmbeddingRef>>;

    /// Delete all mappings for a given embedding set. Returns number of rows removed.
    async fn delete_by_set(&self, embedding_set_id: EmbeddingSetId) -> Result<u64>;
}

/// Domain repo for pipeline jobs.
#[async_trait::async_trait]
pub trait JobsRepo: Repository<Key = JobId, Entity = Job> + Send + Sync {
    /// Create a new job and return its id (`status` is usually `Pending`).
    async fn create(
        &self,
        tenant_id: TenantId,
        kind: JobKind,
        status: JobStatus,
        payload_json: Option<String>,
    ) -> Result<JobId>;

    /// Update the status of a job. Returns `Ok(false)` if not found.
    async fn set_status(&self, id: JobId, status: JobStatus) -> Result<bool>;

    /// List jobs by kind & status (optionally limited).
    async fn list_by_kind_status(
        &self,
        kind: JobKind,
        status: JobStatus,
        limit: Option<usize>,
    ) -> Result<Vec<Job>>;
}

/// Domain repo for tenants.
#[async_trait::async_trait]
pub trait TenantsRepo: Repository<Key = TenantId, Entity = Tenant> + Send + Sync {
    /// Idempotent create by unique `(name, email)`.
    ///
    /// Semantics:
    /// - If a tenant with this `name` already exists:
    ///   - if its `email` matches, return existing id;
    ///   - otherwise return an error (conflict).
    /// - If a tenant with this `email` already exists:
    ///   - if its `name` matches, return existing id;
    ///   - otherwise return an error (conflict).
    /// - Otherwise create a new tenant and return its id.
    async fn create(&self, name: &str, email: &str) -> Result<TenantId>;

    /// Fetch a tenant by its unique name.
    async fn get_by_name(&self, name: &str) -> Result<Option<Tenant>>;

    /// Fetch a tenant by its unique email.
    async fn get_by_email(&self, email: &str) -> Result<Option<Tenant>>;

    /// List tenants (optionally limit).
    async fn list(&self, limit: Option<usize>) -> Result<Vec<Tenant>>;
}

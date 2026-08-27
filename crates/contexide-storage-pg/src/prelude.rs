// crates/contexide-storage-pg/src/prelude.rs
//! Public prelude for `contexide-storage-pg`.
//!
//! Import this module to get the most commonly used types and traits without
//! long paths, e.g.:
//! ```rust
//! use contexide_storage_pg::prelude::*;
//! ```
//!
//! This prelude re-exports:
//! - Core IDs, enums and Result from `contexide-core`
//! - DTOs from storage domain modules (tenants, documents, assets, ...)
//! - Repository traits (generic `Repository` + domain repos)
//! - Convenience functions for Postgres pool & migrations

// === Core (IDs, enums, Result) =================================================

pub use contexide_core::prelude::{
    AssetId, BlockId, BlockModality, ChunkId, ChunkSetId, ContentAddress, DocumentId,
    DocumentStatus, EmbeddingSetId, Error, JobId, Result, TenantId,
};

// === Domain DTOs ===============================================================

pub use contexide_core::storage::{
    Asset, Block, Chunk, ChunkSet, Document, EmbeddingRef, EmbeddingSet, Job, JobKind, JobStatus,
    Tenant,
};

// === Repository traits =========================================================

pub use crate::traits::{
    AssetsRepo, BlocksRepo, ChunkSetsRepo, ChunksRepo, DocumentsRepo, EmbeddingSetsRepo,
    EmbeddingsRepo, JobsRepo, Repository, TenantsRepo,
};

// === Convenience exports (pool & migrations) ==================================

// Pool helpers (explicit, boring API).
pub use crate::pool::{new_pool, ping};

// Migration runner (runtime-loaded reversible migrations).
pub use crate::migrator::{migration_names, run_all as run_migrations};

// === Optional: handy re-exports of concrete repos =============================
//
// Keeping these under nested modules to avoid polluting the flat namespace.
// Import as `use contexide_storage_pg::prelude::pg::PgDocumentsRepo;` or
// `use contexide_storage_pg::prelude::mem::MemDocumentsRepo;`.

pub mod pg {
    //! Postgres-backed repositories (require a `sqlx::Pool<Postgres>`).
    pub use crate::assets::pg::PgAssetsRepo;
    pub use crate::blocks::pg::PgBlocksRepo;
    pub use crate::chunk_sets::pg::PgChunkSetsRepo;
    pub use crate::chunks::pg::PgChunksRepo;
    pub use crate::documents::pg::PgDocumentsRepo;
    pub use crate::embedding_refs::pg::PgEmbeddingsRefRepo;
    pub use crate::embedding_sets::pg::PgEmbeddingSetsRepo;
    pub use crate::jobs::pg::PgJobsRepo;
    pub use crate::tenants::pg::PgTenantsRepo;
}

pub mod mem {
    //! In-memory repositories (useful for unit tests and in-process demos).
    pub use crate::assets::mem::MemAssetsRepo;
    pub use crate::blocks::mem::MemBlocksRepo;
    pub use crate::chunk_sets::mem::MemChunkSetsRepo;
    pub use crate::chunks::mem::MemChunksRepo;
    pub use crate::documents::mem::MemDocumentsRepo;
    pub use crate::embedding_refs::mem::MemEmbeddingsRepo;
    pub use crate::embedding_sets::mem::MemEmbeddingSetsRepo;
    pub use crate::jobs::mem::MemJobsRepo;
    pub use crate::tenants::mem::MemTenantsRepo;
}

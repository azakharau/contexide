//! In-memory implementation of `ChunkSetsRepo` + base `Repository`.
//!
//! Simple thread-safe store backed by `Mutex<HashMap<...>>` and a secondary
//! index by `document_id` to support `list_by_document`. Suitable for unit
//! tests and local development.

use std::collections::HashMap;
use std::sync::Mutex;

use contexide_core::errors::Result;
use contexide_core::prelude::{ChunkSetId, DocumentId, TenantId};

use crate::chunk_sets::ChunkSet;
use crate::traits::{ChunkSetsRepo, Repository};

/// In-memory chunk-sets repository.
#[derive(Default)]
pub struct MemChunkSetsRepo {
    // Primary storage by ChunkSetId.
    by_id: Mutex<HashMap<ChunkSetId, ChunkSet>>,
    // Secondary index: DocumentId -> Vec<ChunkSetId>.
    by_doc: Mutex<HashMap<DocumentId, Vec<ChunkSetId>>>,
}

impl MemChunkSetsRepo {
    /// Construct an empty in-memory repo.
    pub fn new() -> Self {
        Self::default()
    }

    fn index_add(&self, doc: DocumentId, id: ChunkSetId) {
        let mut m = self.by_doc.lock().expect("poisoned");
        m.entry(doc).or_default().push(id);
    }

    fn index_remove(&self, doc: DocumentId, id: ChunkSetId) {
        let mut m = self.by_doc.lock().expect("poisoned");
        if let Some(v) = m.get_mut(&doc) {
            if let Some(pos) = v.iter().position(|x| *x == id) {
                v.swap_remove(pos);
            }
            if v.is_empty() {
                m.remove(&doc);
            }
        }
    }
}

#[async_trait::async_trait]
impl Repository for MemChunkSetsRepo {
    type Key = ChunkSetId;
    type Entity = ChunkSet;

    /// Fetch by id (clone-on-read).
    async fn get(&self, id: ChunkSetId) -> Result<Option<ChunkSet>> {
        let m = self.by_id.lock().expect("poisoned");
        Ok(m.get(&id).cloned())
    }

    /// Save (create or update) and return the stored entity.
    ///
    /// Semantics:
    /// - Insert if missing, update if exists.
    /// - If `document_id` changed on update, fix the secondary index.
    async fn save(&self, entity: ChunkSet) -> Result<ChunkSet> {
        let mut by_id = self.by_id.lock().expect("poisoned");

        match by_id.insert(entity.id, entity.clone()) {
            None => {
                // New record: add to index.
                drop(by_id);
                self.index_add(entity.document_id, entity.id);
            }
            Some(old) => {
                if old.document_id != entity.document_id {
                    drop(by_id);
                    self.index_remove(old.document_id, entity.id);
                    self.index_add(entity.document_id, entity.id);
                }
            }
        }

        Ok(entity)
    }

    /// Delete by id; returns whether a row was removed.
    async fn delete(&self, id: ChunkSetId) -> Result<bool> {
        let mut by_id = self.by_id.lock().expect("poisoned");
        if let Some(old) = by_id.remove(&id) {
            drop(by_id);
            self.index_remove(old.document_id, id);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl ChunkSetsRepo for MemChunkSetsRepo {
    /// Create a new chunk set with generated id.
    async fn create(
        &self,
        tenant_id: TenantId,
        document_id: DocumentId,
        profile_hash: &str,
    ) -> Result<ChunkSetId> {
        let id = ChunkSetId::new();
        let cs = ChunkSet {
            id,
            tenant_id,
            document_id,
            profile_hash: profile_hash.to_string(),
            finalized: false,
        };
        // Reuse save() to update indexes.
        self.save(cs).await?;
        Ok(id)
    }

    /// Mark as finalized (idempotent: returns true only if existed).
    async fn mark_finalized(&self, id: ChunkSetId) -> Result<bool> {
        let mut m = self.by_id.lock().expect("poisoned");
        if let Some(cs) = m.get_mut(&id) {
            cs.finalized = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all sets for a document (best-effort order by id).
    async fn list_by_document(&self, document_id: DocumentId) -> Result<Vec<ChunkSet>> {
        let ids: Vec<ChunkSetId> = {
            let idx = self.by_doc.lock().expect("poisoned");
            idx.get(&document_id).cloned().unwrap_or_default()
        };

        let store = self.by_id.lock().expect("poisoned");
        let mut out: Vec<ChunkSet> = ids
            .into_iter()
            .filter_map(|id| store.get(&id).cloned())
            .collect();

        // Deterministic order (UUIDv7 roughly time-ordered; we still sort).
        out.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::{DocumentId, TenantId};

    #[tokio::test]
    async fn create_get_finalize_list_delete() {
        let repo = MemChunkSetsRepo::new();
        let t = TenantId::new();
        let d = DocumentId::new();

        // create
        let id = repo.create(t, d, "phash").await.unwrap();

        // get
        let cs = repo.get(id).await.unwrap().unwrap();
        assert_eq!(cs.document_id, d);
        assert_eq!(cs.profile_hash, "phash");
        assert!(!cs.finalized);

        // mark finalized
        assert!(repo.mark_finalized(id).await.unwrap());
        let cs2 = repo.get(id).await.unwrap().unwrap();
        assert!(cs2.finalized);

        // list_by_document
        let list = repo.list_by_document(d).await.unwrap();
        assert_eq!(list.len(), 1);

        // delete
        assert!(repo.delete(id).await.unwrap());
        assert!(repo.get(id).await.unwrap().is_none());
        let list2 = repo.list_by_document(d).await.unwrap();
        assert!(list2.is_empty());
    }
}

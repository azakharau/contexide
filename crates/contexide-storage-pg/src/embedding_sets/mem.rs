//! In-memory implementation of `EmbeddingSetsRepo` + base `Repository`.
//!
//! Thread-safe store using `Mutex<HashMap<...>>` and a secondary index by
//! `chunk_set_id` for `list_by_chunk_set`. Suitable for unit tests and local runs.

use std::collections::HashMap;
use std::sync::Mutex;

use contexide_core::errors::Result;
use contexide_core::prelude::{ChunkSetId, EmbeddingSetId, TenantId};

use crate::{
    embedding_sets::EmbeddingSet,
    traits::{EmbeddingSetsRepo, Repository},
};

/// In-memory embedding-sets repository.
#[derive(Default)]
pub struct MemEmbeddingSetsRepo {
    // Primary storage by EmbeddingSetId.
    by_id: Mutex<HashMap<EmbeddingSetId, EmbeddingSet>>,
    // Secondary index: ChunkSetId -> Vec<EmbeddingSetId>.
    by_chunk_set: Mutex<HashMap<ChunkSetId, Vec<EmbeddingSetId>>>,
}

impl MemEmbeddingSetsRepo {
    /// Construct an empty in-memory repo.
    pub fn new() -> Self {
        Self::default()
    }

    fn index_add(&self, set: ChunkSetId, id: EmbeddingSetId) {
        let mut m = self.by_chunk_set.lock().expect("poisoned");
        m.entry(set).or_default().push(id);
    }

    fn index_remove(&self, set: ChunkSetId, id: EmbeddingSetId) {
        let mut m = self.by_chunk_set.lock().expect("poisoned");
        if let Some(vec) = m.get_mut(&set) {
            if let Some(pos) = vec.iter().position(|x| *x == id) {
                vec.swap_remove(pos);
            }
            if vec.is_empty() {
                m.remove(&set);
            }
        }
    }
}

#[async_trait::async_trait]
impl Repository for MemEmbeddingSetsRepo {
    type Key = EmbeddingSetId;
    type Entity = EmbeddingSet;

    /// Fetch by id (clone-on-read).
    async fn get(&self, id: EmbeddingSetId) -> Result<Option<EmbeddingSet>> {
        let m = self.by_id.lock().expect("poisoned");
        Ok(m.get(&id).cloned())
    }

    /// Save (create or update) and return the stored entity.
    ///
    /// Semantics:
    /// - Insert if missing, update if exists.
    /// - If `chunk_set_id` changes on update, fix the secondary index accordingly.
    async fn save(&self, entity: EmbeddingSet) -> Result<EmbeddingSet> {
        let mut by_id = self.by_id.lock().expect("poisoned");

        match by_id.insert(entity.id, entity.clone()) {
            None => {
                // New record: add to index.
                drop(by_id);
                self.index_add(entity.chunk_set_id, entity.id);
            }
            Some(old) => {
                if old.chunk_set_id != entity.chunk_set_id {
                    drop(by_id);
                    self.index_remove(old.chunk_set_id, entity.id);
                    self.index_add(entity.chunk_set_id, entity.id);
                }
            }
        }

        Ok(entity)
    }

    /// Delete by id; returns true if something was removed.
    async fn delete(&self, id: EmbeddingSetId) -> Result<bool> {
        let mut by_id = self.by_id.lock().expect("poisoned");
        if let Some(old) = by_id.remove(&id) {
            drop(by_id);
            self.index_remove(old.chunk_set_id, id);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl EmbeddingSetsRepo for MemEmbeddingSetsRepo {
    /// Create a new embedding set with generated id and `ready=false`.
    async fn create(
        &self,
        tenant_id: TenantId,
        chunk_set_id: ChunkSetId,
        model_kind: &str,
        model_version: &str,
        dim: i32,
        metric: &str,
    ) -> Result<EmbeddingSetId> {
        let id = EmbeddingSetId::new();
        let es = EmbeddingSet {
            id,
            tenant_id,
            chunk_set_id,
            model_kind: model_kind.to_string(),
            model_version: model_version.to_string(),
            dim,
            metric: metric.to_string(),
            ready: false,
        };
        self.save(es).await?;
        Ok(id)
    }

    /// Mark as ready (idempotent: true only if existed).
    async fn mark_ready(&self, id: EmbeddingSetId) -> Result<bool> {
        let mut m = self.by_id.lock().expect("poisoned");
        if let Some(es) = m.get_mut(&id) {
            es.ready = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List embedding sets for a chunk set (deterministic order by id).
    async fn list_by_chunk_set(&self, chunk_set_id: ChunkSetId) -> Result<Vec<EmbeddingSet>> {
        let ids: Vec<EmbeddingSetId> = {
            let idx = self.by_chunk_set.lock().expect("poisoned");
            idx.get(&chunk_set_id).cloned().unwrap_or_default()
        };

        let store = self.by_id.lock().expect("poisoned");
        let mut out: Vec<EmbeddingSet> = ids
            .into_iter()
            .filter_map(|id| store.get(&id).cloned())
            .collect();

        out.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::{ChunkSetId, TenantId};

    #[tokio::test]
    async fn create_get_ready_list_delete() {
        let repo = MemEmbeddingSetsRepo::new();
        let t = TenantId::new();
        let cs = ChunkSetId::new();

        // create
        let id = repo
            .create(t, cs, "e5", "base", 1024, "cosine")
            .await
            .unwrap();

        // get
        let es = repo.get(id).await.unwrap().unwrap();
        assert_eq!(es.chunk_set_id, cs);
        assert_eq!(es.model_kind, "e5");
        assert_eq!(es.model_version, "base");
        assert_eq!(es.dim, 1024);
        assert_eq!(es.metric, "cosine");
        assert!(!es.ready);

        // mark ready
        assert!(repo.mark_ready(id).await.unwrap());
        let es2 = repo.get(id).await.unwrap().unwrap();
        assert!(es2.ready);

        // list_by_chunk_set
        let list = repo.list_by_chunk_set(cs).await.unwrap();
        assert_eq!(list.len(), 1);

        // delete
        assert!(repo.delete(id).await.unwrap());
        assert!(repo.get(id).await.unwrap().is_none());
        let list2 = repo.list_by_chunk_set(cs).await.unwrap();
        assert!(list2.is_empty());
    }
}

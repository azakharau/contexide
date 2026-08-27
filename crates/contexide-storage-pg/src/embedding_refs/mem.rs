//! In-memory implementation of `EmbeddingsRepo` + base `Repository`.
//!
//! - Primary key: composite `(ChunkId, EmbeddingSetId)`.
//! - Secondary index by `embedding_set_id` to support fast listing/cleanup.
//! - Suitable for unit tests and local development.

use std::collections::HashMap;
use std::sync::Mutex;

use contexide_core::errors::Result;
use contexide_core::prelude::{ChunkId, EmbeddingSetId};

use crate::{
    embedding_refs::EmbeddingRef,
    traits::{EmbeddingsRepo, Repository},
};

/// In-memory embeddings repository.
#[derive(Default)]
pub struct MemEmbeddingsRepo {
    // Primary storage keyed by (chunk_id, embedding_set_id).
    by_key: Mutex<HashMap<(ChunkId, EmbeddingSetId), EmbeddingRef>>,
    // Secondary index: embedding_set_id -> Vec<(ChunkId, EmbeddingSetId)>.
    by_set: Mutex<HashMap<EmbeddingSetId, Vec<(ChunkId, EmbeddingSetId)>>>,
}

impl MemEmbeddingsRepo {
    pub fn new() -> Self {
        Self::default()
    }

    fn index_add(&self, set: EmbeddingSetId, key: (ChunkId, EmbeddingSetId)) {
        let mut m = self.by_set.lock().expect("poisoned");
        m.entry(set).or_default().push(key);
    }

    fn index_remove(&self, set: EmbeddingSetId, key: (ChunkId, EmbeddingSetId)) {
        let mut m = self.by_set.lock().expect("poisoned");
        if let Some(vec) = m.get_mut(&set) {
            if let Some(pos) = vec.iter().position(|x| *x == key) {
                vec.swap_remove(pos);
            }
            if vec.is_empty() {
                m.remove(&set);
            }
        }
    }
}

#[async_trait::async_trait]
impl Repository for MemEmbeddingsRepo {
    type Key = (ChunkId, EmbeddingSetId);
    type Entity = EmbeddingRef;

    /// Fetch by composite key (clone-on-read).
    async fn get(&self, id: Self::Key) -> Result<Option<EmbeddingRef>> {
        let m = self.by_key.lock().expect("poisoned");
        Ok(m.get(&id).cloned())
    }

    /// Upsert mapping and maintain secondary index.
    ///
    /// Notes:
    /// - The entity's composite key is `(entity.chunk_id, entity.embedding_set_id)`.
    /// - If the key is new, we add it to the set index.
    /// - If an entry exists, we overwrite `vector_id`/`tenant_id` as provided.
    async fn save(&self, entity: EmbeddingRef) -> Result<EmbeddingRef> {
        let key = (entity.chunk_id, entity.embedding_set_id);
        let mut by_key = self.by_key.lock().expect("poisoned");

        match by_key.insert(key, entity.clone()) {
            None => {
                drop(by_key);
                self.index_add(entity.embedding_set_id, key);
            }
            Some(_old) => {
                // Key unchanged; no index fix necessary (composite key stable).
            }
        }

        Ok(entity)
    }

    /// Delete by composite key.
    async fn delete(&self, id: Self::Key) -> Result<bool> {
        let mut by_key = self.by_key.lock().expect("poisoned");
        if let Some(old) = by_key.remove(&id) {
            drop(by_key);
            self.index_remove(old.embedding_set_id, id);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl EmbeddingsRepo for MemEmbeddingsRepo {
    /// List mappings for a set (deterministic order by chunk_id then set_id).
    async fn list_by_set(&self, embedding_set_id: EmbeddingSetId) -> Result<Vec<EmbeddingRef>> {
        let keys: Vec<(ChunkId, EmbeddingSetId)> = {
            let idx = self.by_set.lock().expect("poisoned");
            idx.get(&embedding_set_id).cloned().unwrap_or_default()
        };

        let store = self.by_key.lock().expect("poisoned");
        let mut out: Vec<EmbeddingRef> = keys
            .into_iter()
            .filter_map(|k| store.get(&k).cloned())
            .collect();

        out.sort_by(|a, b| {
            let ak = (a.chunk_id.0, a.embedding_set_id.0);
            let bk = (b.chunk_id.0, b.embedding_set_id.0);
            ak.cmp(&bk)
        });

        Ok(out)
    }

    /// Delete all mappings for a set. Returns removed count.
    async fn delete_by_set(&self, embedding_set_id: EmbeddingSetId) -> Result<u64> {
        // Take the bucket of keys.
        let keys: Vec<(ChunkId, EmbeddingSetId)> = {
            let mut idx = self.by_set.lock().expect("poisoned");
            idx.remove(&embedding_set_id).unwrap_or_default()
        };

        if keys.is_empty() {
            return Ok(0);
        }

        let mut store = self.by_key.lock().expect("poisoned");
        let mut removed: u64 = 0;
        for k in keys {
            if store.remove(&k).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::{ChunkId, EmbeddingSetId, TenantId};

    fn mk_ref(chunk: ChunkId, set: EmbeddingSetId, vector_id: &str) -> EmbeddingRef {
        EmbeddingRef {
            chunk_id: chunk,
            embedding_set_id: set,
            tenant_id: TenantId::new(),
            vector_id: vector_id.to_string(),
        }
    }

    #[tokio::test]
    async fn save_get_list_delete() {
        let repo = MemEmbeddingsRepo::new();
        let s1 = EmbeddingSetId::new();
        let s2 = EmbeddingSetId::new();
        let c1 = ChunkId::new();
        let c2 = ChunkId::new();

        // upsert two mappings in s1
        let _e1 = repo.save(mk_ref(c1, s1, "v-1")).await.unwrap();
        let _e2 = repo.save(mk_ref(c2, s1, "v-2")).await.unwrap();

        // get by composite key
        assert!(repo.get((c1, s1)).await.unwrap().is_some());

        // list_by_set(s1)
        let list1 = repo.list_by_set(s1).await.unwrap();
        assert_eq!(list1.len(), 2);

        // move c2 to s2 (represented as delete+insert at call sites; here simulate directly)
        repo.delete((c2, s1)).await.unwrap();
        let _ = repo.save(mk_ref(c2, s2, "v-2b")).await.unwrap();

        let list1b = repo.list_by_set(s1).await.unwrap();
        assert_eq!(list1b.len(), 1);
        let list2 = repo.list_by_set(s2).await.unwrap();
        assert_eq!(list2.len(), 1);

        // cleanup
        let removed = repo.delete_by_set(s1).await.unwrap();
        assert_eq!(removed, 1);
        assert!(repo.get((c1, s1)).await.unwrap().is_none());
    }
}

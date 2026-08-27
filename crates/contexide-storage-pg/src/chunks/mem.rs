//! In-memory implementation of `ChunksRepo` + base `Repository`.
//!
//! Simple thread-safe store backed by `Mutex<HashMap<...>>` and a secondary
//! index by `chunk_set_id` to support `list_by_set` and `delete_by_set`.
//! Suitable for unit tests and local development.

use std::collections::HashMap;
use std::sync::Mutex;

use contexide_core::errors::Result;
use contexide_core::prelude::{ChunkId, ChunkSetId};

use crate::chunks::Chunk;
use crate::traits::{ChunksRepo, Repository};

/// In-memory chunks repository (predictable semantics; not optimized).
#[derive(Default)]
pub struct MemChunksRepo {
    // Primary storage by ChunkId.
    by_id: Mutex<HashMap<ChunkId, Chunk>>,
    // Secondary index: ChunkSetId -> Vec<ChunkId>.
    by_set: Mutex<HashMap<ChunkSetId, Vec<ChunkId>>>,
}

impl MemChunksRepo {
    /// Construct an empty in-memory repo.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add (set -> chunk) relation to the secondary index.
    fn index_add(&self, set_id: ChunkSetId, id: ChunkId) {
        let mut m = self.by_set.lock().expect("poisoned");
        m.entry(set_id).or_default().push(id);
    }

    /// Remove a chunk from its set bucket in the index.
    fn index_remove(&self, set_id: ChunkSetId, id: ChunkId) {
        let mut m = self.by_set.lock().expect("poisoned");
        if let Some(vec) = m.get_mut(&set_id) {
            if let Some(pos) = vec.iter().position(|x| *x == id) {
                vec.swap_remove(pos);
            }
            if vec.is_empty() {
                m.remove(&set_id);
            }
        }
    }
}

#[async_trait::async_trait]
impl Repository for MemChunksRepo {
    type Key = ChunkId;
    type Entity = Chunk;

    /// Fetch by id (clone-on-read).
    async fn get(&self, id: ChunkId) -> Result<Option<Chunk>> {
        let m = self.by_id.lock().expect("poisoned");
        Ok(m.get(&id).cloned())
    }

    /// Save (create or update) a chunk and return the stored entity.
    ///
    /// Semantics:
    /// - Insert if missing, update if exists.
    /// - If `chunk_set_id` changes on update, fix the secondary index accordingly.
    async fn save(&self, entity: Chunk) -> Result<Chunk> {
        let mut by_id = self.by_id.lock().expect("poisoned");

        match by_id.insert(entity.id, entity.clone()) {
            None => {
                // New record: add index entry.
                drop(by_id);
                self.index_add(entity.chunk_set_id, entity.id);
            }
            Some(old) => {
                // Updated record: if moved to another set, fix index.
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
    async fn delete(&self, id: ChunkId) -> Result<bool> {
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
impl ChunksRepo for MemChunksRepo {
    /// List chunks for a set ordered by (`order_no`, then `id`).
    async fn list_by_set(&self, chunk_set_id: ChunkSetId) -> Result<Vec<Chunk>> {
        // Snapshot IDs first to minimize lock contention.
        let ids: Vec<ChunkId> = {
            let idx = self.by_set.lock().expect("poisoned");
            idx.get(&chunk_set_id).cloned().unwrap_or_default()
        };

        let store = self.by_id.lock().expect("poisoned");
        let mut out: Vec<Chunk> = ids
            .into_iter()
            .filter_map(|id| store.get(&id).cloned())
            .collect();

        out.sort_by(|a, b| match a.order_no.cmp(&b.order_no) {
            core::cmp::Ordering::Equal => a.id.0.cmp(&b.id.0),
            other => other,
        });

        Ok(out)
    }

    /// Delete all chunks of a given set. Returns count of removed chunks.
    async fn delete_by_set(&self, chunk_set_id: ChunkSetId) -> Result<u64> {
        // Take the bucket of IDs (if any).
        let ids: Vec<ChunkId> = {
            let mut idx = self.by_set.lock().expect("poisoned");
            idx.remove(&chunk_set_id).unwrap_or_default()
        };

        if ids.is_empty() {
            return Ok(0);
        }

        let mut store = self.by_id.lock().expect("poisoned");
        let mut removed: u64 = 0;
        for id in ids {
            if store.remove(&id).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::{ChunkId, ChunkSetId, TenantId};

    fn mk_chunk(set: ChunkSetId, order: i32) -> Chunk {
        Chunk {
            id: ChunkId::new(),
            tenant_id: TenantId::new(),
            chunk_set_id: set,
            order_no: order,
            byte_start: 0,
            byte_end: 10,
            text: format!("chunk-{}", order),
            meta_json: None,
        }
    }

    #[tokio::test]
    async fn save_get_list_delete() {
        let repo = MemChunksRepo::new();
        let s1 = ChunkSetId::new();
        let s2 = ChunkSetId::new();

        // create three chunks under s1
        let c1 = repo.save(mk_chunk(s1, 2)).await.unwrap();
        let c2 = repo.save(mk_chunk(s1, 1)).await.unwrap();
        let c3 = repo.save(mk_chunk(s1, 3)).await.unwrap();

        // list_by_set returns ordered by order_no, then id
        let list = repo.list_by_set(s1).await.unwrap();
        assert_eq!(
            list.iter().map(|c| c.order_no).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        // move c3 to s2 (update)
        let mut c3m = c3.clone();
        c3m.chunk_set_id = s2;
        repo.save(c3m).await.unwrap();

        let list_s1 = repo.list_by_set(s1).await.unwrap();
        assert_eq!(list_s1.len(), 2);
        let list_s2 = repo.list_by_set(s2).await.unwrap();
        assert_eq!(list_s2.len(), 1);

        // delete_by_set(s1) removes remaining two
        let removed = repo.delete_by_set(s1).await.unwrap();
        assert_eq!(removed, 2);

        // single delete
        assert!(!repo.delete(c2.id).await.unwrap()); // already removed
        assert!(repo.get(c1.id).await.unwrap().is_none());
    }
}

//! In-memory implementation of `BlocksRepo` + base `Repository`.
//!
//! Simple thread-safe store backed by `Mutex<HashMap<...>>` and a secondary
//! index by `asset_id` to support `list_by_asset` and `delete_by_asset`.
//! Suitable for unit tests and local development.

use std::collections::HashMap;
use std::sync::Mutex;

use contexide_core::errors::Result;
use contexide_core::{AssetId, BlockId};

use crate::blocks::Block;
use crate::traits::{BlocksRepo, Repository};

/// In-memory blocks repository (not optimized; predictable semantics).
#[derive(Default)]
pub struct MemBlocksRepo {
    // Primary storage by BlockId.
    by_id: Mutex<HashMap<BlockId, Block>>,
    // Secondary index: AssetId -> Vec<BlockId>.
    by_asset: Mutex<HashMap<AssetId, Vec<BlockId>>>,
}

impl MemBlocksRepo {
    /// Construct an empty in-memory repo.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add (asset -> block) relation to the secondary index.
    fn index_add(&self, asset: AssetId, id: BlockId) {
        let mut m = self.by_asset.lock().expect("poisoned");
        m.entry(asset).or_default().push(id);
    }

    /// Remove a block from its asset bucket in the index.
    fn index_remove(&self, asset: AssetId, id: BlockId) {
        let mut m = self.by_asset.lock().expect("poisoned");
        if let Some(vec) = m.get_mut(&asset) {
            if let Some(pos) = vec.iter().position(|x| *x == id) {
                vec.swap_remove(pos);
            }
            if vec.is_empty() {
                m.remove(&asset);
            }
        }
    }
}

#[async_trait::async_trait]
impl Repository for MemBlocksRepo {
    type Key = BlockId;
    type Entity = Block;

    /// Fetch by id (clone-on-read).
    async fn get(&self, id: BlockId) -> Result<Option<Block>> {
        let m = self.by_id.lock().expect("poisoned");
        Ok(m.get(&id).cloned())
    }

    /// Save (create or update) a block and return the stored entity.
    ///
    /// Semantics:
    /// - Insert if missing, update if exists.
    /// - If `asset_id` changes on update, fix the secondary index accordingly.
    async fn save(&self, entity: Block) -> Result<Block> {
        let mut by_id = self.by_id.lock().expect("poisoned");

        match by_id.insert(entity.id, entity.clone()) {
            None => {
                // New record: add index entry.
                drop(by_id);
                self.index_add(entity.asset_id, entity.id);
            }
            Some(old) => {
                // Updated record: if moved to another asset, fix index.
                if old.asset_id != entity.asset_id {
                    drop(by_id);
                    self.index_remove(old.asset_id, entity.id);
                    self.index_add(entity.asset_id, entity.id);
                }
            }
        }

        Ok(entity)
    }

    /// Delete by id; returns true if something was removed.
    async fn delete(&self, id: BlockId) -> Result<bool> {
        let mut by_id = self.by_id.lock().expect("poisoned");
        if let Some(old) = by_id.remove(&id) {
            drop(by_id);
            self.index_remove(old.asset_id, id);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl BlocksRepo for MemBlocksRepo {
    /// List blocks for an asset ordered by (`order_no`, then `id`).
    async fn list_by_asset(&self, asset_id: AssetId) -> Result<Vec<Block>> {
        // Snapshot IDs first to minimize lock contention.
        let ids: Vec<BlockId> = {
            let idx = self.by_asset.lock().expect("poisoned");
            idx.get(&asset_id).cloned().unwrap_or_default()
        };

        let store = self.by_id.lock().expect("poisoned");
        let mut out: Vec<Block> = ids
            .into_iter()
            .filter_map(|id| store.get(&id).cloned())
            .collect();

        out.sort_by(|a, b| match a.order_no.cmp(&b.order_no) {
            core::cmp::Ordering::Equal => a.id.0.cmp(&b.id.0),
            other => other,
        });

        Ok(out)
    }

    /// Delete all blocks of a given asset. Returns count of removed blocks.
    async fn delete_by_asset(&self, asset_id: AssetId) -> Result<u64> {
        // Take the bucket of IDs (if any).
        let ids: Vec<BlockId> = {
            let mut idx = self.by_asset.lock().expect("poisoned");
            idx.remove(&asset_id).unwrap_or_default()
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
    use contexide_core::prelude::{AssetId, BlockId, TenantId};
    use contexide_core::types::BlockModality;

    fn mk_block(asset: AssetId, order: i32) -> Block {
        Block {
            id: BlockId::new(),
            tenant_id: TenantId::new(),
            asset_id: asset,
            modality: BlockModality::Text,
            order_no: order,
            text: Some(format!("block-{}", order)),
            meta_json: None,
        }
    }

    #[tokio::test]
    async fn save_get_list_delete() {
        let repo = MemBlocksRepo::new();
        let a1 = AssetId::new();
        let a2 = AssetId::new();

        // create three blocks under a1
        let b1 = repo.save(mk_block(a1, 2)).await.unwrap();
        let b2 = repo.save(mk_block(a1, 1)).await.unwrap();
        let b3 = repo.save(mk_block(a1, 3)).await.unwrap();

        // list_by_asset returns ordered by order_no, then id
        let list = repo.list_by_asset(a1).await.unwrap();
        assert_eq!(
            list.iter().map(|b| b.order_no).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        // move b3 to a2 (update)
        let mut b3m = b3.clone();
        b3m.asset_id = a2;
        repo.save(b3m).await.unwrap();

        let list_a1 = repo.list_by_asset(a1).await.unwrap();
        assert_eq!(list_a1.len(), 2);
        let list_a2 = repo.list_by_asset(a2).await.unwrap();
        assert_eq!(list_a2.len(), 1);

        // delete_by_asset(a1) removes remaining two
        let removed = repo.delete_by_asset(a1).await.unwrap();
        assert_eq!(removed, 2);

        // single delete
        assert!(!repo.delete(b2.id).await.unwrap()); // already removed
        assert!(repo.get(b1.id).await.unwrap().is_none());
    }
}

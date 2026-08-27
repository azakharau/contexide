// crates/contexide-repo/src/assets/mem.rs
//! In-memory implementation of `AssetsRepo` + base `Repository`.
//!
//! This is a simple, thread-safe store using `Mutex<HashMap<...>>` and a tiny
//! secondary index by `DocumentId` for `list_by_document`. It is suitable for
//! unit tests and local development.

use std::collections::HashMap;
use std::sync::Mutex;

use contexide_core::errors::Result;
use contexide_core::{AssetId, DocumentId};

use crate::assets::Asset;
use crate::traits::{AssetsRepo, Repository};

/// In-memory assets repository (not optimized, but predictable).
#[derive(Default)]
pub struct MemAssetsRepo {
    // Primary storage keyed by AssetId.
    by_id: Mutex<HashMap<AssetId, Asset>>,
    // Secondary index: DocumentId -> Vec<AssetId>.
    by_doc: Mutex<HashMap<DocumentId, Vec<AssetId>>>,
}

impl MemAssetsRepo {
    /// Construct an empty in-memory repo.
    pub fn new() -> Self {
        Self::default()
    }

    /// Helper: update secondary index for (doc -> asset ids).
    fn index_add(&self, doc: DocumentId, id: AssetId) {
        let mut m = self.by_doc.lock().expect("poisoned");
        m.entry(doc).or_default().push(id);
    }

    /// Helper: remove an asset id from a document index bucket.
    fn index_remove(&self, doc: DocumentId, id: AssetId) {
        let mut m = self.by_doc.lock().expect("poisoned");
        if let Some(vec) = m.get_mut(&doc) {
            if let Some(pos) = vec.iter().position(|x| *x == id) {
                vec.swap_remove(pos);
            }
            if vec.is_empty() {
                m.remove(&doc);
            }
        }
    }
}

#[async_trait::async_trait]
impl Repository for MemAssetsRepo {
    type Key = AssetId;
    type Entity = Asset;

    /// Fetch by id (clone-on-read).
    async fn get(&self, id: AssetId) -> Result<Option<Asset>> {
        let m = self.by_id.lock().expect("poisoned");
        Ok(m.get(&id).cloned())
    }

    /// Save (create or update) an asset and return the stored entity.
    ///
    /// Semantics:
    /// - If the id is not present, we insert a new record and update the doc index.
    /// - If the id exists, we replace the record; if `document_id` changed,
    ///   we fix the secondary index accordingly.
    async fn save(&self, entity: Asset) -> Result<Asset> {
        let mut by_id = self.by_id.lock().expect("poisoned");

        match by_id.insert(entity.id, entity.clone()) {
            None => {
                // Newly inserted: add to doc index.
                drop(by_id); // release before locking by_doc to avoid deadlock
                self.index_add(entity.document_id, entity.id);
            }
            Some(old) => {
                // Updated: if document_id changed, fix the index mapping.
                if old.document_id != entity.document_id {
                    drop(by_id);
                    self.index_remove(old.document_id, entity.id);
                    self.index_add(entity.document_id, entity.id);
                }
            }
        }

        Ok(entity)
    }

    /// Delete by id; returns true if something was removed.
    async fn delete(&self, id: AssetId) -> Result<bool> {
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
impl AssetsRepo for MemAssetsRepo {
    /// List all assets for a document (order is insertion order best-effort).
    async fn list_by_document(&self, document_id: DocumentId) -> Result<Vec<Asset>> {
        // Snapshot the ids under the doc first to minimize lock contention.
        let ids: Vec<AssetId> = {
            let idx = self.by_doc.lock().expect("poisoned");
            idx.get(&document_id).cloned().unwrap_or_default()
        };

        // Fetch and clone assets by id.
        let store = self.by_id.lock().expect("poisoned");
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(a) = store.get(&id) {
                out.push(a.clone());
            }
        }
        Ok(out)
    }

    /// Update the blob storage key for a given asset.
    async fn set_storage_key(&self, id: AssetId, storage_key: &str) -> Result<bool> {
        let mut store = self.by_id.lock().expect("poisoned");
        if let Some(a) = store.get_mut(&id) {
            a.storage_key = Some(storage_key.to_string());
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::types::AssetSource;
    use contexide_core::{AssetId, DocumentId, TenantId};

    fn mk_asset(doc: DocumentId) -> Asset {
        Asset {
            id: AssetId::new(),
            tenant_id: TenantId::new(),
            document_id: doc,
            source: AssetSource::Upload,
            original_uri: None,
            content_type: "text/plain".to_string(),
            size_bytes: 2,
            content_hash: "abcd".to_string(),
            storage_key: None,
        }
    }

    #[tokio::test]
    async fn save_get_delete_and_list() {
        let repo = MemAssetsRepo::new();
        let d1 = DocumentId::new();
        let d2 = DocumentId::new();

        // create a1 under d1
        let a1 = mk_asset(d1);
        let a1 = repo.save(a1).await.unwrap();
        assert!(repo.get(a1.id).await.unwrap().is_some());

        // create a2 under d1
        let a2 = mk_asset(d1);
        let a2 = repo.save(a2).await.unwrap();

        // list_by_document(d1) should see [a1, a2] (order best-effort)
        let list1 = repo.list_by_document(d1).await.unwrap();
        assert_eq!(list1.len(), 2);

        // move a2 to d2 (update & re-save)
        let mut a2m = a2.clone();
        a2m.document_id = d2;
        repo.save(a2m.clone()).await.unwrap();

        // index updated: d1 has only a1, d2 has a2
        let list1b = repo.list_by_document(d1).await.unwrap();
        assert_eq!(list1b.len(), 1);
        let list2 = repo.list_by_document(d2).await.unwrap();
        assert_eq!(list2.len(), 1);
        assert_eq!(list2[0].id, a2.id);

        // set storage key
        assert!(repo.set_storage_key(a1.id, "tenant/x/a1").await.unwrap());
        let a1r = repo.get(a1.id).await.unwrap().unwrap();
        assert_eq!(a1r.storage_key.as_deref(), Some("tenant/x/a1"));

        // delete a1
        assert!(repo.delete(a1.id).await.unwrap());
        assert!(repo.get(a1.id).await.unwrap().is_none());
        let list1c = repo.list_by_document(d1).await.unwrap();
        assert!(list1c.is_empty());
    }
}

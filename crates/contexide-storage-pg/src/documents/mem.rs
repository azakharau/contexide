use std::{collections::HashMap, sync::Mutex};

use contexide_core::{DocumentId, errors::Result, types::DocumentStatus};
use uuid::Uuid;

use crate::traits::{DocumentsRepo, Repository};

use super::Document;

/// In-memory DTO returned by repo methods.
///
/// Kept tiny and framework-agnostic so it composes well across layers.
#[derive(Debug, Default)]
pub struct MemDocumentsRepo {
    map: Mutex<HashMap<Uuid, Document>>,
}

impl MemDocumentsRepo {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl DocumentsRepo for MemDocumentsRepo {
    async fn set_status(&self, id: DocumentId, status: DocumentStatus) -> Result<bool> {
        if let Some(d) = self.map.lock().unwrap().get_mut(&id.0) {
            d.status = status;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl Repository for MemDocumentsRepo {
    type Key = DocumentId;
    type Entity = Document;

    async fn get(&self, id: DocumentId) -> Result<Option<Document>> {
        Ok(self.map.lock().unwrap().get(&id.0).cloned())
    }

    async fn save(&self, mut entity: Document) -> Result<Document> {
        if entity.id.0.is_nil() {
            entity.id = DocumentId(Uuid::now_v7());
        }
        self.map.lock().unwrap().insert(entity.id.0, entity.clone());
        Ok(entity)
    }

    async fn delete(&self, id: DocumentId) -> Result<bool> {
        Ok(self.map.lock().unwrap().remove(&id.0).is_some())
    }
}

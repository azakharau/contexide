//! Extraction contracts and DTOs (domain only).
//!
//! Concrete extractors (PDF/HTML/HTTP/blob) live in adapter crates.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::ids::{AssetId, DocumentId, TenantId};
use crate::traits::ExtractorKind;
use crate::types::{AssetSource, BlockModality};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetInput {
    pub tenant_id: TenantId,
    pub document_id: DocumentId,
    pub asset_id: AssetId,
    pub source: AssetSource,
    pub storage_key: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedBlock {
    pub tenant_id: TenantId,
    pub document_id: DocumentId,
    pub asset_id: AssetId,
    pub modality: BlockModality,
    pub mime: Option<String>,
    pub text: Option<String>,
    pub blob: Option<Bytes>,
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

impl ExtractedBlock {
    pub fn text(
        tenant_id: TenantId,
        document_id: DocumentId,
        asset_id: AssetId,
        mime: Option<String>,
        text: String,
    ) -> Self {
        Self {
            tenant_id,
            document_id,
            asset_id,
            modality: BlockModality::Text,
            mime,
            text: Some(text),
            blob: None,
            metadata: serde_json::Map::new(),
        }
    }

    pub fn blob(
        tenant_id: TenantId,
        document_id: DocumentId,
        asset_id: AssetId,
        modality: BlockModality,
        mime: Option<String>,
        blob: Bytes,
    ) -> Self {
        Self {
            tenant_id,
            document_id,
            asset_id,
            modality,
            mime,
            text: None,
            blob: Some(blob),
            metadata: serde_json::Map::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExtractContext {
    pub tenant_id: TenantId,
}

#[async_trait::async_trait]
pub trait Extractor: Send + Sync + 'static {
    fn kind(&self) -> ExtractorKind;
    async fn extract(
        &self,
        ctx: &ExtractContext,
        asset: &AssetInput,
    ) -> Result<Vec<ExtractedBlock>>;
}

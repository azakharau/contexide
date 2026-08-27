//! File/Blob extractor (generic over BlobStore):
//! - Fetch bytes by `storage_key` from a provided BlobStore.
//! - Detect MIME (hint -> sniff -> octet-stream).
//! - For text/HTML/JSON/XML -> text block; otherwise -> binary block.
//!
//! Generic over `B: BlobStore`, so we avoid `dyn` object-safety issues.

use std::sync::Arc;

use bytes::Bytes;
use contexide_blob_storage_s3::BlobStore;
use contexide_core::errors::{Error, Result};
use contexide_core::ids::{AssetId, DocumentId, TenantId};
use contexide_core::traits::ExtractorKind;
use contexide_core::types::BlockModality;
use infer::Infer;
use serde_json::json;

use contexide_core::extractor::{AssetInput, ExtractContext, ExtractedBlock, Extractor};

pub struct BlobFileExtractor<B: BlobStore> {
    store: Arc<B>,
}

impl<B: BlobStore> BlobFileExtractor<B> {
    pub fn new(store: Arc<B>) -> Self {
        Self { store }
    }

    /// 1) use hint; 2) sniff; 3) octet-stream
    fn detect_mime(hint: Option<&str>, data: &[u8]) -> String {
        if let Some(ct) = hint {
            let ct = ct.trim();
            if !ct.is_empty() {
                return ct.to_lowercase();
            }
        }
        let inf = Infer::new();
        if let Some(kind) = inf.get(data) {
            return kind.mime_type().to_lowercase();
        }
        "application/octet-stream".into()
    }

    fn modality_for_mime(mime: &str) -> BlockModality {
        if mime.starts_with("text/") || matches!(mime, "application/json" | "application/xml") {
            BlockModality::Text
        } else if mime.starts_with("image/") {
            BlockModality::Image
        } else {
            BlockModality::Binary
        }
    }

    fn to_text(mime: &str, data: &[u8]) -> Option<String> {
        if mime == "text/html" {
            // html -> text (lossy utf-8)
            let html = String::from_utf8_lossy(data);
            let txt = html2text::from_read(html.as_bytes(), 80).unwrap_or_default();
            let txt = txt.trim();
            if txt.is_empty() {
                None
            } else {
                Some(txt.to_string())
            }
        } else if mime.starts_with("text/")
            || matches!(mime, "application/json" | "application/xml")
        {
            let s = String::from_utf8_lossy(data).to_string();
            let s = s.trim().to_string();
            if s.is_empty() { None } else { Some(s) }
        } else {
            None
        }
    }

    fn as_text_block(
        tenant: TenantId,
        doc: DocumentId,
        asset: AssetId,
        mime: Option<String>,
        text: String,
        bytes_len: usize,
    ) -> ExtractedBlock {
        let mut b = ExtractedBlock::text(tenant, doc, asset, mime, text);
        b.metadata.insert("bytes_len".into(), json!(bytes_len));
        b
    }

    fn as_blob_block(
        tenant: TenantId,
        doc: DocumentId,
        asset: AssetId,
        modality: BlockModality,
        mime: Option<String>,
        data: Bytes,
    ) -> ExtractedBlock {
        ExtractedBlock::blob(tenant, doc, asset, modality, mime, data)
    }
}

#[async_trait::async_trait]
impl<B: BlobStore + 'static> Extractor for BlobFileExtractor<B> {
    fn kind(&self) -> ExtractorKind {
        ExtractorKind::File
    }

    async fn extract(
        &self,
        _ctx: &ExtractContext,
        asset: &AssetInput,
    ) -> Result<Vec<ExtractedBlock>> {
        let key = asset
            .storage_key
            .as_ref()
            .ok_or_else(|| Error::NotFound("asset.storage_key"))?;

        let data = self.store.get_bytes(key).await?;
        if data.is_empty() {
            return Ok(vec![]);
        }

        let mime = Self::detect_mime(asset.content_type.as_deref(), &data);
        let modality = Self::modality_for_mime(&mime);

        let block = if let Some(text) = Self::to_text(&mime, &data) {
            Self::as_text_block(
                asset.tenant_id,
                asset.document_id,
                asset.asset_id,
                Some(mime),
                text,
                data.len(),
            )
        } else {
            Self::as_blob_block(
                asset.tenant_id,
                asset.document_id,
                asset.asset_id,
                modality,
                Some(mime),
                data,
            )
        };

        Ok(vec![block])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_blob_storage_s3::MemStore;
    use contexide_core::ids::{AssetId, DocumentId, TenantId};
    use contexide_core::types::AssetSource;

    fn mk_ids() -> (TenantId, DocumentId, AssetId) {
        (TenantId::new(), DocumentId::new(), AssetId::new())
    }

    #[tokio::test]
    async fn extracts_plain_text() {
        let (t, d, a) = mk_ids();
        let store = Arc::new(MemStore::default());
        // preload
        store
            .put_bytes(
                "mem://x",
                Bytes::from_static(b"hello world"),
                Some("text/plain"),
            )
            .await
            .unwrap();

        let ex = BlobFileExtractor::new(store);

        let asset = AssetInput {
            tenant_id: t,
            document_id: d,
            asset_id: a,
            source: AssetSource::Upload,
            storage_key: Some("mem://x".into()),
            content_type: Some("text/plain".into()),
        };

        let out = ex
            .extract(&ExtractContext { tenant_id: t }, &asset)
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].modality, BlockModality::Text);
        assert_eq!(out[0].text.as_deref(), Some("hello world"));
    }
}

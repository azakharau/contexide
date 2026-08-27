//! PDF extractor:
//! - Fetches bytes either from BlobStore or via HTTP (if storage_key is http/https).
//! - Detects PDF by content-type or sniffing (infer).
//! - Extracts text via `pdf_extract::extract_text_from_mem`.
//! - Falls back to binary block if no text could be extracted (e.g., scanned PDF).
//!
//! Notes:
//! - Requires crate `pdf-extract` (pure Rust; reasonable text results for many PDFs).
//! - Router should prefer this extractor when asset is (or likely is) a PDF.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use bytes::Bytes;
use contexide_blob_storage_s3::BlobStore;
use contexide_core::errors::{Error, Result};
use contexide_core::ids::{AssetId, DocumentId, TenantId};
use contexide_core::traits::ExtractorKind;
use contexide_core::types::BlockModality;
use infer::Infer;
use pdf_extract as pdfx;
use reqwest::{Client, StatusCode, redirect};
use serde_json::json;

use contexide_core::extractor::{AssetInput, ExtractContext, ExtractedBlock, Extractor};

pub struct PdfExtractor<B: BlobStore> {
    store: Arc<B>,
    http: Client,
}

impl<B: BlobStore> PdfExtractor<B> {
    /// Construct with a BlobStore and optional HTTP client.
    pub fn new(store: Arc<B>, client: Option<Client>) -> Self {
        Self {
            store,
            http: client.unwrap_or_else(Self::default_client),
        }
    }

    fn default_client() -> Client {
        Client::builder()
            .timeout(Duration::from_secs(20))
            .redirect(redirect::Policy::limited(5))
            .user_agent("contexide-pdf-extractor/0.1")
            .build()
            .expect("reqwest client")
    }

    /// Detect MIME: priority is hint -> sniff -> octet-stream.
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

    /// Return true if MIME looks like PDF.
    #[inline]
    fn is_pdf(mime: &str) -> bool {
        mime == "application/pdf" || mime == "application/x-pdf"
    }

    /// Convert extracted text into a text block with metadata.
    fn as_text_block(
        tenant: TenantId,
        doc: DocumentId,
        asset: AssetId,
        mime: Option<String>,
        text: String,
        bytes_len: usize,
        extra_meta: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> ExtractedBlock {
        let mut b = ExtractedBlock::text(tenant, doc, asset, mime, text);
        b.metadata.insert("bytes_len".into(), json!(bytes_len));
        for (k, v) in extra_meta {
            b.metadata.insert(k, v);
        }
        b
    }

    /// Binary fallback block with metadata.
    fn as_blob_block(
        tenant: TenantId,
        doc: DocumentId,
        asset: AssetId,
        mime: Option<String>,
        data: Bytes,
        extra_meta: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> ExtractedBlock {
        let mut b = ExtractedBlock::blob(tenant, doc, asset, BlockModality::Binary, mime, data);
        for (k, v) in extra_meta {
            b.metadata.insert(k, v);
        }
        b
    }

    /// Fetch bytes: http(s) via reqwest, else via BlobStore.
    async fn fetch_bytes(
        &self,
        asset: &AssetInput,
    ) -> Result<(Bytes, Option<String>, Option<StatusCode>)> {
        if let Some(key) = &asset.storage_key {
            if key.starts_with("http://") || key.starts_with("https://") {
                let resp = self
                    .http
                    .get(key)
                    .send()
                    .await
                    .with_context(|| format!("GET {} failed", key))?;
                let status = resp.status();
                if !status.is_success() {
                    return Err(Error::Other(anyhow!("GET {} -> {}", key, status)));
                }
                let ct_hdr = resp
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let body = resp
                    .bytes()
                    .await
                    .with_context(|| format!("read body {} failed", key))?;
                return Ok((body, ct_hdr, Some(status)));
            }
            // Otherwise, treat as blob key
            let bytes = self.store.get_bytes(key).await?;
            return Ok((bytes, asset.content_type.clone(), None));
        }
        Err(Error::NotFound("asset.storage_key"))
    }
}

#[async_trait::async_trait]
impl<B: BlobStore + 'static> Extractor for PdfExtractor<B> {
    fn kind(&self) -> ExtractorKind {
        ExtractorKind::Pdf
    }

    async fn extract(
        &self,
        _ctx: &ExtractContext,
        asset: &AssetInput,
    ) -> Result<Vec<ExtractedBlock>> {
        let (bytes, ct_hint, http_status) = self.fetch_bytes(asset).await?;
        if bytes.is_empty() {
            return Ok(vec![]);
        }

        let mime = Self::detect_mime(ct_hint.as_deref(), &bytes);
        if !Self::is_pdf(&mime) {
            // Not a PDF; let router/fallback handle it.
            return Ok(vec![]);
        }

        // Try to extract text; if it fails or empty, return binary block.
        let text = pdfx::extract_text_from_mem(&bytes)
            .map_err(|e| Error::Other(anyhow!("pdf extract failed: {}", e)))?;
        let text = text.trim().to_string();

        // Build metadata
        let mut meta = vec![("content_type".to_string(), json!(mime))];
        if let Some(status) = http_status {
            meta.push(("http_status".to_string(), json!(status.as_u16())));
        }
        if let Some(url) = asset
            .storage_key
            .as_deref()
            .filter(|u| u.starts_with("http"))
        {
            meta.push(("source_url".to_string(), json!(url)));
        }

        let block = if !text.is_empty() {
            Self::as_text_block(
                asset.tenant_id,
                asset.document_id,
                asset.asset_id,
                Some("application/pdf".into()),
                text,
                bytes.len(),
                meta,
            )
        } else {
            Self::as_blob_block(
                asset.tenant_id,
                asset.document_id,
                asset.asset_id,
                Some("application/pdf".into()),
                bytes,
                meta,
            )
        };

        Ok(vec![block])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_blob_storage_s3::MemStore;
    use contexide_core::types::AssetSource;

    fn ids() -> (TenantId, DocumentId, AssetId) {
        (TenantId::new(), DocumentId::new(), AssetId::new())
    }

    #[tokio::test]
    async fn non_pdf_returns_empty() {
        let store = Arc::new(MemStore::default());
        store
            .put_bytes("k", Bytes::from_static(b"not a pdf"), Some("text/plain"))
            .await
            .unwrap();
        let ex = PdfExtractor::new(store, None);

        let (t, d, a) = ids();
        let asset = AssetInput {
            tenant_id: t,
            document_id: d,
            asset_id: a,
            source: AssetSource::Upload,
            storage_key: Some("k".into()),
            content_type: Some("text/plain".into()),
        };

        let out = ex
            .extract(&ExtractContext { tenant_id: t }, &asset)
            .await
            .unwrap();
        assert!(out.is_empty());
    }
}

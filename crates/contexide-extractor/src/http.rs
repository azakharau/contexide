// crates/contexide-extractor/src/http.rs
//! HTTP extractor:
//! - Fetches a URL from `asset.storage_key` (must be http/https).
//! - Uses `reqwest` client with sane defaults (timeout, redirects).
//! - MIME detection: header -> sniff -> octet-stream.
//! - Text for text/*, application/json, application/xml, text/html (via `html2text`).
//! - Returns a single `ExtractedBlock` with basic metadata.

use std::time::Duration;

use anyhow::{Context, anyhow};
use bytes::Bytes;
use contexide_core::errors::{Error, Result};
use contexide_core::ids::{AssetId, DocumentId, TenantId};
use contexide_core::traits::ExtractorKind;
use contexide_core::types::BlockModality;
use infer::Infer;
use reqwest::{Client, StatusCode, redirect};
use serde_json::json;

use contexide_core::extractor::{AssetInput, ExtractContext, ExtractedBlock, Extractor};

pub struct HttpExtractor {
    client: Client,
}

impl HttpExtractor {
    /// Build with provided client or a sensible default.
    pub fn new(client: Option<Client>) -> Self {
        Self {
            client: client.unwrap_or_else(Self::default_client),
        }
    }

    fn default_client() -> Client {
        Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(redirect::Policy::limited(5))
            .user_agent("contexide-http-extractor/0.1")
            .build()
            .expect("reqwest client")
    }

    /// 1) header hint; 2) sniff; 3) octet-stream.
    fn detect_mime(hint: Option<&str>, body: &[u8]) -> String {
        if let Some(ct) = hint {
            let ct = ct.trim();
            if !ct.is_empty() {
                return ct.to_lowercase();
            }
        }
        let inf = Infer::new();
        if let Some(kind) = inf.get(body) {
            return kind.mime_type().to_lowercase();
        }
        "application/octet-stream".into()
    }

    /// Coarse modality from MIME.
    fn modality_for_mime(mime: &str) -> BlockModality {
        if mime.starts_with("text/") || matches!(mime, "application/json" | "application/xml") {
            BlockModality::Text
        } else if mime.starts_with("image/") {
            BlockModality::Image
        } else {
            BlockModality::Binary
        }
    }

    /// Convert bytes to text for the supported MIME types.
    fn to_text(mime: &str, data: &[u8]) -> Option<String> {
        if mime == "text/html" {
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

    #[allow(clippy::too_many_arguments)]
    fn as_text_block(
        tenant: TenantId,
        doc: DocumentId,
        asset: AssetId,
        mime: Option<String>,
        text: String,
        bytes_len: usize,
        url: &str,
        status: StatusCode,
    ) -> ExtractedBlock {
        let mut b = ExtractedBlock::text(tenant, doc, asset, mime, text);
        b.metadata.insert("bytes_len".into(), json!(bytes_len));
        b.metadata.insert("source_url".into(), json!(url));
        b.metadata
            .insert("http_status".into(), json!(status.as_u16()));
        b
    }

    #[allow(clippy::too_many_arguments)]
    fn as_blob_block(
        tenant: TenantId,
        doc: DocumentId,
        asset: AssetId,
        modality: BlockModality,
        mime: Option<String>,
        data: Bytes,
        url: &str,
        status: StatusCode,
    ) -> ExtractedBlock {
        let mut b = ExtractedBlock::blob(tenant, doc, asset, modality, mime, data);
        b.metadata.insert("source_url".into(), json!(url));
        b.metadata
            .insert("http_status".into(), json!(status.as_u16()));
        b
    }

    /// Validate and return URL string from asset.storage_key.
    fn require_url(asset: &AssetInput) -> Result<&str> {
        let url = asset
            .storage_key
            .as_deref()
            .ok_or_else(|| Error::NotFound("asset.storage_key"))?;
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(Error::Other(anyhow!(
                "storage_key is not an http(s) URL: {}",
                url
            )));
        }
        Ok(url)
    }
}

#[async_trait::async_trait]
impl Extractor for HttpExtractor {
    fn kind(&self) -> ExtractorKind {
        ExtractorKind::Html
    }

    async fn extract(
        &self,
        _ctx: &ExtractContext,
        asset: &AssetInput,
    ) -> Result<Vec<ExtractedBlock>> {
        let url = Self::require_url(asset)?;
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {} failed", url))?;

        let status = resp.status();
        if !status.is_success() {
            return Err(Error::Other(anyhow!("GET {} -> {}", url, status)));
        }

        let ct_hdr = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let bytes = resp
            .bytes()
            .await
            .with_context(|| format!("read body {} failed", url))?;

        let mime = Self::detect_mime(ct_hdr.as_deref(), &bytes);
        let modality = Self::modality_for_mime(&mime);

        let block = if let Some(text) = Self::to_text(&mime, &bytes) {
            Self::as_text_block(
                asset.tenant_id,
                asset.document_id,
                asset.asset_id,
                Some(mime),
                text,
                bytes.len(),
                url,
                status,
            )
        } else {
            Self::as_blob_block(
                asset.tenant_id,
                asset.document_id,
                asset.asset_id,
                modality,
                Some(mime),
                bytes,
                url,
                status,
            )
        };

        Ok(vec![block])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modality_basic() {
        assert!(matches!(
            HttpExtractor::modality_for_mime("text/plain"),
            BlockModality::Text
        ));
        assert!(matches!(
            HttpExtractor::modality_for_mime("image/png"),
            BlockModality::Image
        ));
        assert!(matches!(
            HttpExtractor::modality_for_mime("application/octet-stream"),
            BlockModality::Binary
        ));
    }

    #[test]
    fn url_validation() {
        use contexide_core::types::AssetSource;
        let asset = AssetInput {
            tenant_id: TenantId::new(),
            document_id: DocumentId::new(),
            asset_id: AssetId::new(),
            source: AssetSource::Upload,
            storage_key: Some("https://example.com".into()),
            content_type: None,
        };
        assert!(HttpExtractor::require_url(&asset).is_ok());

        let bad = AssetInput {
            storage_key: Some("file://x".into()),
            ..asset
        };
        assert!(HttpExtractor::require_url(&bad).is_err());
    }
}

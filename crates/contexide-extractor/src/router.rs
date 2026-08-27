// crates/contexide-extractor/src/router.rs
//! Extractor router (MVP):
//! - Decide which extractor to call based on `AssetInput` hints.
//! - Prefer PDF extractor when likely a PDF (content-type or .pdf extension).
//! - Use HTTP extractor for http(s) URLs.
//! - Fallback to File extractor for everything else.
//!
//! Design notes:
//! - Keep it deterministic and cheap; avoid double-fetch when possible.
//! - PDF extractor itself re-validates MIME/sniffing and may return empty if not PDF;
//!   in that case we fallback to HTTP/File branch.
//!
//! Extensibility:
//! - Add more extractors and rules as needed (audio/image/office/etc).
//! - Consider priority list or a small rule engine later.

use std::sync::Arc;

use contexide_blob_storage_s3::BlobStore;
use contexide_core::errors::Result;
use contexide_core::traits::ExtractorKind;
use reqwest::Client;

use crate::file::BlobFileExtractor;
use crate::http::HttpExtractor;
use crate::pdf::PdfExtractor;
use contexide_core::extractor::{AssetInput, ExtractContext, ExtractedBlock, Extractor};

pub struct ExtractRouter<B: BlobStore> {
    file: BlobFileExtractor<B>,
    http: HttpExtractor,
    pdf: PdfExtractor<B>,
}

impl<B: BlobStore + 'static> ExtractRouter<B> {
    /// Build router with required dependencies.
    ///
    /// `store` is used by File/Pdf extractors; `http_client` is reused by Http/Pdf.
    pub fn new(store: Arc<B>, http_client: Option<Client>) -> Self {
        let http = HttpExtractor::new(http_client.clone());
        let pdf = PdfExtractor::new(Arc::clone(&store), http_client);
        let file = BlobFileExtractor::new(store);
        Self { file, http, pdf }
    }

    /// Public entry: choose extractor and return extracted blocks.
    pub async fn extract(
        &self,
        ctx: &ExtractContext,
        asset: &AssetInput,
    ) -> Result<Vec<ExtractedBlock>> {
        match self.choose(asset) {
            Route::Pdf => {
                // Try PDF; if it returns empty (not a PDF or failed to extract text), fallback.
                let out = self.pdf.extract(ctx, asset).await?;
                if !out.is_empty() {
                    return Ok(out);
                }
                // Decide fallback branch based on URL vs blob.
                if is_url(asset.storage_key.as_deref()) {
                    self.http.extract(ctx, asset).await
                } else {
                    self.file.extract(ctx, asset).await
                }
            }
            Route::Http => self.http.extract(ctx, asset).await,
            Route::File => self.file.extract(ctx, asset).await,
        }
    }

    /// Light-weight decision without I/O.
    pub fn choose(&self, asset: &AssetInput) -> Route {
        // 1) If it's likely a PDF, try PDF first (with fallback).
        if is_likely_pdf(asset) {
            return Route::Pdf;
        }
        // 2) If it's a URL, use HTTP extractor.
        if is_url(asset.storage_key.as_deref()) {
            return Route::Http;
        }
        // 3) Default to file extractor.
        Route::File
    }

    /// Optional: expose which extractor would be chosen (for logging/metrics).
    pub fn chosen_kind(&self, asset: &AssetInput) -> ExtractorKind {
        match self.choose(asset) {
            Route::Pdf => ExtractorKind::Pdf,
            Route::Http => ExtractorKind::Html,
            Route::File => ExtractorKind::File,
        }
    }
}

/// Route enum used internally and for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Try PDF first; if it yields empty result, fallback to Http/File.
    Pdf,
    /// Route to HTTP extractor.
    Http,
    /// Route to File extractor.
    File,
}

/// Heuristic: treat as PDF when content_type mentions "pdf" or path ends with ".pdf".
fn is_likely_pdf(asset: &AssetInput) -> bool {
    if let Some(ct) = asset.content_type.as_deref()
        && ct.to_ascii_lowercase().contains("pdf")
    {
        return true;
    }
    if let Some(k) = asset.storage_key.as_deref() {
        // Normalize: take last path segment, ignore query/fragment if URL.
        let lower = k.to_ascii_lowercase();
        if lower.ends_with(".pdf") || lower.contains(".pdf?") {
            return true;
        }
    }
    false
}

/// True if `storage_key` looks like an http(s) URL.
fn is_url(key: Option<&str>) -> bool {
    if let Some(k) = key {
        k.starts_with("http://") || k.starts_with("https://")
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::ids::{AssetId, DocumentId, TenantId};
    use contexide_core::types::AssetSource;

    fn asset_with(key: Option<&str>, ct: Option<&str>) -> AssetInput {
        AssetInput {
            tenant_id: TenantId::new(),
            document_id: DocumentId::new(),
            asset_id: AssetId::new(),
            source: AssetSource::Upload,
            storage_key: key.map(|s| s.to_string()),
            content_type: ct.map(|s| s.to_string()),
        }
    }

    #[test]
    fn choose_pdf_by_ct() {
        let a = asset_with(Some("k"), Some("application/pdf"));
        // store/http are not used by `choose`, so we can build with dummy client/store later if needed.
        assert!(is_likely_pdf(&a));
    }

    #[test]
    fn choose_pdf_by_ext() {
        let a = asset_with(Some("https://x/y.ZIP.pdf?dl=1"), None);
        assert!(is_likely_pdf(&a));
    }

    #[test]
    fn choose_http_for_url() {
        let a = asset_with(Some("https://example.com/file.txt"), None);
        assert!(is_url(a.storage_key.as_deref()));
    }

    #[test]
    fn choose_file_for_blob_key() {
        let a = asset_with(Some("tenant/abc/asset123"), None);
        assert!(!is_url(a.storage_key.as_deref()));
    }
}

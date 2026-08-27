//! Blob storage contract (transport-agnostic).
//!
//! Concrete stores (S3/MinIO, memory, filesystem) live in adapter crates.

use std::time::Duration;

use bytes::Bytes;
use futures::io::AsyncRead;

use crate::errors::Result;

/// Minimal object metadata for reads.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    pub file_size: Option<u64>,
    pub etag: Option<String>,
}

#[async_trait::async_trait]
pub trait BlobStore: Send + Sync {
    async fn put_bytes(&self, key: &str, bytes: Bytes, content_type: Option<&str>) -> Result<()>;

    async fn put_reader<R>(
        &self,
        reader: R,
        content_type: Option<&str>,
        content_length: Option<u64>,
    ) -> Result<String>
    where
        R: AsyncRead + Unpin + Send + Sync + 'static;

    async fn get_bytes(&self, key: &str) -> Result<Bytes>;
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn delete(&self, key: &str) -> Result<bool>;

    async fn presign_get(&self, key: &str, expires: Duration) -> Result<String>;
    async fn presign_put(
        &self,
        key: &str,
        expires: Duration,
        content_type: Option<&str>,
    ) -> Result<String>;
}

//! S3/MinIO-backed implementation of `BlobStore` (no streaming).
//!
//! Notes:
//! - Uses `contexide-config::BlobStorageConfig` (ENV-only).
//! - MinIO: custom `endpoint` + `force_path_style(true)`.
//! - `put_reader` buffers into RAM in MVP and returns generated key.

use std::time::Duration;

use anyhow::anyhow;
use bytes::Bytes;
use futures::io::{AsyncRead, AsyncReadExt};

use aws_config::{self, BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    Client, config::Builder as S3ConfigBuilder, error::SdkError, presigning::PresigningConfig,
    primitives::ByteStream as SdkByteStream,
};

use contexide_config::BlobStorageConfig;
use contexide_core::errors::{Error, Result};

use crate::BlobStore;

/// S3/MinIO store.
pub struct S3Store {
    client: Client,
    bucket: String,
}

impl S3Store {
    /// Build client from typed config.
    pub async fn from_config(cfg: &BlobStorageConfig) -> Result<Self> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(Region::new(
            cfg.region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string()),
        ));

        // Use static credentials if provided; otherwise fall back to default chain.
        if !cfg.access_key.is_empty() && !cfg.secret_key.is_empty() {
            loader = loader.credentials_provider(Credentials::new(
                cfg.access_key.clone(),
                cfg.secret_key.clone(),
                None,
                None,
                "contexide-blob-storage",
            ));
        }

        let shared = loader.load().await;

        let mut s3_cfg: S3ConfigBuilder =
            S3ConfigBuilder::from(&shared).force_path_style(cfg.path_style);
        s3_cfg = s3_cfg.endpoint_url(cfg.endpoint.clone());

        let client = Client::from_conf(s3_cfg.build());

        Ok(Self {
            client,
            bucket: cfg.bucket.clone(),
        })
    }

    #[inline]
    fn normalize_key(&self, key: &str) -> String {
        key.trim_start_matches('/').to_string()
    }
}

#[async_trait::async_trait]
impl BlobStore for S3Store {
    async fn put_bytes(&self, key: &str, bytes: Bytes, content_type: Option<&str>) -> Result<()> {
        let key = self.normalize_key(key);

        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(SdkByteStream::from(bytes));

        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }

        req.send()
            .await
            .map_err(|e| Error::Other(anyhow!("put_object {} failed: {}", key, e)))?;

        Ok(())
    }

    async fn put_reader<R>(
        &self,
        mut reader: R,
        content_type: Option<&str>,
        _content_length: Option<u64>,
    ) -> Result<String>
    where
        R: AsyncRead + Unpin + Send + Sync + 'static,
    {
        let key = format!("uploads/{}", uuid::Uuid::now_v7());

        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| Error::Other(anyhow!("read_to_end failed: {}", e)))?;

        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(SdkByteStream::from(buf));

        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }

        req.send()
            .await
            .map_err(|e| Error::Other(anyhow!("put_object {} failed: {}", key, e)))?;

        Ok(key)
    }

    async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        let key = self.normalize_key(key);

        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Error::Other(anyhow!("get_object {} failed: {}", key, e)))?;

        let data = out
            .body
            .collect()
            .await
            .map_err(|e| Error::Other(anyhow!("collect body {} failed: {}", key, e)))?;

        Ok(data.into_bytes())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let key = self.normalize_key(key);

        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                if let SdkError::ServiceError(s) = &err {
                    use aws_sdk_s3::operation::head_object::HeadObjectError;
                    if matches!(s.err(), HeadObjectError::NotFound(_)) {
                        return Ok(false);
                    }
                }
                Err(Error::Other(anyhow!("head_object {} failed: {}", key, err)))
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let key = self.normalize_key(key);

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Error::Other(anyhow!("delete_object {} failed: {}", key, e)))?;

        Ok(true)
    }

    async fn presign_get(&self, key: &str, expires: Duration) -> Result<String> {
        let key = self.normalize_key(key);

        let cfg = PresigningConfig::expires_in(expires)
            .map_err(|e| Error::Other(anyhow!("presign cfg get {} failed: {}", key, e)))?;

        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .presigned(cfg)
            .await
            .map_err(|e| Error::Other(anyhow!("presign GET {} failed: {}", key, e)))?;

        Ok(presigned.uri().to_string())
    }

    async fn presign_put(
        &self,
        key: &str,
        expires: Duration,
        content_type: Option<&str>,
    ) -> Result<String> {
        let key = self.normalize_key(key);

        let cfg = PresigningConfig::expires_in(expires)
            .map_err(|e| Error::Other(anyhow!("presign cfg put {} failed: {}", key, e)))?;

        let mut builder = self.client.put_object().bucket(&self.bucket).key(&key);

        if let Some(ct) = content_type {
            builder = builder.content_type(ct);
        }

        let presigned = builder
            .presigned(cfg)
            .await
            .map_err(|e| Error::Other(anyhow!("presign PUT {} failed: {}", key, e)))?;

        Ok(presigned.uri().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn trait_obj_is_possible() {
        fn _assert_obj_safe<T: BlobStore>() {}
        _assert_obj_safe::<S3Store>();
    }
}

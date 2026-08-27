//! In-memory implementation of `BlobStore` (no streaming).
//!
//! Use for tests/local runs. Stores objects in a Mutex<HashMap>.

use std::{collections::HashMap, sync::Mutex, time::Duration};

use anyhow::anyhow;
use bytes::Bytes;
use futures::io::{AsyncRead, AsyncReadExt};

use contexide_core::errors::{Error, Result};

use crate::BlobStore;

#[derive(Debug, Clone)]
struct Entry {
    bytes: Bytes,
    _content_type: Option<String>,
    _etag: Option<String>,
}

pub struct MemStore {
    map: Mutex<HashMap<String, Entry>>,
    pub namespace: String,
}

impl Default for MemStore {
    fn default() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            namespace: "mem".to_string(),
        }
    }
}

impl MemStore {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            namespace: namespace.into(),
        }
    }

    #[inline]
    fn normalize_key(&self, key: &str) -> String {
        key.trim_start_matches('/').to_string()
    }
}

#[async_trait::async_trait]
impl BlobStore for MemStore {
    async fn put_bytes(&self, key: &str, bytes: Bytes, content_type: Option<&str>) -> Result<()> {
        let key = self.normalize_key(key);
        let mut g = self
            .map
            .lock()
            .map_err(|_| Error::Other(anyhow!("mutex poisoned")))?;
        g.insert(
            key,
            Entry {
                bytes,
                _content_type: content_type.map(|s| s.to_string()),
                _etag: None,
            },
        );
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
        let key = format!("{}/{}", self.namespace, uuid::Uuid::now_v7());
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| Error::Other(anyhow!("read_to_end failed: {}", e)))?;

        let mut g = self
            .map
            .lock()
            .map_err(|_| Error::Other(anyhow!("mutex poisoned")))?;
        g.insert(
            key.clone(),
            Entry {
                bytes: Bytes::from(buf),
                _content_type: content_type.map(|s| s.to_string()),
                _etag: None,
            },
        );
        Ok(key)
    }

    async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        let key = self.normalize_key(key);
        let g = self
            .map
            .lock()
            .map_err(|_| Error::Other(anyhow!("mutex poisoned")))?;
        let e = g.get(&key).ok_or_else(|| Error::NotFound("blob"))?;
        Ok(e.bytes.clone())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let key = self.normalize_key(key);
        let g = self
            .map
            .lock()
            .map_err(|_| Error::Other(anyhow!("mutex poisoned")))?;
        Ok(g.contains_key(&key))
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let key = self.normalize_key(key);
        let mut g = self
            .map
            .lock()
            .map_err(|_| Error::Other(anyhow!("mutex poisoned")))?;
        Ok(g.remove(&key).is_some())
    }

    async fn presign_get(&self, key: &str, expires: Duration) -> Result<String> {
        let key = self.normalize_key(key);
        Ok(format!("mem://get/{}?exp={}s", key, expires.as_secs()))
    }

    async fn presign_put(
        &self,
        key: &str,
        expires: Duration,
        content_type: Option<&str>,
    ) -> Result<String> {
        let key = self.normalize_key(key);
        let ct = content_type.unwrap_or("");
        Ok(format!(
            "mem://put/{}?exp={}s&ct={}",
            key,
            expires.as_secs(),
            urlencoding::encode(ct)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_roundtrip() {
        let store = MemStore::default();
        store
            .put_bytes("k", Bytes::from_static(b"hello"), Some("text/plain"))
            .await
            .unwrap();

        let got = store.get_bytes("k").await.unwrap();
        assert_eq!(&got[..], b"hello");
    }

    #[tokio::test]
    async fn reader_generates_key_and_exists() {
        let store = MemStore::new("ns");
        let key = store
            .put_reader(&b"xyz"[..], Some("application/octet-stream"), None)
            .await
            .unwrap();
        assert!(key.starts_with("ns/"));
        assert!(store.exists(&key).await.unwrap());
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let store = MemStore::default();
        assert!(!store.delete("missing").await.unwrap());
        store
            .put_bytes("x", Bytes::from_static(b"1"), None)
            .await
            .unwrap();
        assert!(store.delete("x").await.unwrap());
        assert!(!store.delete("x").await.unwrap());
    }
}

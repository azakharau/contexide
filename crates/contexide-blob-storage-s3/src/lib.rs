pub use contexide_core::blob::{BlobStore, ObjectMeta};
use contexide_core::errors::Result;
pub use mem::MemStore;
pub use s3::S3Store;

pub mod mem;
pub mod s3;

pub mod prelude {
    pub use crate::{BlobStore, MemStore, ObjectMeta, S3Store};
}

/// Build an `S3Store` from environment (via `contexide-config`).
///
/// Useful for binaries/tests that don’t want to plumb config manually.
/// Fails if required env vars are missing or invalid.
pub async fn s3_from_env() -> Result<S3Store> {
    let cfg = contexide_config::load_blob_storage()?;
    S3Store::from_config(&cfg).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn prelude_exports_compile() {
        // Ensure re-exports stay stable.
        fn _uses_prelude() {
            use crate::prelude::*;
            let _ = std::any::type_name::<S3Store>();
            let _ = std::any::type_name::<MemStore>();
            let _ = std::any::type_name::<ObjectMeta>();
        }
        _uses_prelude();
    }
}

use contexide_core::{EmbeddingSetId, TenantId};

/// Helper: derive a stable collection name from domain anchors.
///
/// Convention: `<prefix>__<tenant>__<embedding_set>`
/// - `prefix` comes from config (e.g., "contexide").
/// - `tenant` and `embedding_set` are UUIDs (hyphenated).
pub fn derive_collection_name(
    prefix: &str,
    tenant: TenantId,
    embedding_set: EmbeddingSetId,
) -> String {
    format!(
        "{}__{}__{}",
        prefix,
        tenant.0.hyphenated(),
        embedding_set.0.hyphenated()
    )
}

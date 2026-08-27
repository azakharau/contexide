use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetFetchedPayload {
    pub asset_id: crate::AssetId,
    pub origin_uri: String,
    pub storage_uri: String,
    pub mime: String,
    pub size_bytes: u64,
    pub content_hash: String, // b3 hex
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlocksExtractedPayload {
    pub asset_id: crate::AssetId,
    pub blocks_count: u32,
    pub extractor_kind: String,
    pub extractor_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunksCreatedPayload {
    pub chunk_set_id: crate::ChunkSetId,
    pub count: u32,
    pub profile_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingsCreatedPayload {
    pub embedding_set_id: crate::EmbeddingSetId,
    pub count: u32,
    pub model_id: String,
}

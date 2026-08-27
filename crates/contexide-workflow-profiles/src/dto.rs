use contexide_core::prelude::{AssetId, DocumentId, TenantId};

/// Common fields shared by all workflow starts.
#[derive(Debug, Clone)]
pub struct WorkflowStartCommon {
    pub tenant_id: TenantId,
    /// Optional pre-created document id. Some profiles may require it.
    pub document_id: Option<DocumentId>,
    /// Optional opaque correlation id for observability.
    pub correlation_id: Option<String>,
}

/// Inputs for ingest-only pipeline.
#[derive(Debug, Clone)]
pub struct IngestOnlyInput {
    pub common: WorkflowStartCommon,
    /// Assets to process (e.g., uploaded files).
    pub asset_ids: Vec<AssetId>,
    /// Optional language hint.
    pub language_hint: Option<String>,
}

/// Inputs for full RAG index pipeline.
#[derive(Debug, Clone)]
pub struct FullRagIndexInput {
    pub common: WorkflowStartCommon,
    pub asset_ids: Vec<AssetId>,
    /// Retrieval profile name (optional).
    pub retrieval_profile: Option<String>,
    /// Force reprocess existing artifacts.
    pub force_rebuild: bool,
}

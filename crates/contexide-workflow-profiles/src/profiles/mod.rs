use std::sync::Arc;

use async_trait::async_trait;
use contexide_core::errors::Result;
use contexide_core::prelude::{DagRunId, TenantId};
use contexide_workflow_core::dag::Dag;

use crate::dto::{FullRagIndexInput, IngestOnlyInput};
use crate::errors::ProfileError;

pub mod full_rag_index;
pub mod ingest_only;

/// Stable identifiers for workflow profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkflowProfileKind {
    /// Minimal ingest-only pipeline (extract → normalize → chunk).
    IngestOnly,
    /// Full RAG index build (extract → normalize → chunk → embed → index).
    FullRagIndex,
}

/// Small abstraction for creating a DagRun from a Dag definition.
#[async_trait]
pub trait DagStarter: Send + Sync {
    async fn start(&self, tenant: TenantId, dag: Dag) -> Result<DagRunId>;
}

/// Facade holding dependencies needed by profile starters.
pub struct WorkflowProfiles {
    starter: Arc<dyn DagStarter>,
}

impl WorkflowProfiles {
    pub fn new(starter: Arc<dyn DagStarter>) -> Self {
        Self { starter }
    }

    /// Start ingest-only profile.
    pub async fn start_ingest_only(&self, input: IngestOnlyInput) -> Result<DagRunId> {
        let dag = ingest_only::build_dag(&input)?;
        self.starter.start(input.common.tenant_id, dag).await
    }

    /// Start full RAG index profile.
    pub async fn start_full_rag_index(&self, input: FullRagIndexInput) -> Result<DagRunId> {
        let dag = full_rag_index::build_dag(&input)?;
        self.starter.start(input.common.tenant_id, dag).await
    }
}

/// Validate shared input constraints.
fn validate_assets(assets: &[contexide_core::prelude::AssetId]) -> Result<()> {
    if assets.is_empty() {
        return Err(ProfileError::NoAssets.into());
    }
    Ok(())
}

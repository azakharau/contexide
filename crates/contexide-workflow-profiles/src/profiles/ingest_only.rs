use contexide_core::prelude::{AssetId, Result};
use contexide_workflow_core::dag::{Dag, DagEdge, DagNode, TaskKind};

use crate::dto::IngestOnlyInput;
use crate::errors::ProfileError;
use crate::profiles::validate_assets;

/// Build DAG for ingest-only pipeline: Extract -> Normalize -> Chunk per asset.
pub fn build_dag(input: &IngestOnlyInput) -> Result<Dag> {
    validate_assets(&input.asset_ids)?;
    let doc_id = input
        .common
        .document_id
        .ok_or(ProfileError::MissingDocumentId)?;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for asset_id in &input.asset_ids {
        append_linear_chain(asset_id, &mut nodes, &mut edges);
    }

    let dag = Dag {
        name: "ingest_only".to_string(),
        version: 1,
        nodes,
        edges,
    };

    dag.validate()
        .map_err(|e| contexide_core::errors::Error::Other(e.into()))?;

    // currently doc_id is unused in DAG structure; kept for future metadata.
    let _ = doc_id;
    Ok(dag)
}

fn append_linear_chain(asset: &AssetId, nodes: &mut Vec<DagNode>, edges: &mut Vec<DagEdge>) {
    let extract_key = format!("extract:{}", asset.0);
    let normalize_key = format!("normalize:{}", asset.0);
    let chunk_key = format!("chunk:{}", asset.0);

    nodes.push(DagNode {
        key: extract_key.clone(),
        kind: TaskKind::Parse,
        label: Some("extract".into()),
    });
    nodes.push(DagNode {
        key: normalize_key.clone(),
        kind: TaskKind::Normalize,
        label: Some("normalize".into()),
    });
    nodes.push(DagNode {
        key: chunk_key.clone(),
        kind: TaskKind::Chunk,
        label: Some("chunk".into()),
    });

    edges.push(DagEdge {
        from: extract_key,
        to: normalize_key.clone(),
    });
    edges.push(DagEdge {
        from: normalize_key,
        to: chunk_key,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::{AssetId, DocumentId, TenantId};

    #[test]
    fn builds_linear_dag_per_asset() {
        let input = IngestOnlyInput {
            common: crate::dto::WorkflowStartCommon {
                tenant_id: TenantId::new(),
                document_id: Some(DocumentId::new()),
                correlation_id: None,
            },
            asset_ids: vec![AssetId::new(), AssetId::new()],
            language_hint: None,
        };

        let dag = build_dag(&input).expect("dag");
        assert_eq!(dag.nodes.len(), 6);
        assert_eq!(dag.edges.len(), 4);
        assert!(dag.validate().is_ok());
    }
}

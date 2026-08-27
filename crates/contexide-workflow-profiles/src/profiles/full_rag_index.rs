use contexide_core::prelude::{AssetId, Result};
use contexide_workflow_core::dag::{Dag, DagEdge, DagNode, TaskKind};

use crate::dto::FullRagIndexInput;
use crate::errors::ProfileError;
use crate::profiles::validate_assets;

/// Build DAG for full RAG pipeline: Extract -> Normalize -> Chunk -> Embed -> Index.
///
/// Embed/Index are modeled as downstream nodes per asset; dynamic fan-out
/// per chunk_set is expected to be added by the executor when chunker outputs
/// concrete chunk_set_ids.
pub fn build_dag(input: &FullRagIndexInput) -> Result<Dag> {
    validate_assets(&input.asset_ids)?;
    let _doc_id = input
        .common
        .document_id
        .ok_or(ProfileError::MissingDocumentId)?;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for asset_id in &input.asset_ids {
        append_chain(asset_id, &mut nodes, &mut edges);
    }

    let dag = Dag {
        name: "full_rag_index".to_string(),
        version: 1,
        nodes,
        edges,
    };

    dag.validate()
        .map_err(|e| contexide_core::errors::Error::Other(e.into()))?;
    Ok(dag)
}

fn append_chain(asset: &AssetId, nodes: &mut Vec<DagNode>, edges: &mut Vec<DagEdge>) {
    let extract_key = format!("extract:{}", asset.0);
    let normalize_key = format!("normalize:{}", asset.0);
    let chunk_key = format!("chunk:{}", asset.0);
    let embed_key = format!("embed:{}", asset.0);
    let index_key = format!("index:{}", asset.0);

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
    nodes.push(DagNode {
        key: embed_key.clone(),
        kind: TaskKind::Embed,
        label: Some("embed".into()),
    });
    nodes.push(DagNode {
        key: index_key.clone(),
        kind: TaskKind::Index,
        label: Some("index".into()),
    });

    edges.push(DagEdge {
        from: extract_key,
        to: normalize_key.clone(),
    });
    edges.push(DagEdge {
        from: normalize_key,
        to: chunk_key.clone(),
    });
    edges.push(DagEdge {
        from: chunk_key,
        to: embed_key.clone(),
    });
    edges.push(DagEdge {
        from: embed_key,
        to: index_key,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use contexide_core::prelude::{AssetId, DocumentId, TenantId};

    #[test]
    fn builds_full_chain_per_asset() {
        let input = FullRagIndexInput {
            common: crate::dto::WorkflowStartCommon {
                tenant_id: TenantId::new(),
                document_id: Some(DocumentId::new()),
                correlation_id: None,
            },
            asset_ids: vec![AssetId::new()],
            retrieval_profile: None,
            force_rebuild: false,
        };

        let dag = build_dag(&input).expect("dag");
        assert_eq!(dag.nodes.len(), 5);
        assert_eq!(dag.edges.len(), 4);
        assert!(dag.validate().is_ok());
    }
}

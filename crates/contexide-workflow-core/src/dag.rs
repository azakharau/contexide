//! In-memory DAG definition for workflows.
//!
//! This module defines a lightweight representation of a workflow DAG:
//! - `Dag`     — full graph (nodes + edges)
//! - `DagNode` — logical node (task kind + stable key inside DAG)
//! - `DagEdge` — dependency between nodes
//! - `TaskKind` — high-level domain for nodes (ingest, parse, chunk, embed, ...)
//!
//! The `Dag` type is intended for planning and execution logic. It is
//! independent from persistence (Postgres) and transport (NATS).
//!
//! Typical usage:
//! - Planner builds a `Dag` (from template or config).
//! - Executor validates it (`Dag::validate()`).
//! - Executor uses `topo_order()` to drive scheduling decisions.

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// High-level domain kind for a task node.
///
/// These kinds are intentionally coarse-grained and map to families of workers
/// (e.g. "chunker", "embedder"). More specific behavior is configured via
/// task parameters outside of this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// Ingest / registration of a document or assets.
    Ingest,
    /// Parsing / extraction (PDF, HTML, DOCX, OCR, ASR, ...).
    Parse,
    /// Text cleanup and normalization (whitespace, quotes, markup).
    Normalize,
    /// Chunking into passages.
    Chunk,
    /// Embedding generation.
    Embed,
    /// Writing results into a search index or store.
    Index,
}

/// Logical node inside a DAG.
///
/// Nodes are identified within a DAG by a stable string `key`. This key is
/// used in edges (`from`/`to`) and later can be mapped to concrete Task ids
/// when a `DagRun` is instantiated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DagNode {
    /// Stable key inside a DAG, e.g. "ingest", "parse_pdf", "chunk", "embed".
    pub key: String,
    /// High-level functional kind of this node.
    pub kind: TaskKind,
    /// Optional human-friendly label (for UI/logging).
    pub label: Option<String>,
}

/// Directed edge between two nodes (dependency).
///
/// Semantics: `from` must complete successfully before `to` can be scheduled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DagEdge {
    /// Upstream node key.
    pub from: String,
    /// Downstream node key.
    pub to: String,
}

/// Full DAG definition: nodes + directed edges.
///
/// `Dag` is intentionally small and self-contained so it can be stored in
/// JSON, passed over NATS, or reconstructed from templates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dag {
    /// Human-readable name of the workflow (e.g. "default_ingest").
    pub name: String,
    /// Monotonic version for this DAG definition (used for audit/rollout).
    pub version: i32,
    /// All nodes participating in the DAG.
    pub nodes: Vec<DagNode>,
    /// Directed edges between nodes.
    pub edges: Vec<DagEdge>,
}

/// Validation errors for DAG structure.
#[derive(Debug, Error)]
pub enum DagValidationError {
    #[error("duplicate node key: {0}")]
    DuplicateNodeKey(String),

    #[error("edge references missing node: {from} -> {to}")]
    MissingNode { from: String, to: String },

    #[error("cycle detected in DAG")]
    Cycle,
}

impl Dag {
    /// Build a mapping from node key to index in `nodes` array,
    /// while checking for duplicates.
    fn build_index(&self) -> Result<HashMap<&str, usize>, DagValidationError> {
        let mut idx = HashMap::with_capacity(self.nodes.len());
        for (i, node) in self.nodes.iter().enumerate() {
            if let Some(prev) = idx.insert(node.key.as_str(), i) {
                // A duplicate key is a fatal structural error.
                let _ = prev; // keep compiler happy if unused
                return Err(DagValidationError::DuplicateNodeKey(node.key.clone()));
            }
        }
        Ok(idx)
    }

    /// Validate that:
    /// - all edge endpoints exist
    /// - graph is acyclic
    ///
    /// Does **not** check any business-specific constraints (like "must have
    /// at least one ingest node"). Those belong to higher-level policies.
    pub fn validate(&self) -> Result<(), DagValidationError> {
        // Empty DAG is technically valid (no work to do).
        if self.nodes.is_empty() {
            return Ok(());
        }

        let index = self.build_index()?;

        // Build adjacency list and indegree counts for Kahn's algorithm.
        let n = self.nodes.len();
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut indegree: Vec<usize> = vec![0; n];

        for edge in &self.edges {
            let from_ix =
                index
                    .get(edge.from.as_str())
                    .ok_or_else(|| DagValidationError::MissingNode {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                    })?;
            let to_ix =
                index
                    .get(edge.to.as_str())
                    .ok_or_else(|| DagValidationError::MissingNode {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                    })?;

            adj[*from_ix].push(*to_ix);
            indegree[*to_ix] += 1;
        }

        // Kahn's algorithm for cycle detection (topological sort).
        let mut q = VecDeque::new();
        for (i, &deg) in indegree.iter().enumerate() {
            if deg == 0 {
                q.push_back(i);
            }
        }

        let mut seen: usize = 0;

        while let Some(u) = q.pop_front() {
            seen += 1;
            for &v in &adj[u] {
                indegree[v] -= 1;
                if indegree[v] == 0 {
                    q.push_back(v);
                }
            }
        }

        if seen != n {
            // Not all nodes were visited -> cycle exists.
            return Err(DagValidationError::Cycle);
        }

        Ok(())
    }

    /// Return node keys in topological order (upstream → downstream).
    ///
    /// Useful for planning or debugging. Fails if DAG is invalid or cyclic.
    pub fn topo_order(&self) -> Result<Vec<String>, DagValidationError> {
        if self.nodes.is_empty() {
            return Ok(Vec::new());
        }

        let index = self.build_index()?;
        let n = self.nodes.len();
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut indegree: Vec<usize> = vec![0; n];

        for edge in &self.edges {
            let from_ix =
                index
                    .get(edge.from.as_str())
                    .ok_or_else(|| DagValidationError::MissingNode {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                    })?;
            let to_ix =
                index
                    .get(edge.to.as_str())
                    .ok_or_else(|| DagValidationError::MissingNode {
                        from: edge.from.clone(),
                        to: edge.to.clone(),
                    })?;

            adj[*from_ix].push(*to_ix);
            indegree[*to_ix] += 1;
        }

        let mut q = VecDeque::new();
        for (i, &deg) in indegree.iter().enumerate() {
            if deg == 0 {
                q.push_back(i);
            }
        }

        let mut order_ix = Vec::with_capacity(n);

        while let Some(u) = q.pop_front() {
            order_ix.push(u);
            for &v in &adj[u] {
                indegree[v] -= 1;
                if indegree[v] == 0 {
                    q.push_back(v);
                }
            }
        }

        if order_ix.len() != n {
            return Err(DagValidationError::Cycle);
        }

        // Map indices back to node keys (cloned).
        let mut result = Vec::with_capacity(n);
        for ix in order_ix {
            result.push(self.nodes[ix].key.clone());
        }
        Ok(result)
    }

    /// Return all nodes that have no incoming edges (possible entry points).
    pub fn roots(&self) -> Vec<&DagNode> {
        if self.nodes.is_empty() {
            return Vec::new();
        }

        let index = match self.build_index() {
            Ok(idx) => idx,
            Err(_) => return Vec::new(),
        };

        let mut has_incoming: Vec<bool> = vec![false; self.nodes.len()];
        for edge in &self.edges {
            if let Some(&to_ix) = index.get(edge.to.as_str()) {
                has_incoming[to_ix] = true;
            }
        }

        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(i, node)| if !has_incoming[i] { Some(node) } else { None })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_node(key: &str, kind: TaskKind) -> DagNode {
        DagNode {
            key: key.to_string(),
            kind,
            label: None,
        }
    }

    #[test]
    fn simple_valid_dag() {
        let dag = Dag {
            name: "simple".to_string(),
            version: 1,
            nodes: vec![
                mk_node("ingest", TaskKind::Ingest),
                mk_node("parse", TaskKind::Parse),
                mk_node("chunk", TaskKind::Chunk),
            ],
            edges: vec![
                DagEdge {
                    from: "ingest".into(),
                    to: "parse".into(),
                },
                DagEdge {
                    from: "parse".into(),
                    to: "chunk".into(),
                },
            ],
        };

        dag.validate().unwrap();
        let order = dag.topo_order().unwrap();
        // One of valid topological orders is exactly this chain.
        assert_eq!(order, vec!["ingest", "parse", "chunk"]);
        let roots = dag.roots();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].key, "ingest");
    }

    #[test]
    fn detects_duplicate_keys() {
        let dag = Dag {
            name: "dup".into(),
            version: 1,
            nodes: vec![
                mk_node("ingest", TaskKind::Ingest),
                mk_node("ingest", TaskKind::Ingest),
            ],
            edges: vec![],
        };

        let err = dag.validate().unwrap_err();
        matches!(err, DagValidationError::DuplicateNodeKey(_));
    }

    #[test]
    fn detects_missing_node_in_edge() {
        let dag = Dag {
            name: "missing".into(),
            version: 1,
            nodes: vec![mk_node("ingest", TaskKind::Ingest)],
            edges: vec![DagEdge {
                from: "ingest".into(),
                to: "parse".into(),
            }],
        };

        let err = dag.validate().unwrap_err();
        matches!(err, DagValidationError::MissingNode { .. });
    }

    #[test]
    fn detects_cycle() {
        let dag = Dag {
            name: "cycle".into(),
            version: 1,
            nodes: vec![
                mk_node("a", TaskKind::Ingest),
                mk_node("b", TaskKind::Parse),
            ],
            edges: vec![
                DagEdge {
                    from: "a".into(),
                    to: "b".into(),
                },
                DagEdge {
                    from: "b".into(),
                    to: "a".into(),
                },
            ],
        };

        let err = dag.validate().unwrap_err();
        matches!(err, DagValidationError::Cycle);
    }
}

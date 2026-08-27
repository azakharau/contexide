//! Domain worker binaries for the workflow data plane.
//!
//! This crate wires `contexide-worker-runtime` with minimal domain-specific
//! handlers for the extractor / normalizer / chunker / embedder / indexer
//! workers. The handlers are intentionally lightweight placeholders; they
//! validate payload shape superficially and emit `WorkerStatus` messages so
//! the control plane can advance the DAG. Real domain logic should be
//! plugged in behind these handlers over time.

pub mod bootstrap;
pub mod config;
pub mod handlers;

pub use config::WorkerAppConfig;

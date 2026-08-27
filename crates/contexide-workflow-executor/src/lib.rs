//! Workflow control plane for `contexide`.
//!
//! This crate ties together:
//! - `contexide-workflow-core` for domain models and status enums.
//! - `contexide-storage` for persisting DagRuns, Tasks and TaskRuns in Postgres (or in-memory for tests).
//! - `contexide-messaging` for message shapes sent over NATS JetStream (transport provided by caller).
//!
//! Workers form the data plane (extract/normalize/chunk/embed/index). The executor
//! runs the control plane: plan runs, schedule tasks, react to worker results, and
//! publish/consume workflow messages.

pub mod config;
pub mod domain;
pub mod messaging;
pub mod planner;
pub mod retry_logic;
pub mod runtime;
pub mod scheduler;
pub mod storage;

pub use runtime::WorkflowExecutor;

//! Worker runtime for long-running workflow workers (data plane).
//!
//! This crate provides a generic event loop over NATS JetStream that
//! receives `WorkerRequest` messages and dispatches them to a domain
//! handler, then sends back `WorkerStatus` on the corresponding `*.done`
//! subject.
//!
//! It is domain-agnostic: each worker binary plugs in its own
//! `WorkerHandler` implementation.

pub mod config;
pub mod context;
pub mod runner;
pub mod signals;
pub mod traits;

pub use crate::config::WorkerRuntimeConfig;
pub use crate::context::WorkerContext;
pub use crate::runner::{WorkerRunner, WorkerRunnerBuilder};
pub use crate::traits::{DynWorkerHandler, WorkerHandler};

//! Small helpers shared by worker binaries.

use anyhow::Result;
use contexide_worker_runtime::{WorkerRunnerBuilder, WorkerRuntimeConfig};
use tracing_subscriber::{EnvFilter, fmt};

use crate::{WorkerAppConfig, handlers};

/// Initialize `tracing` subscriber with env filter fallback.
pub fn init_tracing() {
    // Keep logging setup simple and consistent across all workers.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt().with_env_filter(env_filter).with_target(true).init();
}

/// Build and run a worker for a given kind using shared defaults.
pub async fn run_worker(worker_kind: &str) -> Result<()> {
    let app_cfg = WorkerAppConfig::load()?;
    let rt_cfg: WorkerRuntimeConfig = app_cfg.runtime_config(worker_kind);
    let handler = handlers::handler_for(worker_kind);

    let runner = WorkerRunnerBuilder::new(rt_cfg, handler).build().await?;
    runner.run().await?;
    Ok(())
}

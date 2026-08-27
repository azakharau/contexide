//! Normalizer worker binary.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    contexide_workers::bootstrap::init_tracing();
    contexide_workers::bootstrap::run_worker("normalizer").await
}

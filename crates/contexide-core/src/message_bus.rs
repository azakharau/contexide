//! Transport-agnostic message bus abstraction for the workflow/executor.

use crate::errors::Result;

#[async_trait::async_trait]
pub trait MessageBus: Send + Sync {
    async fn publish(&self, subject: &str, payload: &[u8]) -> Result<()>;
    async fn subscribe(
        &self,
        subject: &str,
    ) -> Result<futures::stream::BoxStream<'static, Result<IncomingMessage>>>;
}

/// Minimal incoming message shape (payload + subject).
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub subject: String,
    pub data: Vec<u8>,
}

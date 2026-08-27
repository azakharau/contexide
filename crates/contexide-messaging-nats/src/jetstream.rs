//! Thin NATS/JetStream wrapper used by executor and workers.
//!
//! This MVP uses plain NATS subjects for publish/subscribe while keeping the
//! API surface small and mockable. ACK/NACK are no-ops for plain NATS but are
//! provided to keep the interface stable if we switch to real JetStream
//! consumers later.

use std::sync::Arc;

use async_nats::{Client, Message};
use contexide_config::MessagingConfig;
use contexide_core::errors::{Error, Result};
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;

type Published = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// Connect to NATS using messaging config.
pub async fn connect_jetstream(cfg: &MessagingConfig) -> Result<JetStreamClient> {
    let client = async_nats::connect(cfg.nats_url.clone())
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!(e)))?;
    Ok(JetStreamClient {
        inner: Arc::new(JetStreamInner::Real { client }),
    })
}

/// Wrapper over subscription message.
#[derive(Clone)]
pub struct JetStreamMessage {
    inner: JetStreamMessageInner,
}

#[derive(Clone)]
enum JetStreamMessageInner {
    Real(Message),
    Mock { subject: String, data: Vec<u8> },
}

impl JetStreamMessage {
    pub fn subject(&self) -> &str {
        match &self.inner {
            JetStreamMessageInner::Real(m) => &m.subject,
            JetStreamMessageInner::Mock { subject, .. } => subject,
        }
    }

    pub fn data(&self) -> &[u8] {
        match &self.inner {
            JetStreamMessageInner::Real(m) => &m.payload,
            JetStreamMessageInner::Mock { data, .. } => data,
        }
    }

    pub async fn ack(&self) -> Result<()> {
        // Plain NATS has no ack; noop for now.
        let _ = &self.inner;
        Ok(())
    }

    pub async fn nack(&self) -> Result<()> {
        // Plain NATS has no nack; noop for now.
        let _ = &self.inner;
        Ok(())
    }
}

#[derive(Clone)]
pub struct JetStreamClient {
    inner: Arc<JetStreamInner>,
}

#[derive(Clone)]
enum JetStreamInner {
    Real { client: Client },
    Mock(MockJetStream),
}

impl JetStreamClient {
    /// Publish a message to a subject.
    pub async fn publish(&self, subject: &str, payload: &[u8]) -> Result<()> {
        match &*self.inner {
            JetStreamInner::Real { client } => {
                client
                    .publish(subject.to_string(), payload.to_vec().into())
                    .await
                    .map_err(|e| Error::Other(anyhow::anyhow!(e)))?;
            }
            JetStreamInner::Mock(mock) => {
                mock.published
                    .lock()
                    .await
                    .push((subject.to_string(), payload.to_vec()));
            }
        }
        Ok(())
    }

    /// Subscribe to a subject, returning a stream of messages.
    pub async fn subscribe(
        &self,
        subject: &str,
    ) -> Result<BoxStream<'static, Result<JetStreamMessage>>> {
        match &*self.inner {
            JetStreamInner::Real { client } => {
                let sub = client
                    .subscribe(subject.to_string())
                    .await
                    .map_err(|e| Error::Other(anyhow::anyhow!(e)))?;
                let stream = sub
                    .map(|m| {
                        Ok(JetStreamMessage {
                            inner: JetStreamMessageInner::Real(m),
                        })
                    })
                    .boxed();
                Ok(stream)
            }
            JetStreamInner::Mock(mock) => {
                let rx = mock
                    .receiver
                    .lock()
                    .await
                    .take()
                    .expect("mock stream already taken");
                let stream = ReceiverStream::new(rx).map(Ok).boxed();
                Ok(stream)
            }
        }
    }
}

#[derive(Clone)]
pub struct MockJetStream {
    pub published: Published,
    receiver: Arc<Mutex<Option<tokio::sync::mpsc::Receiver<JetStreamMessage>>>>,
}

pub fn mock_jetstream(
    requests: Vec<contexide_core::messaging::WorkerRequest>,
    subject: &str,
) -> (JetStreamClient, Published) {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    for req in requests {
        let _ = tx.try_send(JetStreamMessage {
            inner: JetStreamMessageInner::Mock {
                subject: subject.to_string(),
                data: serde_json::to_vec(&req).unwrap(),
            },
        });
    }
    let published: Published = Arc::new(Mutex::new(Vec::new()));
    let mock = MockJetStream {
        published: published.clone(),
        receiver: Arc::new(Mutex::new(Some(rx))),
    };
    (
        JetStreamClient {
            inner: Arc::new(JetStreamInner::Mock(mock)),
        },
        published,
    )
}

pub fn mock_jetstream_with_shutdown(
    requests: Vec<contexide_core::messaging::WorkerRequest>,
    subject: &str,
) -> (
    JetStreamClient,
    Published,
    tokio::sync::mpsc::Sender<()>,
    tokio::sync::mpsc::Receiver<()>,
) {
    let (js, published) = mock_jetstream(requests, subject);
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    (js, published, tx, rx)
}

//! `contexide-messaging-nats` — JetStream adapter + re-export of messaging contracts from `contexide-core`.

mod jetstream;

pub use crate::jetstream::{JetStreamClient, JetStreamMessage, connect_jetstream};
pub use contexide_core::messaging::*;

pub use crate::jetstream::{mock_jetstream, mock_jetstream_with_shutdown};

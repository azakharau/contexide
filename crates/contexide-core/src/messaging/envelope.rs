//! Generic message envelope.
//!
//! The envelope carries:
//! - `meta`   — transport-agnostic metadata (ids, tenant, timestamps, schema).
//! - `payload` — arbitrary domain payload (`T`).
//!
//! The idea is that transports (NATS, Kafka, HTTP) only know about envelopes,
//! not about concrete domain types. Domain code works with strongly-typed
//! payloads parameterized via `T`.

use crate::prelude::TenantId;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// Common metadata shared across all message types.
///
/// This is intentionally small and stable; domain-specific fields live
/// in the `payload` part of the envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMeta {
    /// Unique identifier of this message (UUIDv7 for better index locality).
    pub message_id: Uuid,

    /// Optional correlation id used to group related messages.
    ///
    /// Typical mapping:
    /// - workflow domain: `dag_run_id` or similar.
    pub correlation_id: Option<Uuid>,

    /// Optional causation id (the message that caused this one).
    ///
    /// Useful for tracking chains like:
    ///   command -> task-request -> task-done -> event
    pub causation_id: Option<Uuid>,

    /// Optional tenant scope of the message.
    pub tenant_id: Option<TenantId>,

    /// Schema / version of the *payload*.
    ///
    /// This is a small number you can bump when changing the JSON shape
    /// of a particular payload type, while keeping backward compatibility
    /// logic on the consumer side.
    pub schema_version: u16,

    /// Timestamp when the message was created (UTC).
    ///
    /// Stored as RFC3339 in JSON.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl MessageMeta {
    /// Create fresh metadata with a new UUIDv7 and current UTC time.
    ///
    /// `schema_version` is passed explicitly so payload types can decide
    /// on their own versioning scheme.
    pub fn new(schema_version: u16) -> Self {
        Self {
            message_id: Uuid::now_v7(),
            correlation_id: None,
            causation_id: None,
            tenant_id: None,
            schema_version,
            created_at: OffsetDateTime::now_utc(),
        }
    }

    /// Attach a tenant id.
    pub fn with_tenant(mut self, tenant: TenantId) -> Self {
        self.tenant_id = Some(tenant);
        self
    }

    /// Attach a correlation id.
    pub fn with_correlation(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Attach a causation id.
    pub fn with_causation(mut self, causation_id: Uuid) -> Self {
        self.causation_id = Some(causation_id);
        self
    }
}

/// Generic envelope for any serializable domain payload.
///
/// `T` is supposed to be a small, `serde`-friendly type (struct / enum).
/// The envelope can be serialized as JSON and published via any transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    /// Transport-agnostic metadata (ids, tenant, timestamps, schema).
    pub meta: MessageMeta,
    /// Strongly-typed domain payload.
    pub payload: T,
}

impl<T> Envelope<T> {
    /// Create a new envelope with default metadata (new id, now, no tenant,
    /// no correlation/causation) and provided `schema_version`.
    pub fn new(payload: T, schema_version: u16) -> Self {
        Self {
            meta: MessageMeta::new(schema_version),
            payload,
        }
    }

    /// Map the inner payload while preserving metadata.
    ///
    /// Useful when converting from generic to more specific types (or vice versa)
    /// without losing correlation/causation information.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Envelope<U> {
        Envelope {
            meta: self.meta,
            payload: f(self.payload),
        }
    }

    /// Attach tenant information to the envelope.
    pub fn with_tenant(mut self, tenant: TenantId) -> Self {
        self.meta.tenant_id = Some(tenant);
        self
    }

    /// Attach a correlation id to the envelope.
    pub fn with_correlation(mut self, correlation_id: Uuid) -> Self {
        self.meta.correlation_id = Some(correlation_id);
        self
    }

    /// Attach a causation id to the envelope.
    pub fn with_causation(mut self, causation_id: Uuid) -> Self {
        self.meta.causation_id = Some(causation_id);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn json_roundtrip_works() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        struct Payload {
            msg: String,
        }

        let tenant = TenantId::new();
        let payload = Payload {
            msg: "hello".to_string(),
        };

        let env = Envelope::new(payload, 1)
            .with_tenant(tenant)
            .with_correlation(Uuid::now_v7());

        let json = serde_json::to_string(&env).expect("serialize");
        let back: Envelope<Payload> = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back.payload.msg, "hello");
        assert_eq!(back.meta.schema_version, 1);
        assert_eq!(back.meta.tenant_id, Some(tenant));
        assert!(back.meta.correlation_id.is_some());
    }
}

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{JobId, Stage, TenantId};

pub mod payloads;
pub mod subjects;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<T> {
    pub job_id: JobId,
    pub tenant_id: TenantId,
    pub stage: Stage,
    pub idempotency_key: String,
    pub payload: T,
    pub emitted_at: OffsetDateTime,
}

impl<T> Event<T> {
    pub fn new(
        job_id: JobId,
        tenant_id: TenantId,
        stage: Stage,
        idempotency_key: impl Into<String>,
        payload: T,
    ) -> Self {
        Self {
            job_id,
            tenant_id,
            stage,
            idempotency_key: idempotency_key.into(),
            payload,
            emitted_at: OffsetDateTime::now_utc(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JobId, Stage, TenantId};
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn event_roundtrip_with_time() {
        let ev = Event::new(
            JobId(Uuid::nil()),
            TenantId(Uuid::nil()),
            Stage::Extract,
            "extract:abc",
            json!({"ok": true}),
        );
        let s = serde_json::to_string(&ev).unwrap();
        let back: Event<serde_json::Value> = serde_json::from_str(&s).unwrap();
        assert_eq!(back.stage, Stage::Extract);
        assert!(back.emitted_at <= OffsetDateTime::now_utc() + time::Duration::seconds(5));
    }
}

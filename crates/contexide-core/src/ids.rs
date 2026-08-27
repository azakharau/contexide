//! `ids` — type-safe domain identifiers over `Uuid`.
//!
//! Why:
//! - Strong types (`DocumentId`, `AssetId`, …) instead of bare `Uuid` in signatures.
//! - Impossible to mix up IDs from different domains.
//! - Use **UUIDv7** (time-ordered) for better DB index locality.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[macro_export]
macro_rules! newtype_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub Uuid);

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            #[inline]
            pub fn new() -> Self { Self(Uuid::now_v7()) }
        }

        impl From<Uuid> for $name { fn from(u: Uuid) -> Self { Self(u) } }
        impl From<$name> for Uuid { fn from(id: $name) -> Self { id.0 } }

        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
                <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
            }
            fn compatible(ty: &<sqlx::Postgres as sqlx::Database>::TypeInfo) -> bool {
                <Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        impl<'r> sqlx::Decode<'r, sqlx::Postgres> for $name {
            fn decode(
                value: sqlx::postgres::PgValueRef<'r>
            ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
                <Uuid as sqlx::Decode<sqlx::Postgres>>::decode(value).map(Self)
            }
        }

        impl<'q> sqlx::Encode<'q, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer
            ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync + 'static>> {
                <Uuid as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
            }
            fn size_hint(&self) -> usize {
                <Uuid as sqlx::Encode<sqlx::Postgres>>::size_hint(&self.0)
            }
        }
    };
}

newtype_id!(TenantId);
newtype_id!(DocumentId);
newtype_id!(AssetId);
newtype_id!(BlockId);
newtype_id!(ChunkSetId);
newtype_id!(ChunkId);
newtype_id!(EmbeddingSetId);
newtype_id!(JobId);
newtype_id!(/// DAG definition identifier
DagId);

newtype_id!(/// Concrete DAG run identifier
DagRunId);

newtype_id!(/// Logical task identifier within a DAG run
TaskId);

newtype_id!(/// Concrete task attempt (run) identifier
TaskRunId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v7_generation_works() {
        let a = DocumentId::new();
        let b = DocumentId::new();
        assert_ne!(a.0, b.0);
    }

    #[test]
    fn roundtrip_uuid() {
        let u = Uuid::now_v7();
        let id = DocumentId::from(u);
        let back: Uuid = id.into();
        assert_eq!(u, back);
    }
}

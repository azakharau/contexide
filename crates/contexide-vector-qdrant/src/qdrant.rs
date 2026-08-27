//! Qdrant-backed implementation for `contexide-vector-qdrant`.
//!
//! Target: qdrant-client = "1.15"
//! - Uses official builder-style API (`create_collection`, `upsert_points`, `search`, `count`, `delete_points`).
//! - UUID point ids are stored as Qdrant `PointId::Uuid(String)`.
//! - Simple payload passthrough (HashMap<String, Value>) -> serde_json via `into_json()`.

#![allow(clippy::needless_lifetimes)]

use std::collections::HashMap;

use anyhow::Context;
use qdrant_client::qdrant::r#match::MatchValue;
use qdrant_client::qdrant::{
    Condition, CountPointsBuilder, CreateCollectionBuilder, DeleteCollectionBuilder,
    DeletePointsBuilder, Distance, Filter, ListValue, PointId, PointStruct, PointsIdsList,
    SearchPointsBuilder, Struct, UpsertPointsBuilder, Value, VectorParams, VectorsConfig, point_id,
    value, vectors_config,
};
use qdrant_client::{Qdrant, config::QdrantConfig};
use serde_json::{Map as JsonMap, Value as JsonValue};
use uuid::Uuid;

use contexide_core::errors::{Error, Result};

/// Public DTO для результата поиска
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: Uuid,
    pub score: f32,
    pub payload: Option<JsonMap<String, JsonValue>>,
}

#[derive(Clone)]
pub struct QdrantStore {
    client: Qdrant,
    prefix: String,
    dim: u64,
    /// "Cosine" | "Dot" | "Euclid"
    distance: Distance,
}

impl QdrantStore {
    pub async fn new(
        endpoint: &str,
        api_key: Option<String>,
        prefix: String,
        dim: u64,
        distance: Distance,
    ) -> Result<Self> {
        let mut cfg = QdrantConfig::from_url(endpoint);

        if let Some(key) = api_key {
            cfg.set_api_key(key.as_str());
        }

        let client = cfg
            .build()
            .with_context(|| format!("create Qdrant client for {endpoint}"))?;

        Ok(Self {
            client,
            prefix,
            dim,
            distance,
        })
    }

    #[inline]
    fn coll(&self, name: &str) -> String {
        format!("{}__{}", self.prefix, name)
    }

    /// ensure: создаёт коллекцию если её нет; если уже есть — no-op
    pub async fn ensure_collection(&self, name: &str) -> Result<()> {
        let collection = self.coll(name);

        // Попробуем получить метаданные — если 404, создаём
        let exists = self
            .client
            .collection_exists(&collection)
            .await
            .or_else(|e| if is_not_found(&e) { Ok(false) } else { Err(e) })
            .with_context(|| format!("get_collection {collection}"))?;

        if exists {
            return Ok(());
        }

        let vectors_config = VectorsConfig {
            config: Some(vectors_config::Config::Params(VectorParams {
                size: self.dim,
                distance: self.distance as i32,
                ..Default::default()
            })),
        };

        self.client
            .create_collection(
                CreateCollectionBuilder::new(collection.clone()).vectors_config(vectors_config),
            )
            .await
            .with_context(|| format!("create_collection {collection}"))?;

        Ok(())
    }

    pub async fn drop_collection(&self, name: &str) -> Result<()> {
        let collection = self.coll(name);
        self.client
            .delete_collection(DeleteCollectionBuilder::new(collection.clone()))
            .await
            .with_context(|| format!("delete_collection {collection}"))?;
        Ok(())
    }

    /// Upsert пачки точек (id, vector, payload)
    pub async fn upsert_points(
        &self,
        name: &str,
        points: impl IntoIterator<Item = (Uuid, Vec<f32>, Option<JsonMap<String, JsonValue>>)>,
    ) -> Result<()> {
        let collection = self.coll(name);

        let pts: Vec<PointStruct> = points
            .into_iter()
            .map(|(id, vec, payload)| {
                // payload: serde_json::Map -> qdrant payload (HashMap<String, Value>)
                let payload_q: Option<HashMap<String, Value>> = payload.map(|m| {
                    m.into_iter()
                        .map(|(k, v)| (k, json_to_qdrant_value(v)))
                        .collect()
                });

                match payload_q {
                    Some(payload) => PointStruct::new(id.to_string(), vec, payload),
                    None => PointStruct::new(id.to_string(), vec, HashMap::<String, Value>::new()),
                }
            })
            .collect();

        self.client
            .upsert_points(UpsertPointsBuilder::new(collection.clone(), pts))
            .await
            .with_context(|| format!("upsert_points {collection}"))?;

        Ok(())
    }

    /// Поиск ближайших соседей с опциональным фильтром по payload.
    pub async fn search(
        &self,
        name: &str,
        query: &[f32],
        limit: u64,
        cond_equal: Option<Vec<(&str, JsonValue)>>,
    ) -> Result<Vec<SearchHit>> {
        let collection = self.coll(name);

        let filter = cond_equal.and_then(to_filter);

        let mut builder = SearchPointsBuilder::new(collection, query.to_vec(), limit);
        if let Some(f) = filter {
            builder = builder.filter(f);
        }

        let out = self
            .client
            .search_points(builder)
            .await
            .with_context(|| "search_points failed")?;

        let hits = out
            .result
            .into_iter()
            .filter_map(|sp| {
                // id
                let id = match point_id_to_uuid(sp.id) {
                    Some(u) => u,
                    None => return None, // в MVP пропускаем не-UUID
                };

                // payload -> json map
                let payload_json = if sp.payload.is_empty() {
                    None
                } else {
                    let map = sp
                        .payload
                        .into_iter()
                        .map(|(k, v)| (k, v.into_json()))
                        .collect::<JsonMap<_, _>>();
                    Some(map)
                };

                Some(SearchHit {
                    id,
                    score: sp.score,
                    payload: payload_json,
                })
            })
            .collect();

        Ok(hits)
    }

    /// Удаление по списку UUID ids.
    pub async fn delete_by_ids(&self, name: &str, ids: &[Uuid]) -> Result<u64> {
        let collection = self.coll(name);

        let ids_list = PointsIdsList {
            ids: ids
                .iter()
                .cloned()
                .map(|u| PointId::from(u.to_string()))
                .collect(),
        };

        let res = self
            .client
            .delete_points(DeletePointsBuilder::new(collection).points(ids_list))
            .await
            .with_context(|| "delete_points failed")?;

        if let Some(res) = res.result
            && let Some(op_id) = res.operation_id
        {
            return Ok(op_id);
        }
        Err(Error::Other(anyhow::anyhow!(
            "delete_points: missing operation_id"
        )))
    }

    /// Подсчёт точек под фильтром (или всех, если `None`).
    pub async fn count(
        &self,
        name: &str,
        cond_equal: Option<Vec<(&str, JsonValue)>>,
    ) -> Result<u64> {
        let collection = self.coll(name);

        let filter = cond_equal.and_then(|pairs| to_filter(pairs));

        let mut builder = CountPointsBuilder::new(collection.clone());
        if let Some(f) = filter {
            builder = builder.filter(f);
        }

        let out = self
            .client
            .count(builder)
            .await
            .with_context(|| "count failed")?;

        Ok(out.result.unwrap_or_default().count)
    }
}

/* ===== helpers ===== */

fn is_not_found(e: &qdrant_client::QdrantError) -> bool {
    // В SDK ошибки обёрнуты; NotFound распознаётся по сообщению/статусу.
    let s = e.to_string().to_lowercase();
    s.contains("not found") || s.contains("404")
}

/// serde_json::Value -> qdrant::Value (простые типы MVP).
fn json_to_qdrant_value(v: JsonValue) -> Value {
    match v {
        JsonValue::Null => Value {
            kind: Some(value::Kind::NullValue(0)),
        },
        JsonValue::Bool(b) => Value {
            kind: Some(value::Kind::BoolValue(b)),
        },
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value {
                    kind: Some(value::Kind::IntegerValue(i)),
                }
            } else if let Some(f) = n.as_f64() {
                Value {
                    kind: Some(value::Kind::DoubleValue(f)),
                }
            } else {
                Value {
                    kind: Some(value::Kind::NullValue(0)),
                }
            }
        }
        JsonValue::String(s) => Value {
            kind: Some(value::Kind::StringValue(s)),
        },
        JsonValue::Array(arr) => {
            // массив строк/чисел -> как строковый массив (MVP)
            let strings: Vec<String> = arr.into_iter().map(|x| x.to_string()).collect();
            Value {
                kind: Some(value::Kind::ListValue(ListValue {
                    values: strings
                        .into_iter()
                        .map(|s| Value {
                            kind: Some(value::Kind::StringValue(s)),
                        })
                        .collect(),
                })),
            }
        }
        JsonValue::Object(obj) => {
            let map = obj
                .into_iter()
                .map(|(k, v)| (k, json_to_qdrant_value(v)))
                .collect::<HashMap<_, _>>();
            Value {
                kind: Some(value::Kind::StructValue(Struct { fields: map })),
            }
        }
    }
}

/// Пары ("field", json) -> Filter::all([matches(...) ...])
fn to_filter(pairs: Vec<(&str, JsonValue)>) -> Option<Filter> {
    let mut conditions = Vec::new();
    for (k, v) in pairs {
        if let Some(mv) = to_match_value(&v) {
            conditions.push(Condition::matches(k.to_string(), mv));
        }
    }
    if conditions.is_empty() {
        None
    } else {
        Some(Filter::all(conditions))
    }
}

fn to_match_value(v: &JsonValue) -> Option<MatchValue> {
    match v {
        JsonValue::String(s) => Some(MatchValue::from(s.clone())),
        JsonValue::Bool(b) => Some(MatchValue::from(*b)),
        JsonValue::Number(n) => n.as_i64().map(MatchValue::from),
        JsonValue::Array(arr) => {
            // попробуем массив строк
            let mut strings = Vec::new();
            let mut ints = Vec::new();
            let mut all_str = true;
            let mut all_int = true;
            for x in arr {
                match x {
                    JsonValue::String(s) => strings.push(s.clone()),
                    JsonValue::Number(n) if n.is_i64() => ints.push(n.as_i64().unwrap()),
                    _ => {
                        all_str = false;
                        all_int = false;
                    }
                }
            }
            if all_str {
                Some(MatchValue::from(strings))
            } else if all_int {
                Some(MatchValue::from(ints))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Достаём UUID из PointIdOptions::Uuid(..). Иначе возвращаем None (MVP).
fn point_id_to_uuid(id: Option<PointId>) -> Option<Uuid> {
    let id = id?;
    match id.point_id_options? {
        point_id::PointIdOptions::Uuid(s) => Uuid::parse_str(&s).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn match_value_basic() {
        assert!(to_match_value(&JsonValue::String("x".into())).is_some());
        assert!(to_match_value(&JsonValue::Bool(true)).is_some());
        assert!(to_match_value(&JsonValue::Number(42.into())).is_some());
    }
}

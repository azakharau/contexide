//! `idempo` — idempotency key builders for pipeline stages.
//!
//! Why:
//! - Workers may see the same input multiple times (at-least-once delivery).
//! - A deterministic **idempotency key** lets us upsert/ignore duplicates safely.
//!
//! Format:
//! - `"stage:part1:part2:..."`
//! - All non-ASCII-safe chars are **sanitized** to `-` (allowed: `a-z0-9._-`).
//! - UUIDs are used in their canonical string form (8-4-4-4-12).
//!
//! Examples:
//! - `fetch:<asset_id>:1.2.3`
//! - `extract:<asset_id>:pdf:1.0.0`
//! - `clean:<asset_id>:<profile_hash>`
//! - `chunk:<chunk_set_id>:<profile_hash>`
//! - `embed:<embedding_set_id>:bge-large-1024`
//! - `persist:<document_id>:<chunk_set_id>:<embedding_set_id>`

use crate::ids::{AssetId, ChunkSetId, DocumentId, EmbeddingSetId};
use crate::traits::ExtractorKind;

/// Sanitize a piece to the safe subset `a-z0-9._-` and lowercase it.
/// Anything else (whitespace, slashes, Unicode, colons, etc.) becomes `-`.
fn sanitize_token(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let c = ch.to_ascii_lowercase();
        match c {
            'a'..='z' | '0'..='9' | '.' | '_' | '-' => out.push(c),
            _ => out.push('-'),
        }
    }
    out
}

/// Join a stage name with sanitized parts using `:` separators.
fn join(stage: &str, parts: &[&str]) -> String {
    let mut key =
        String::with_capacity(stage.len() + parts.iter().map(|p| p.len() + 1).sum::<usize>());
    key.push_str(stage);
    for p in parts {
        key.push(':');
        key.push_str(&sanitize_token(p));
    }
    key
}

/// `fetch:{asset_id}:{worker_version}`
#[inline]
pub fn fetch(asset_id: AssetId, worker_version: &str) -> String {
    join("fetch", &[&asset_id.0.to_string(), worker_version])
}

/// `extract:{asset_id}:{kind}:{version}`
#[inline]
pub fn extract(asset_id: AssetId, kind: ExtractorKind, version: &str) -> String {
    join(
        "extract",
        &[&asset_id.0.to_string(), kind.as_str(), version],
    )
}

/// `clean:{asset_id}:{profile_hash}`
#[inline]
pub fn clean(asset_id: AssetId, profile_hash: &str) -> String {
    join("clean", &[&asset_id.0.to_string(), profile_hash])
}

/// `chunk:{chunk_set_id}:{profile_hash}`
#[inline]
pub fn chunk(chunk_set_id: ChunkSetId, profile_hash: &str) -> String {
    join("chunk", &[&chunk_set_id.0.to_string(), profile_hash])
}

/// `embed:{embedding_set_id}:{model_id}`
#[inline]
pub fn embed(embedding_set_id: EmbeddingSetId, model_id: &str) -> String {
    join("embed", &[&embedding_set_id.0.to_string(), model_id])
}

/// `persist:{document_id}:{chunk_set_id}:{embedding_set_id}`
#[inline]
pub fn persist(
    document_id: DocumentId,
    chunk_set_id: ChunkSetId,
    embedding_set_id: EmbeddingSetId,
) -> String {
    join(
        "persist",
        &[
            &document_id.0.to_string(),
            &chunk_set_id.0.to_string(),
            &embedding_set_id.0.to_string(),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn stable_and_human_readable() {
        let aid = AssetId(Uuid::nil());
        assert_eq!(fetch(aid, "1.2.3"), format!("fetch:{}:1.2.3", Uuid::nil()));
        assert_eq!(
            extract(aid, ExtractorKind::Pdf, "1.0.0"),
            format!("extract:{}:pdf:1.0.0", Uuid::nil())
        );
        assert_eq!(
            clean(aid, "ABC:DEF"),
            format!("clean:{}:abc-def", Uuid::nil()), // ':' becomes '-'
        );
    }

    #[test]
    fn embed_sanitizes_model_id() {
        let eid = EmbeddingSetId(Uuid::nil());
        let key = embed(eid, "bge/large@v1:1024");
        assert!(key.ends_with("bge-large-v1-1024"));
    }

    #[test]
    fn persist_includes_all_ids() {
        let d = DocumentId(Uuid::nil());
        let cs = ChunkSetId(Uuid::nil());
        let es = EmbeddingSetId(Uuid::nil());
        let k = persist(d, cs, es);
        let parts: Vec<_> = k.split(':').collect();
        assert_eq!(parts[0], "persist");
        assert_eq!(parts.len(), 4);
    }
}

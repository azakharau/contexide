Contexide Core
==============

`contexide-core` hosts *all* domain contracts and DTOs for the Contexide stack:
strongly typed IDs, errors, shared enums, workflow model, messaging contracts,
and traits for storage/blob/vector/embeddings/chunking/normalization/extraction.

Rules:
- Only lightweight dependencies (`serde`, `uuid`, `thiserror`, `time`, etc.).
- No concrete integrations (no SQLx, HTTP clients, SDKs) except optional feature
  gates (e.g., `"db"` for sqlx type impls).
- Transport bindings (NATS/JetStream), storage drivers (Postgres/SQLx),
  blob/vector backends (S3/Qdrant), tokenizers, normalizers, extractors, and
  embeddings providers **must live in adapter crates**.
- Comments stay in English.

Suggested module map:
- `ids` — UUIDv7 newtypes.
- `errors` — `Error`/`Result`.
- `types` — core enums/value objects.
- `workflow` — DAG/DagRun/Task/TaskRun + retry/quota/execution policies.
- `messaging` — envelopes, workflow/worker contracts (no transport).
- `blob` / `vector` / `embeddings` / `chunker` / `normalizer` / `extractor` — traits + DTOs.
- `storage_traits` — repository contracts and domain entities (DB-agnostic).
- `message_bus` — abstract publish/subscribe interfaces.
- `prelude` — ergonomic re-exports of common types.

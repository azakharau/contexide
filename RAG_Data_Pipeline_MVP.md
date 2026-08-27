# RAG Data Pipeline (Rust) — Current MVP

Stack: Rust + SQLx • self-hosted infrastructure

This doc reflects the **current code state** of the `contexide` workspace as of Nov 26, 2025: library layers are implemented (ingest primitives, normalization, chunking, embeddings, Qdrant adapter), but no API/orchestrator/compose is present yet.

---

## 0) Goals and Boundaries
**Goals:**
- Library layer for processing unstructured data: extract → normalize → chunk → embed → push to external vector store.
- Multi-tenant, UUIDv7 ids, idempotency at the repo layer.
- Minimal dependencies: Postgres + SQLx, MinIO/S3 for raw data, Qdrant for vectors, TEI/OpenAI for embeddings.

**Out of scope (for now):**
- HTTP API, NATS JetStream, CLI/ops tooling (not in repo).
- pgvector/FTS in Postgres — vectors live only in external Qdrant; Postgres stores references.

---

## 1) Architecture (what exists in code)
- **contexide-core** — IDs, errors, pipeline `Stage`, modalities, asset sources, events.
- **contexide-config** — ENV-only config (`CONTEXIDE_*`), dev defaults.
- **contexide-storage** — Postgres repos + migrations for all domain entities, runtime migrator `migrator::run_all`.
- **contexide-blob-storage** — `BlobStore` trait + `S3Store` (MinIO/S3, presign) and `MemStore`.
- **contexide-extractor** — `ExtractRouter` with PDF (`pdf_extract`), HTTP (HTML→text), Blob (MinIO/S3) extractors; routing by MIME/ext/URL.
- **contexide-normalizer** — Unicode NFKC + whitespace/newline cleanup.
- **contexide-chunker** — `SlidingWindowChunker` + generic tokenizers (HF tokenizers wrapper).
- **contexide-embeddings** — `EmbeddingsProvider`, TEI HTTP and OpenAI providers, `ModelInfo { id, dims, max_batch }`.
- **contexide-vector-storage** — `VectorStore` abstraction + Qdrant adapter (collections, upsert/search/delete, payload, HNSW params).
- **contexide-indexer** — `SimpleIndexer` batches chunk texts, calls embeddings provider, upserts to `VectorSink` (Qdrant) with payload.
- **contexide-messaging** — envelopes + workflow commands/events (transport-agnostic).
- **contexide-workflow-core** — in-memory DAG/Task models, statuses, topo validation.

---

## 2) Data & Storage (Postgres + Qdrant)
### 2.1 Postgres tables (from `contexide-storage/migrations`)
- `tenants(id, name, email)`
- `documents(id, tenant_id, title, status text check in ('draft','processing','ready','failed','archived'), created_at)`
- `assets(id, tenant_id, document_id, filename, mime, size_bytes, storage_key, created_at)`
- `blocks(id, tenant_id, document_id, order_no, modality text|image|table, text, media_asset_id?, created_at)`
- `chunk_sets(id, tenant_id, document_id, profile_hash, finalized bool, created_at)`
- `chunks(id, tenant_id, chunk_set_id, order_no, byte_start, byte_end, text, meta_json?, created_at)`
- `embedding_sets(id, tenant_id, chunk_set_id, model_kind, model_version, dim, metric, ready bool, created_at)`
- `embedding_refs(chunk_id, embedding_set_id, tenant_id, vector_id, created_at)` — link chunk → point_id in vector DB
- `jobs(id, tenant_id, kind text check in ('ingest','extract','normalize','chunk','embed','index'), status text check in ('pending','running','done','failed'), payload_json?, created_at)`

IDs are UUIDv7 (see `contexide-core::ids`). Every table carries `tenant_id`. Migrations are reversible (`*.up/.down`); runner is `migrator::run_all` (no-op if dir missing).

### 2.2 Vector store
- Qdrant via `contexide-vector-storage::qdrant`.
- Collection name = `<collection_prefix>__<logical_name>`; metric from config (cosine/dot/euclid); HNSW params `m`, `ef_construct`.
- Postgres keeps references only (`embedding_refs.vector_id`), vectors live in Qdrant.

### 2.3 Object store
- `BlobStore` interface: put/get/exists/delete + presign GET/PUT.
- Implementations: `S3Store` (MinIO/S3, path-style), `MemStore` (tests/dev).

---

## 3) Pipeline (how to assemble from crates)
- **Ingress/asset**: not implemented; assume asset already in BlobStore (`storage_key`) or reachable via URL.
- **Extract** (`contexide-extractor`): router picks PDF→`PdfExtractor` (text or binary fallback), URL→`HttpExtractor` (text/html/json→text else blob), else `BlobFileExtractor` (sniff MIME, text/html/json→text else blob). Output: `ExtractedBlock { modality, text?, blob?, metadata }`.
- **Normalize** (`contexide-normalizer`): Unicode NFKC, remove zero-width, normalize newlines, collapse whitespace, trim.
- **Chunk** (`contexide-chunker`): token sliding window `ChunkSpec { window_tokens, overlap_tokens, min_chunk_tokens }`, deterministic decode, token offsets.
- **Embed** (`contexide-embeddings`): TEI HTTP or OpenAI; vector length validated vs `ModelInfo::dims` (default 1024).
- **Index** (`contexide-indexer` + `contexide-vector-storage`): `SimpleIndexer` batches texts, ensures collection, upserts vectors with payload.
- **Persist** (`contexide-storage`): repos for documents/assets/blocks/chunks/embedding_sets/embedding_refs/jobs.
- **Workflow/Messaging**: types for commands/events and DAG model exist; transport/executor not implemented.

---

## 4) Config & Launch
- ENV-only config (`CONTEXIDE_<SCOPE>_<KEY>`). Scopes: `STORAGE`, `BLOB_STORAGE`, `VECTOR`, `EMBEDDINGS`, `API`, `WORKERS`. Examples:
  - `CONTEXIDE_STORAGE_DATABASE_URL=postgres://...`
  - `CONTEXIDE_BLOB_STORAGE_S3_ENDPOINT=http://localhost:9000`
  - `CONTEXIDE_VECTOR_VECTOR_ENDPOINT=http://localhost:6333`
  - `CONTEXIDE_EMBEDDINGS_EMBEDDINGS_MODEL=bge-m3`
- No binaries provided; use crates directly. Migration example:

```rust
use contexide_storage::{pool, migrator};
let pool = pool::new_pool("postgres://user:pass@localhost:5432/contexide").await?;
migrator::run_all(&pool).await?;
```

---

## 5) Gaps / Next Steps
- Add HTTP API (ingress/status/search) and orchestrator (NATS JetStream) atop messaging/workflow.
- Ops automation (docker-compose/Helm) and CLI for migrations/buckets.
- Extend extractors (DOCX/OCR/ASR), language-aware cleaner.
- Hybrid search (FTS+vector), reranker, metadata filters.
- **Client offload (WASM plan):** move classification/normalization/chunking into a WebAssembly module executed on client machines. Client sends ready chunks+metadata; server DAG starts from Embed/Index, reducing traffic and load.

---

## 6) Planned Orchestrator (control/data plane)
Built on existing pieces (`contexide-messaging`, `contexide-workflow-core`) but requires runtime implementation.

**Goals:** represent pipeline as DAG; separate control plane (planning/state) and data plane (execution); Postgres as meta-DB; NATS JetStream as transport; workers in Kubernetes (task-per-pod or pool).

**Domain model:**
- `Dag` (template), `DagRun` (instance), `Task` (node within DagRun), `TaskRun` (attempt/retry). Statuses: DagRun `created|running|success|failed|canceled|partial_failed?`; Task `pending|ready|running|success|failed|skipped`; TaskRun `created|running|success|failed|aborted`.

**Components:**
- API: accepts requests, validates/JWT, publishes start command to JetStream.
- Planner: builds/selects DAG, creates DagRun+Tasks in DB, finds initial ready nodes.
- Executor (control plane): single source of truth for statuses; consumes commands and task statuses from JetStream; writes DagRun/Task/TaskRun to Postgres; decides ready tasks with quotas/priorities; publishes `*.request` to workers; handles retries; finalizes DagRun.
- Workers (data plane): domain binaries. Modes: task-per-pod for heavy (OCR/ASR/LLM), long-running pool for light (normalize/chunk). Consume `domain.request`, publish `domain.done`, use Blob/DB.
- Message bus (NATS JetStream): at-least-once; subjects like `chunker.request|done`, `embedder.request|done`, `normalizer.request|done`; payload: {task_id, task_run_id, tenant_id, params/meta, attempt}.
- Meta-DB (Postgres): Dag/DagRun/Task/TaskRun, launch params, artifacts (URIs), model/worker versions, JSONB outputs.

**Execution flow:** API → Planner/Executor create DagRun → Executor selects ready tasks → publishes requests → workers run → send `*.done` → Executor updates statuses, unlocks new tasks → finalizes DagRun and returns artifacts/status.

**Idempotency:** task_id identifies task, task_run_id identifies attempt; DB UPSERT; workers check final state; JetStream may redeliver.

**Fan-out/branching:** workers return structured output (e.g., list of chunks); Executor may add new Tasks dynamically (embed per chunk, conditional OCR branches). Control stays in control plane.

**Backpressure/quotas:** global and per-tenant limits, priorities; mode (task-per-pod vs pool) per domain.

**Observability:** logs with dag_run_id/task_run_id, metrics (durations, error rate, ready/pending depth), tracing (OpenTelemetry), UI/CLI for DAGRun graph.

**Versioning:** record DAG version, worker docker images, model versions (LLM/embedding/reranker) for reproduce/rollout.

**Client offload (WASM):** planned npm JS+WASM for local classify/normalize/chunk. Client sends ready chunks/metadata; server DAG can start at Embed/Index, reducing ingest load.

---

## 7) Open Questions
1. DB schema for DagRun/Task/TaskRun: fields, indexes, artifacts, links to document/asset/chunk/embedding.
2. Planner ↔ Executor API: who creates tasks vs who mutates status; DAG handoff format.
3. Worker modes config: domain-specific task-per-pod vs pool; tenant limits.
4. JetStream protocol: exact payload schemas, versioning, headers policy (tenant_id, correlation_id).
5. Retries/manual restarts: creating new TaskRun, restarting part of DAG vs full DagRun.

---

## Appendix A: Qdrant (short)
- `ensure_collection(name, dim, metric, hnsw)` — creates `<prefix>__<name>` if absent.
- `upsert_points` uses `Uuid` point_id, payload is `serde_json::Map`.
- Filters: equality on payload (`Filter::all(matches(...))`), strings/numbers/array of strings/numbers supported.

---

## Appendix B: Status enums
- Pipeline stages: `Fetch | Extract | Clean | Chunk | Embed | Persist` (`contexide-core::types::Stage`).
- Job.kind: `ingest|extract|normalize|chunk|embed|index`; Job.status: `pending|running|done|failed`.
- Asset source: `upload|url|s3`. Block modality: `text|image|audio|video|table|binary`.

---

Keep this doc and README in sync when new runtime components (API/orchestrator/ops) appear or schemas evolve.

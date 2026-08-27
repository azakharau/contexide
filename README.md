# Contexide

Contexide is a Rust workspace for building multi-tenant document ingestion and
retrieval pipelines. It separates domain contracts from infrastructure
adapters so applications can assemble extraction, normalization, chunking,
embeddings, persistence, messaging, workflow execution, and vector indexing.

The project is an engineering prototype rather than a hosted product. It does
not currently ship an HTTP API or a turnkey deployment.

## Architecture

The workspace is split around explicit boundaries:

- `contexide-core` contains IDs, entities, events, workflow types, and traits.
- `contexide-storage-pg` provides PostgreSQL repositories and migrations.
- `contexide-blob-storage-s3` supports S3-compatible stores and memory.
- `contexide-vector-qdrant` implements the vector sink for Qdrant.
- `contexide-extractor`, `contexide-normalizer`, `contexide-chunker-impl`,
  `contexide-embeddings`, and `contexide-indexer` implement the data path.
- `contexide-messaging-nats`, `contexide-workflow-*`, and
  `contexide-worker-*` implement workflow and worker foundations.

```text
asset -> extract -> normalize -> chunk -> embed -> index
            |                                  |
            +---------- PostgreSQL ------------+
                         S3 / Qdrant
```

Stages are tenant-scoped and use UUIDv7 identifiers. Chunk and embedding sets
are append-only so documents can be reprocessed without mutating prior output.

## What is implemented

- deterministic text normalization and sliding-window chunking;
- PDF, HTTP, and blob extraction routes;
- TEI-compatible and OpenAI-compatible embedding providers;
- PostgreSQL migrations plus in-memory repository implementations;
- S3-compatible blob storage and Qdrant vector indexing;
- typed messaging envelopes, workflow profiles, retry policies, and worker
  runtime primitives;
- unit tests that run without external services.

External-service integration tests and production deployment assets are still
out of scope.

## Development

Install a current stable Rust toolchain, then run:

```bash
cargo test --workspace --all-targets --locked
```

Useful focused checks:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p contexide-chunker-impl
```

PostgreSQL, NATS, S3-compatible storage, Qdrant, and an embedding service are
only required when exercising their real adapters. Most crates include
in-memory or mocked boundaries for local tests.

## Configuration

Runtime crates read `CONTEXIDE_*` environment variables. Representative names:

```text
CONTEXIDE_STORAGE_DATABASE_URL
CONTEXIDE_BLOB_STORAGE_S3_ENDPOINT
CONTEXIDE_BLOB_STORAGE_S3_BUCKET
CONTEXIDE_VECTOR_VECTOR_ENDPOINT
CONTEXIDE_VECTOR_VECTOR_COLLECTION_PREFIX
CONTEXIDE_EMBEDDINGS_EMBEDDINGS_PROVIDER
CONTEXIDE_EMBEDDINGS_EMBEDDINGS_MODEL
```

Credentials are intentionally not stored in the repository. See the typed
loaders in `contexide-config` for the complete settings and development
defaults.

## Design notes

[`RAG_Data_Pipeline_MVP.md`](RAG_Data_Pipeline_MVP.md) records the original
MVP design and implementation boundaries. Code and migrations are the source
of truth when the design note and implementation differ.

## License

MIT. See [`LICENSE`](LICENSE).

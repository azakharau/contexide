-- Mapping (chunk_id, embedding_set_id) -> vector_id in vector DB
create table embedding_refs (
  chunk_id         uuid not null references chunks(id) on delete cascade,
  embedding_set_id uuid not null references embedding_sets(id) on delete cascade,
  tenant_id        uuid not null references tenants(id) on delete cascade,
  vector_id        text not null,
  created_at       timestamptz not null default now(),
  primary key (chunk_id, embedding_set_id)
);

create index idx_embedding_refs_set    on embedding_refs(embedding_set_id);
create index idx_embedding_refs_tenant on embedding_refs(tenant_id);

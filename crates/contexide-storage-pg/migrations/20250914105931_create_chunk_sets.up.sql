-- Group of chunks produced under a specific chunking profile
create table chunk_sets (
  id           uuid primary key,
  tenant_id    uuid not null references tenants(id) on delete cascade,
  document_id  uuid not null references documents(id) on delete cascade,
  profile_hash text not null,
  finalized    boolean not null default false,
  created_at   timestamptz not null default now()
);

create index idx_chunk_sets_doc       on chunk_sets(document_id);
create index idx_chunk_sets_doc_final on chunk_sets(document_id, finalized);
create index idx_chunk_sets_tenant    on chunk_sets(tenant_id);

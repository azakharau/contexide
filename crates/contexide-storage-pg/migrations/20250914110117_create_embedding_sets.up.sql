-- Vectorization runs per chunk set and model
create table embedding_sets (
  id            uuid primary key,
  tenant_id     uuid not null references tenants(id) on delete cascade,
  chunk_set_id  uuid not null references chunk_sets(id) on delete cascade,
  model_kind    text not null,
  model_version text not null,
  dim           int  not null,
  metric        text not null,
  ready         boolean not null default false,
  created_at    timestamptz not null default now()
);

create index idx_embedding_sets_chunk_set on embedding_sets(chunk_set_id);
create index idx_embedding_sets_ready     on embedding_sets(ready);
create index idx_embedding_sets_tenant    on embedding_sets(tenant_id);

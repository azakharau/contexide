-- Final normalized text pieces
create table chunks (
  id            uuid primary key,
  tenant_id     uuid not null references tenants(id) on delete cascade,
  chunk_set_id  uuid not null references chunk_sets(id) on delete cascade,
  order_no      int  not null,
  byte_start    int  not null,
  byte_end      int  not null,
  text          text not null,
  meta_json     text,
  created_at    timestamptz not null default now()
);

create index idx_chunks_set       on chunks(chunk_set_id);
create index idx_chunks_set_order on chunks(chunk_set_id, order_no);
create index idx_chunks_tenant    on chunks(tenant_id);

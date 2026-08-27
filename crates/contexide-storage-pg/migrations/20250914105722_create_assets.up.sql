-- Binary assets attached to a document (files, images, etc.)
create table assets (
  id           uuid primary key,
  tenant_id    uuid not null references tenants(id) on delete cascade,
  document_id  uuid not null references documents(id) on delete cascade,
  filename     text not null,
  mime         text not null,
  size_bytes   bigint not null check (size_bytes >= 0),
  storage_key  text,
  created_at   timestamptz not null default now()
);

create index idx_assets_tenant   on assets(tenant_id);
create index idx_assets_document on assets(document_id);

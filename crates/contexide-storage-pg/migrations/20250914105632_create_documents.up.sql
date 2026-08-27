-- Documents belong to a tenant
create table documents (
  id         uuid primary key,
  tenant_id  uuid not null references tenants(id) on delete cascade,
  title      text not null,
  status     text not null check (status in ('draft','processing','ready','failed','archived')),
  created_at timestamptz not null default now()
);

create index idx_documents_tenant on documents(tenant_id);
create index idx_documents_status on documents(status);

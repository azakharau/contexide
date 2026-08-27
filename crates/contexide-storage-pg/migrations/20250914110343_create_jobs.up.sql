-- Pipeline jobs for workers and monitoring
create table jobs (
  id           uuid primary key,
  tenant_id    uuid not null references tenants(id) on delete cascade,
  kind         text not null check (kind in ('ingest','extract','normalize','chunk','embed','index')),
  status       text not null check (status in ('pending','running','done','failed')),
  payload_json text,
  created_at   timestamptz not null default now()
);

create index idx_jobs_kind         on jobs(kind);
create index idx_jobs_kind_status  on jobs(kind, status);
create index idx_jobs_tenant       on jobs(tenant_id);

-- Source blocks after extraction (text/image/table...)
create table blocks (
  id             uuid primary key,
  tenant_id      uuid not null references tenants(id) on delete cascade,
  document_id    uuid not null references documents(id) on delete cascade,
  order_no       int  not null,
  modality       text not null check (modality in ('text','image','table')),
  text           text,
  media_asset_id uuid,
  created_at     timestamptz not null default now()
);

alter table blocks
  add constraint fk_blocks_media_asset
  foreign key (media_asset_id) references assets(id) on delete set null;

create index idx_blocks_doc_order on blocks(document_id, order_no);
create index idx_blocks_tenant    on blocks(tenant_id);
create index idx_blocks_asset     on blocks(media_asset_id);

-- Tenants with unique name & email
create table tenants (
  id         uuid primary key,
  name       text not null unique,
  email      text not null unique,
  created_at timestamptz not null default now()
);

create unique index uq_tenants_name on tenants(name);
create unique index uq_tenants_email on tenants(email);

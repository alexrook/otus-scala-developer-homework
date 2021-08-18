create extension if not exists "uuid-ossp";

create table roles (
    code text primary key,
    name text not null
);

insert into roles(code, name)
values ('reader', 'Reader'),
('manager', 'Manager'),
('admin', 'Admin');

create table users (
    id uuid primary key default uuid_generate_v4(),
    first_name text not null,
    last_name text not null,
    age integer not null
)
;

create table users_to_roles (
    users_id uuid not null references users(id),
    roles_code text not null references roles(code)
)
;


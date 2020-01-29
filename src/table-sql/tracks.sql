create table tracks(
id text primary key,
name text not null,
available_in_us boolean not null,
duration_ms int not null,
explicit boolean not null,
uri text unique not null,
preview_url text
);

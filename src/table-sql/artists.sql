create table artists(
id text primary key,
name text not null,
uri text unique not null
);

create table albums(
id text primary key,
name text not null,
uri text unique not null,
available_in_us boolean not null,
album_type text not null,
release_year smallint, --unclear if this is available everywhere
release_month smallint, --definitely not available everywhere
release_day smallint, --ditto
popularity smallint,
label text
);

create table tracks(
id text primary key,
name text not null,
main_artist text references artists(id),
available_in_us boolean not null,
duration_ms int not null,
explicit boolean not null,
uri text unique not null,
preview_url text,
acousticness real,
danceability real,
energy real,
instrumentalness real,
key smallint,
liveness real,
loudness real,
mode boolean, --major/minor
speechiness real,
tempo real,
time_signature smallint,
valence real
);

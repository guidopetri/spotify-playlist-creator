create table albums_x_artists(
album_id text references albums(id) not null,
artist_id text references artists(id) not null
);

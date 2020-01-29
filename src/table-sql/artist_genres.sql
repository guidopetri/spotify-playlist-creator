create table artist_genres(
artist_id text references artists(id) not null,
genre_name text not null
);

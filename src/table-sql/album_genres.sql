create table album_genres(
album_id text references albums(id) not null,
genre text not null
);

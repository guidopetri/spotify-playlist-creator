create table popularity(
id serial primary key,
element_id text not null,
element_type text not null, --'artist', 'album', 'track'
popularity smallint,
popularity_date date not null --may be slightly off (lagged) according to api docs
);

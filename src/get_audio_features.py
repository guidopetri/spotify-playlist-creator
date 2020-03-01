#! /usr/bin/env python3

from luigi import Task, LocalTarget
from luigi.format import Nop
from luigi.util import requires, inherits
from postgres_templates import TransactionFactTable, CopyWrapper, HashableDict
from spotify_api import check_for_refresh


class GetSavedTracks(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return [LocalTarget(file_location.format('saved_songs'), format=Nop),
                LocalTarget(file_location.format('mini_albums'), format=Nop),
                LocalTarget(file_location.format('mini_artists'), format=Nop),
                ]

    def run(self):
        from requests import get
        import pickle
        import time

        [x.makedirs() for x in self.output()]

        access_token = check_for_refresh()

        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = 'https://api.spotify.com/v1/me/tracks'
        params = {'limit': 50}

        songs = []

        while url is not None:
            for attempt in range(2):
                access_token = check_for_refresh()
                headers = {'Authorization': 'Bearer {}'.format(access_token)}

                r = get(url, params=params, headers=headers)

                if r.status_code == 200:
                    break
            else:  # no break
                print('Error accessing url: {}'.format(url))
                r.raise_for_status()

            data = r.json()
            songs.extend(data['items'])

        albums = set(song['track']['album']['id']
                     for song in songs)

        artists = set(artist['id'] for song in songs
                      for artist in song['track']['artists'])
        url = data['next']

        with self.output()[0].open('w') as f:
            pickle.dump(songs, f, protocol=-1)

        with self.output()[1].open('w') as f:
            pickle.dump(albums, f, protocol=-1)

        with self.output()[2].open('w') as f:
            pickle.dump(artists, f, protocol=-1)


@requires(GetSavedTracks)
class GetAlbums(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('full_albums'), format=Nop)

    def run(self):
        from requests import get
        from more_itertools import chunked
        from pandas import DataFrame
        import pickle

        self.output().makedirs()

        with self.input()[1].open('r') as f:
            short_albums = pickle.load(f)

        access_token = check_for_refresh()

        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = 'https://api.spotify.com/v1/albums'

        albums = []
        grouped = chunked(short_albums, 20)

        for group in grouped:
            params = {'ids': ','.join(album for album in group)}

            for attempt in range(2):
                access_token = check_for_refresh()
                headers = {'Authorization': 'Bearer {}'.format(access_token)}

                r = get(url, params=params, headers=headers)

                if r.status_code == 200:
                    break
            else:  # no break
                print('Error accessing url: {}'.format(url))
                r.raise_for_status()

            data = r.json()
            albums.extend(data['albums'])

        album_data = [(album['id'],
                       album['name'],
                       album['uri'],
                       'US' in album['available_markets'],
                       album['album_type'],
                       album['release_date'],
                       album['label'],
                       album['genres'],
                       [artist['id'] for artist in album['artists']])
                      for album in albums]

        full_albums = DataFrame(album_data,
                                columns=['id',
                                         'name',
                                         'uri',
                                         'available_in_us',
                                         'album_type',
                                         'release_date',
                                         'label',
                                         'genre',
                                         'artist',
                                         ])

        with self.output().temporary_path() as temp_path:
            full_albums.to_pickle(temp_path, compression=None)


@requires(GetAlbums)
class ExplodeArtistsAlbums(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('album_artists'), format=Nop)

    def run(self):
        import pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            full_albums = pickle.load(f)

        album_artists = full_albums[['id', 'artist']]
        album_artists = album_artists.explode('artist')
        album_artists.columns = ['album_id', 'artist_id']

        with self.output().temporary_path() as temp_path:
            album_artists.to_pickle(temp_path, compression=None)


@requires(GetSavedTracks, ExplodeArtistsAlbums)
class GetArtists(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('full_artists'), format=Nop)

    def run(self):
        from requests import get
        from more_itertools import chunked
        from pandas import DataFrame, read_pickle
        import pickle

        self.output().makedirs()

        with self.input()[0][2].open('r') as f:
            short_artists = pickle.load(f)

        with self.input()[1].open('r') as f:
            artist_x_album = read_pickle(f, compression=None)

        access_token = check_for_refresh()

        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = 'https://api.spotify.com/v1/artists'

        short_artists.update(artist_x_album['artist_id'].values)

        artists = []
        grouped = chunked(short_artists, 50)

        for group in grouped:
            params = {'ids': ','.join(artist for artist in group)}

            for attempt in range(2):
                access_token = check_for_refresh()
                headers = {'Authorization': 'Bearer {}'.format(access_token)}

                r = get(url, params=params, headers=headers)

                if r.status_code == 200:
                    break
            else:  # no break
                print('Error accessing url: {}'.format(url))
                r.raise_for_status()

            data = r.json()
            artists.extend(data['artists'])

        artist_data = [(artist['id'],
                        artist['name'],
                        artist['uri'],
                        artist['genres'])
                       for artist in artists]

        full_artists = DataFrame(artist_data,
                                 columns=['id', 'name', 'uri', 'genre'])

        with self.output().temporary_path() as temp_path:
            full_artists.to_pickle(temp_path, compression=None)


@requires(GetArtists)
class ExplodeGenresArtists(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('artist_genres'), format=Nop)

    def run(self):
        import pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            full_artists = pickle.load(f)

        artist_genres = full_artists[['id', 'genre']]
        artist_genres = artist_genres.explode('genre')
        artist_genres.columns = ['artist_id', 'genre_name']

        with self.output().temporary_path() as temp_path:
            artist_genres.to_pickle(temp_path, compression=None)


@requires(GetArtists)
class CleanArtists(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('clean_artists'), format=Nop)

    def run(self):
        import pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            full_artists = pickle.load(f)

        full_artists.drop(['genre'], axis=1, inplace=True)

        with self.output().temporary_path() as temp_path:
            full_artists.to_pickle(temp_path, compression=None)


@requires(GetAlbums)
class ExplodeGenresAlbums(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('album_genres'), format=Nop)

    def run(self):
        import pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            full_albums = pickle.load(f)

        album_genres = full_albums[['id', 'genre']]
        album_genres = album_genres.explode('genre')
        album_genres.columns = ['album_id', 'genre_name']

        with self.output().temporary_path() as temp_path:
            album_genres.to_pickle(temp_path, compression=None)


@requires(GetAlbums)
class CleanAlbums(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('clean_albums'), format=Nop)

    def run(self):
        import pickle
        from pandas import concat
        from numpy import nan

        self.output().makedirs()

        with self.input().open('r') as f:
            full_albums = pickle.load(f)

        full_albums.drop(['genre', 'artist'], axis=1, inplace=True)

        regex_pat = (r'^(?P<release_year>\d{4})?-?'
                     r'(?P<release_month>\d{2})?-?'
                     r'(?P<release_day>\d{2})?$')

        release_info = full_albums['release_date'].str.extractall(regex_pat)
        # release_info = release_info.astype(float).astype('Int64')
        release_info.fillna('\\N', inplace=True)

        release_info.index = release_info.index.droplevel(1)

        clean_albums = concat([full_albums, release_info], axis=1)

        with self.output().temporary_path() as temp_path:
            clean_albums.to_pickle(temp_path, compression=None)


@requires(GetSavedTracks)
class GetAudioFeatures(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('audio_features'), format=Nop)

    def run(self):
        from requests import get
        from more_itertools import chunked
        from pandas import DataFrame
        import pickle

        self.output().makedirs()

        with self.input()[0].open('r') as f:
            songs = pickle.load(f)

        access_token = check_for_refresh()

        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = 'https://api.spotify.com/v1/audio-features/'

        audio_features = []
        grouped = chunked(songs, 100)

        for group in grouped:
            params = {'ids': ','.join(song['track']['id'] for song in group)}

            for attempt in range(2):
                access_token = check_for_refresh()
                headers = {'Authorization': 'Bearer {}'.format(access_token)}

                r = get(url, params=params, headers=headers)

                if r.status_code == 200:
                    break
            else:  # no break
                print('Error accessing url: {}'.format(url))
                r.raise_for_status()

            data = r.json()
            audio_features.extend(data['audio_features'])

        audio_data = [(song['id'],
                       song['acousticness'],
                       song['danceability'],
                       song['energy'],
                       song['instrumentalness'],
                       song['key'],
                       song['liveness'],
                       song['loudness'],
                       song['mode'],
                       song['speechiness'],
                       song['tempo'],
                       song['time_signature'],
                       song['valence'])
                      for song in audio_features]

        audio_data_df = DataFrame(audio_data,
                                  columns=['id',
                                           'acousticness',
                                           'danceability',
                                           'energy',
                                           'instrumentalness',
                                           'key',
                                           'liveness',
                                           'loudness',
                                           'mode',
                                           'speechiness',
                                           'tempo',
                                           'time_signature',
                                           'valence'])

        with self.output().temporary_path() as temp_path:
            audio_data_df.to_pickle(temp_path, compression=None)


@requires(GetSavedTracks)
class CleanTracks(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('clean_tracks'), format=Nop)

    def run(self):
        import pickle
        from pandas import DataFrame

        self.output().makedirs()

        with self.input()[0].open('r') as f:
            songs = pickle.load(f)

        songs = [song['track'] for song in songs]

        song_data = [(song['id'],
                      song['name'],
                      'US' in song['available_markets'],
                      song['duration_ms'],
                      song['explicit'],
                      song['uri'],
                      song['preview_url'])
                     for song in songs]

        song_data_df = DataFrame(song_data,
                                 columns=['id',
                                          'name',
                                          'available_in_us',
                                          'duration_ms',
                                          'explicit',
                                          'uri',
                                          'preview_url'])

        with self.output().temporary_path() as temp_path:
            song_data_df.to_pickle(temp_path, compression=None)


@requires(CleanTracks, GetAudioFeatures)
class MergeTracks(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('complete_tracks'), format=Nop)

    def run(self):
        import pickle
        from pandas import merge

        self.output().makedirs()

        with self.input()[0].open('r') as f:
            songs = pickle.load(f)

        with self.input()[1].open('r') as f:
            features = pickle.load(f)

        tracks = merge(songs, features, on='id')

        with self.output().temporary_path() as temp_path:
            tracks.to_pickle(temp_path, compression=None)


@requires(CleanArtists)
class ArtistList(TransactionFactTable):
    priority = 2
    pass


@requires(ExplodeGenresArtists)
class GenreXArtistList(TransactionFactTable):
    pass


@requires(CleanAlbums)
class AlbumList(TransactionFactTable):
    priority = 2
    pass


@requires(ExplodeGenresAlbums)
class GenreXAlbumList(TransactionFactTable):
    pass


@requires(ExplodeArtistsAlbums)
class ArtistXAlbumList(TransactionFactTable):
    pass


@requires(MergeTracks)
class SavedTracksList(TransactionFactTable):
    pass


@inherits(GetArtists)
class CopyTracks(CopyWrapper):

    jobs = [{'table_type': ArtistList,
             'fn':         CleanArtists,
             'table':      'artists',
             'columns':    ['id',
                            'name',
                            'uri',
                            ],
             'id_cols':    ['id',
                            ],
             'date_cols':  [],
             'merge_cols': HashableDict()},
            {'table_type': GenreXArtistList,
             'fn': ExplodeGenresArtists,
             'table': 'artist_genres',
             'columns': ['artist_id',
                         'genre_name',
                         ],
             'id_cols': ['artist_id',
                         'genre_name',
                         ],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': ArtistXAlbumList,
             'fn': ExplodeArtistsAlbums,
             'table': 'albums_x_artists',
             'columns': ['album_id',
                         'artist_id',
                         ],
             'id_cols': ['album_id',
                         'artist_id',
                         ],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': GenreXAlbumList,
             'fn': ExplodeGenresAlbums,
             'table': 'album_genres',
             'columns': ['album_id',
                         'genre_name',
                         ],
             'id_cols': ['album_id',
                         'genre_name',
                         ],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': AlbumList,
             'fn':         CleanAlbums,
             'table':      'albums',
             'columns':    ['id',
                            'name',
                            'uri',
                            'available_in_us',
                            'album_type',
                            'release_year',
                            'release_month',
                            'release_day',
                            'label'
                            ],
             'id_cols':    ['id',
                            ],
             'date_cols':  [],
             'merge_cols': HashableDict()},
            {'table_type': SavedTracksList,
             'fn': MergeTracks,
             'table': 'tracks',
             'columns': ['id',
                         'name',
                         'available_in_us',
                         'duration_ms',
                         'explicit',
                         'uri',
                         'preview_url',
                         'acousticness',
                         'danceability',
                         'energy',
                         'instrumentalness',
                         'key',
                         'liveness',
                         'loudness',
                         'mode',
                         'speechiness',
                         'tempo',
                         'time_signature',
                         'valence',
                         ],
             'id_cols': ['id'],
             'date_cols': [],
             'merge_cols': HashableDict()},
            ]

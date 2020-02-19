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
        error_url = None

        songs = []

        while url:
            access_token = check_for_refresh()

            r = get(url, params=params, headers=headers)

            if r.status_code != 200:
                if error_url == url:
                    r.raise_for_status()
                print('Error accessing url: {}'.format(url))
                error_url = url
                time.sleep(1)
                continue

            data = r.json()
            songs.extend(data['items'])
            url = None  # currently testing

        albums = set(song['track']['album']['id']
                     for song in songs)

        artists = set(artist['id'] for song in songs
                      for artist in song['track']['artists'])

        with self.output()[0].open('w') as f:
            pickle.dump(songs, f, protocol=-1)

        with self.output()[1].open('w') as f:
            pickle.dump(albums, f, protocol=-1)

        with self.output()[2].open('w') as f:
            pickle.dump(artists, f, protocol=-1)


@requires(GetSavedTracks)
class GetArtists(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('full_artists'), format=Nop)

    def run(self):
        from requests import get
        from more_itertools import chunked
        from pandas import DataFrame
        import pickle

        self.output().makedirs()

        with self.input()[2].open('r') as f:
            short_artists = pickle.load(f)

        access_token = check_for_refresh()

        headers = {'Authorization': 'Bearer {}'.format(access_token)}
        url = 'https://api.spotify.com/v1/artists'

        artists = []
        grouped = chunked(short_artists, 50)

        for group in grouped:
            params = {'ids': ','.join(artist for artist in group)}

            for attempt in range(2):
                access_token = check_for_refresh()

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

        with self.output().open('w') as f:
            pickle.dump(full_artists, f, protocol=-1)


@requires(GetArtists)
class ExplodeGenres(Task):

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

        with self.output().open('w') as f:
            pickle.dump(artist_genres, f, protocol=-1)


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

        full_artists.drop('genre', axis=1, inplace=True)

        with self.output().open('w') as f:
            pickle.dump(full_artists, f, protocol=-1)

# class GetAudioFeatures(Task):

#     auth_token = Parameter(visibility=ParameterVisibility.PRIVATE,
#                            significant=False)

#     def run(self):

#         songs = []

#         track_features_url = 'https://api.spotify.com/v1/audio-features/'

#         headers = {'Authorization': 'Bearer {}'.format(self.auth_token),
#                    }

#         params = {'ids': ','.join([x['track']['id']
#                                    for x in songs['items']]),
#                   }

#         r = requests.get(track_features_url,
#                          headers=headers,
#                          params=params,
#                          )

#         if r.status_code != 200:
#             print('error')

#         features = r.json()['audio_features']

#     def output(self):
#         pass


@requires(CleanArtists)
class ArtistList(TransactionFactTable):
    pass


@requires(ExplodeGenres)
class GenreList(TransactionFactTable):
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
            {'table_type': GenreList,
             'fn': ExplodeGenres,
             'table': 'artist_genres',
             'columns': ['artist_id',
                         'genre_name',
                         ],
             'id_cols': ['artist_id',
                         'genre_name',
                         ],
             'date_cols': [],
             'merge_cols': HashableDict()},
            # {'table_type': MoveClocks,
            #  'fn': ExplodeClocks,
            #  'table': 'game_clocks',
            #  'columns': ['game_link',
            #              'half_move',
            #              'clock',
            #              ],
            #  'id_cols': ['game_link',
            #              'half_move'],
            #  'date_cols': [],
            #  'merge_cols': HashableDict()},
            # {'table_type': MoveList,
            #  'fn': ExplodeMoves,
            #  'table': 'game_moves',
            #  'columns': ['game_link',
            #              'half_move',
            #              'move',
            #              ],
            #  'id_cols': ['game_link',
            #              'half_move'],
            #  'date_cols': [],
            #  'merge_cols': HashableDict()},
            ]

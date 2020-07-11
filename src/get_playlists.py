#! /usr/bin/env python3

from luigi import Task, LocalTarget
from luigi.format import Nop
from luigi.util import requires, inherits
from postgres_templates import TransactionFactTable, CopyWrapper, HashableDict
from spotify_api import check_for_refresh


class GetGenrePlaylists(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('playlists'), format=Nop)

    def run(self):
        from requests import get
        import pickle

        self.output().makedirs()

        access_token = check_for_refresh()

        headers = {'Authorization': 'Bearer {}'.format(access_token)}

        user_url = 'https://api.spotify.com/v1/me'
        r = get(user_url, headers=headers)
        user_data = r.json()

        url = f'https://api.spotify.com/v1/users/{user_data["id"]}/playlists'
        params = {'limit': 50}

        playlists = []

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

            playlists.extend(data['items'])
            url = data['next']

        with self.output().open('w') as f:
            pickle.dump(playlists, f, protocol=-1)


@requires(GetGenrePlaylists)
class FilterPlaylists(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('filtered_playlists'),
                           format=Nop)

    def run(self):
        import pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            playlists = pickle.load(f)

        names = [x['name'] for x in playlists]
        start = names.index('Deal with later') + 1
        end = names.index('Shazam')
        names = names[start:end]

        filtered_playlists = [playlist
                              for playlist in playlists
                              if playlist['name'] in names]

        with self.output().open('w') as f:
            pickle.dump(filtered_playlists, f, protocol=-1)


@requires(FilterPlaylists)
class PlaylistInfos(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('playlist_infos'),
                           format=Nop)

    def run(self):
        import pickle
        from pandas import DataFrame

        self.output().makedirs()

        with self.input().open('r') as f:
            playlists = pickle.load(f)

        playlists_df = DataFrame(((x['id'], x['name']) for x in playlists),
                                 columns=['playlist_id', 'genre_name'])

        with self.output().temporary_path() as temp_path:
            playlists_df.to_pickle(temp_path, compression=None)


@requires(FilterPlaylists)
class GetTracksByPlaylist(Task):

    def output(self):
        import os

        file_location = os.path.expanduser('~/Temp/luigi/spotify/{}.pckl')
        return LocalTarget(file_location.format('tracks_playlists'),
                           format=Nop)

    def run(self):
        from requests import get
        from itertools import cycle
        import pickle
        from pandas import DataFrame

        self.output().makedirs()

        with self.input().open('r') as f:
            playlists = pickle.load(f)

        params = {'limit': 100, 'fields': 'items(track(id)),next'}

        playlist_tracks = []

        for playlist in playlists:
            url = playlist['tracks']['href']
            tracks = []

            while url is not None:
                for attempt in range(2):
                    access_token = check_for_refresh()
                    headers = {'Authorization': f'Bearer {access_token}'}

                    r = get(url, params=params, headers=headers)

                    if r.status_code == 200:
                        break
                else:  # no break
                    print('Error accessing url: {}'.format(url))
                    r.raise_for_status()

                data = r.json()

                tracks.extend([x['track']['id'] for x in data['items']])

                url = data['next']

            playlist_tracks.append([x
                                    for x in zip(cycle([playlist['name']]),
                                                 tracks)])

        playlists_df = DataFrame(playlist_tracks,
                                 columns=['genre_name', 'track_id'])

        with self.output().temporary_path() as temp_path:
            playlists_df.to_pickle(temp_path, compression=None)


@requires(PlaylistInfos)
class PlaylistsList(TransactionFactTable):
    pass


@requires(GetTracksByPlaylist)
class PlaylistXTracksList(TransactionFactTable):
    pass


@inherits(GetGenrePlaylists)
class CopyTracks(CopyWrapper):

    jobs = [{'table_type': PlaylistXTracksList,
             'fn': GetTracksByPlaylist,
             'table': 'playlist_x_tracks',
             'columns': ['track_id',
                         'genre_name',
                         ],
             'id_cols': ['track_id',
                         'genre_name',
                         ],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': PlaylistsList,
             'fn': PlaylistInfos,
             'table': 'playlists',
             'columns': ['playlist_id',
                         'genre_name',
                         ],
             'id_cols': ['playlist_id',
                         'genre_name',
                         ],
             'date_cols': [],
             'merge_cols': HashableDict()},
            ]
